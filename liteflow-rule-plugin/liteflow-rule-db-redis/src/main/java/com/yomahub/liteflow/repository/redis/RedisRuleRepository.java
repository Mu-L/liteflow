package com.yomahub.liteflow.repository.redis;

import cn.hutool.core.util.StrUtil;
import com.yomahub.liteflow.exception.SeqGapException;
import com.yomahub.liteflow.repository.RuleChangeListener;
import com.yomahub.liteflow.repository.RuleRepository;
import com.yomahub.liteflow.repository.vo.*;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RTopic;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.client.protocol.ScoredEntry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Rule-DB 的 Redis 权威源实现。
 *
 * <p>读路径：
 * <ul>
 *     <li>{@link #fetchManifest()}：读 chain-index/script-index 两个 HASH，按 {@code "version|md5"}
 *     与 {@code "version|md5|type|language|name"} 拆分；末尾 {@code seq} 复用 {@link #fetchLatestSeq()}。</li>
 *     <li>{@link #fetchChain(String)} / {@link #fetchScript(String)}：读对应内容 HASH（el/route/namespace/
 *     version/md5/enable 或 script/name/type/language/version/md5/enable）；{@code enable} 以 {@code "0"} 判否。</li>
 *     <li>{@link #fetchLatestSeq()}：读 {@code seq} key（由发布端 INCR 维护）。</li>
 *     <li>{@link #fetchChangesSince(long)}：ZSet {@code changelog} 按 score 区间 {@code (seq, +inf]} 取成员（JSON），
 *     并做断档检测——最小 score &gt; seq+1 时抛 {@link SeqGapException} 触发全量对账。</li>
 *     <li>{@link #subscribe(RuleChangeListener)}：订阅 {@code notify} 频道，逐条 JSON 解码后回调。</li>
 * </ul>
 *
 * <p>所有 RMap/RScoredSortedSet/RTopic/RBucket 均使用 {@link StringCodec}，使值以 String 形式返回。
 *
 * @author Bryan.Zhang
 * @since 2.16.2
 */
public class RedisRuleRepository implements RuleRepository {

	private final RedisConnectionManager connectionManager = new RedisConnectionManager();

	private RedissonClient redisson() {
		return connectionManager.getClient();
	}

	@Override
	public RuleManifest fetchManifest() {
		RedissonClient client = redisson();
		RuleManifest manifest = new RuleManifest();
		List<ChainMeta> chains = new ArrayList<>();
		Map<String, String> chainIndex = readAllMap(client, RedisKeys.chainIndex());
		for (Map.Entry<String, String> e : chainIndex.entrySet()) {
			// value = "version|md5"
			String[] parts = e.getValue().split("\\|", -1);
			chains.add(new ChainMeta(e.getKey(), parseLong(parts, 0), get(parts, 1)));
		}
		List<ScriptMeta> scripts = new ArrayList<>();
		Map<String, String> scriptIndex = readAllMap(client, RedisKeys.scriptIndex());
		for (Map.Entry<String, String> e : scriptIndex.entrySet()) {
			// value = "version|md5|type|language|name"
			String[] parts = e.getValue().split("\\|", -1);
			scripts.add(new ScriptMeta(e.getKey(), parseLong(parts, 0), get(parts, 1),
					get(parts, 2), get(parts, 3), get(parts, 4)));
		}
		manifest.setChains(chains);
		manifest.setScripts(scripts);
		manifest.setLatestSeq(fetchLatestSeq());
		return manifest;
	}

	@Override
	public ChainRecord fetchChain(String chainId) {
		Map<String, String> h = readAllMap(redisson(), RedisKeys.chain(chainId));
		if (h == null || h.isEmpty()) {
			return null;
		}
		ChainRecord r = new ChainRecord();
		r.setChainId(chainId);
		r.setEl(h.get("el"));
		r.setRoute(h.get("route"));
		r.setNamespace(h.get("namespace"));
		r.setVersion(parseLong(h.get("version")));
		r.setMd5(h.get("md5"));
		r.setEnable(!"0".equals(h.get("enable")));
		return r;
	}

	@Override
	public ScriptRecord fetchScript(String nodeId) {
		Map<String, String> h = readAllMap(redisson(), RedisKeys.script(nodeId));
		if (h == null || h.isEmpty()) {
			return null;
		}
		ScriptRecord r = new ScriptRecord();
		r.setNodeId(nodeId);
		r.setScript(h.get("script"));
		r.setName(h.get("name"));
		r.setType(h.get("type"));
		r.setLanguage(h.get("language"));
		r.setVersion(parseLong(h.get("version")));
		r.setMd5(h.get("md5"));
		r.setEnable(!"0".equals(h.get("enable")));
		return r;
	}

	@Override
	public long fetchLatestSeq() {
		Object v = redisson().getBucket(RedisKeys.seq(), StringCodec.INSTANCE).get();
		return v == null ? 0 : parseLong(v.toString());
	}

	@Override
	public List<ChangeRecord> fetchChangesSince(long seq) {
		RScoredSortedSet<String> log = redisson().getScoredSortedSet(RedisKeys.changelog(), StringCodec.INSTANCE);
		// 断档检测：最小 score > seq+1 说明中间被裁剪
		Collection<ScoredEntry<String>> firstEntry = log.entryRange(0, 0);
		if (seq > 0 && !firstEntry.isEmpty()) {
			double min = firstEntry.iterator().next().getScore();
			if (min > seq + 1) {
				throw new SeqGapException("redis changelog gap: since=" + seq + " min=" + (long) min);
			}
		}
		List<ChangeRecord> result = new ArrayList<>();
		// (seq, +inf]：exclusive 下界
		Collection<String> members = log.valueRange(seq, false, Double.POSITIVE_INFINITY, true);
		for (String json : members) {
			result.add(ChangeCodec.fromJson(json));
		}
		return result;
	}

	@Override
	public void subscribe(RuleChangeListener listener) {
		RTopic topic = redisson().getTopic(RedisKeys.notifyChannel(), StringCodec.INSTANCE);
		topic.addListener(String.class, (channel, msg) -> {
			ChangeRecord c = ChangeCodec.fromJson(msg);
			List<ChangeRecord> one = new ArrayList<>();
			one.add(c);
			listener.onChanges(one);
		});
	}

	@Override
	public void close() {
		connectionManager.shutdown();
	}

	/** 读取整个 HASH 为 {@code Map<String, String>}；显式类型见证避免链式调用退化为 {@code Map<Object,Object>}。 */
	private static Map<String, String> readAllMap(RedissonClient client, String key) {
		return client.<String, String>getMap(key, StringCodec.INSTANCE).readAllMap();
	}

	private static long parseLong(String s) {
		return StrUtil.isBlank(s) ? 0 : Long.parseLong(s.trim());
	}

	private static long parseLong(String[] parts, int idx) {
		return idx < parts.length ? parseLong(parts[idx]) : 0;
	}

	private static String get(String[] parts, int idx) {
		String v = idx < parts.length ? parts[idx] : null;
		return StrUtil.isBlank(v) ? null : v;
	}
}

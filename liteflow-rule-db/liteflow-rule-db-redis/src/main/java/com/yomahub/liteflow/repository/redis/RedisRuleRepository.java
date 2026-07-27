package com.yomahub.liteflow.repository.redis;

import cn.hutool.core.util.StrUtil;
import com.yomahub.liteflow.exception.ConfigErrorException;
import com.yomahub.liteflow.exception.SeqGapException;
import com.yomahub.liteflow.property.LiteflowConfigGetter;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.property.RuleDbRedisConfig;
import com.yomahub.liteflow.publisher.exception.RuleStorageException;
import com.yomahub.liteflow.repository.RuleRepository;
import com.yomahub.liteflow.repository.vo.ChainMeta;
import com.yomahub.liteflow.repository.vo.ChainRecord;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import com.yomahub.liteflow.repository.vo.RuleManifest;
import com.yomahub.liteflow.repository.vo.ScriptMeta;
import com.yomahub.liteflow.repository.vo.ScriptRecord;
import org.redisson.api.RBatch;
import org.redisson.api.RFuture;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.client.protocol.ScoredEntry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Redis authoritative repository using ID sets and pipelined metadata reads. */
public class RedisRuleRepository implements RuleRepository {

	static final int DEFAULT_CHANGELOG_BATCH_SIZE = 1000;
	static final int MANIFEST_SNAPSHOT_ATTEMPTS = 5;

	private static final Set<String> CHAIN_META_FIELDS = fields("version", "md5", "enable");
	private static final Set<String> SCRIPT_META_FIELDS = fields(
			"version", "md5", "enable", "type", "language", "name");

	private final RedisConnectionManager connectionManager;
	private final RedisKeys keys;

	public RedisRuleRepository() {
		RuleDbConfig config = LiteflowConfigGetter.get().getRuleDb();
		RuleDbRedisConfig redis = config == null || config.getRedis() == null
				? new RuleDbRedisConfig() : config.getRedis();
		String applicationName = config == null ? null : config.getApplicationName();
		this.connectionManager = new RedisConnectionManager(redis);
		this.keys = new RedisKeys(redis.getKeyPrefix(), applicationName, redis.getKeyHashTag());
	}

	RedisRuleRepository(RedisConnectionManager connectionManager, RedisKeys keys) {
		this.connectionManager = connectionManager;
		this.keys = keys;
	}

	@Override
	public RuleManifest fetchManifest() {
		long before = -1;
		long after = -1;
		for (int attempt = 0; attempt < MANIFEST_SNAPSHOT_ATTEMPTS; attempt++) {
			before = fetchLatestSeq();
			RuleManifest manifest = fetchManifestOnce();
			after = manifest.getLatestSeq();
			if (before == after) {
				return manifest;
			}
		}
		throw new RuleStorageException("Redis manifest changed during " + MANIFEST_SNAPSHOT_ATTEMPTS
				+ " consecutive snapshot attempts (before=" + before + ", after=" + after + ")");
	}

	private RuleManifest fetchManifestOnce() {
		RedissonClient client = redisson();
		List<String> chainIds = sorted(client.<String>getSet(keys.chainIds(), StringCodec.INSTANCE).readAll());
		List<String> scriptIds = sorted(client.<String>getSet(keys.scriptIds(), StringCodec.INSTANCE).readAll());

		RBatch batch = client.createBatch();
		List<RFuture<Map<String, String>>> chainFutures = new ArrayList<>();
		for (String chainId : chainIds) {
			chainFutures.add(batch.<String, String>getMap(keys.chain(chainId), StringCodec.INSTANCE)
					.getAllAsync(CHAIN_META_FIELDS));
		}
		List<RFuture<Map<String, String>>> scriptFutures = new ArrayList<>();
		for (String nodeId : scriptIds) {
			scriptFutures.add(batch.<String, String>getMap(keys.script(nodeId), StringCodec.INSTANCE)
					.getAllAsync(SCRIPT_META_FIELDS));
		}
		RFuture<String> sequenceFuture = batch.<String>getBucket(keys.seq(), StringCodec.INSTANCE).getAsync();
		batch.execute();

		List<ChainMeta> chains = new ArrayList<>();
		for (int i = 0; i < chainIds.size(); i++) {
			Map<String, String> metadata = chainFutures.get(i).getNow();
			if (isEnabled(metadata)) {
				chains.add(new ChainMeta(chainIds.get(i), parseLong(metadata.get("version")), metadata.get("md5")));
			}
		}
		List<ScriptMeta> scripts = new ArrayList<>();
		for (int i = 0; i < scriptIds.size(); i++) {
			Map<String, String> metadata = scriptFutures.get(i).getNow();
			if (isEnabled(metadata)) {
				scripts.add(new ScriptMeta(scriptIds.get(i), parseLong(metadata.get("version")), metadata.get("md5"),
						valueOrNull(metadata.get("type")), valueOrNull(metadata.get("language")),
						valueOrNull(metadata.get("name"))));
			}
		}

		RuleManifest manifest = new RuleManifest();
		manifest.setChains(chains);
		manifest.setScripts(scripts);
		manifest.setLatestSeq(parseLong(sequenceFuture.getNow()));
		return manifest;
	}

	@Override
	public ChainRecord fetchChain(String chainId) {
		Map<String, String> content = readAllMap(redisson(), keys.chain(chainId));
		if (content.isEmpty()) {
			return null;
		}
		ChainRecord record = new ChainRecord();
		record.setChainId(chainId);
		record.setEl(content.get("el"));
		record.setRoute(valueOrNull(content.get("route")));
		record.setNamespace(valueOrNull(content.get("namespace")));
		record.setVersion(parseLong(content.get("version")));
		record.setMd5(content.get("md5"));
		record.setEnable(!"0".equals(content.get("enable")));
		return record;
	}

	@Override
	public ChainMeta fetchChainMeta(String chainId) {
		Map<String, String> metadata = redisson().<String, String>getMap(
				keys.chain(chainId), StringCodec.INSTANCE).getAll(CHAIN_META_FIELDS);
		if (!isEnabled(metadata)) {
			return null;
		}
		return new ChainMeta(chainId, parseLong(metadata.get("version")), metadata.get("md5"));
	}

	@Override
	public ScriptRecord fetchScript(String nodeId) {
		Map<String, String> content = readAllMap(redisson(), keys.script(nodeId));
		if (content.isEmpty()) {
			return null;
		}
		ScriptRecord record = new ScriptRecord();
		record.setNodeId(nodeId);
		record.setScript(content.get("script"));
		record.setName(valueOrNull(content.get("name")));
		record.setType(valueOrNull(content.get("type")));
		record.setLanguage(valueOrNull(content.get("language")));
		record.setVersion(parseLong(content.get("version")));
		record.setMd5(content.get("md5"));
		record.setEnable(!"0".equals(content.get("enable")));
		return record;
	}

	@Override
	public ScriptMeta fetchScriptMeta(String nodeId) {
		Map<String, String> metadata = redisson().<String, String>getMap(
				keys.script(nodeId), StringCodec.INSTANCE).getAll(SCRIPT_META_FIELDS);
		if (!isEnabled(metadata)) {
			return null;
		}
		return new ScriptMeta(nodeId, parseLong(metadata.get("version")), metadata.get("md5"),
				valueOrNull(metadata.get("type")), valueOrNull(metadata.get("language")),
				valueOrNull(metadata.get("name")));
	}

	public long fetchLatestSeq() {
		String value = redisson().<String>getBucket(keys.seq(), StringCodec.INSTANCE).get();
		return parseLong(value);
	}

	public List<ChangeRecord> fetchChangesSince(long seq) {
		return fetchChangesSince(seq, DEFAULT_CHANGELOG_BATCH_SIZE);
	}

	public List<ChangeRecord> fetchChangesSince(long seq, int limit) {
		if (limit <= 0) {
			throw new ConfigErrorException("rule-db redis change-log batch size must be positive");
		}
		RScoredSortedSet<String> log = redisson().getScoredSortedSet(keys.changelog(), StringCodec.INSTANCE);
		Collection<ScoredEntry<String>> firstEntry = log.entryRange(0, 0);
		if (seq > 0 && !firstEntry.isEmpty()) {
			double min = firstEntry.iterator().next().getScore();
			if (min > seq + 1) {
				throw new SeqGapException("redis changelog gap: since=" + seq + " min=" + (long) min);
			}
		}

		List<ChangeRecord> changes = new ArrayList<>();
		Collection<String> members = log.valueRange(seq, false, Double.POSITIVE_INFINITY, true, 0, limit);
		for (String json : members) {
			changes.add(ChangeCodec.fromJson(json));
		}
		validateSeqContinuity(changes, seq);
		return changes;
	}

	public void close() {
		connectionManager.shutdown();
	}

	private RedissonClient redisson() {
		return connectionManager.getClient();
	}

	private static Map<String, String> readAllMap(RedissonClient client, String key) {
		return client.<String, String>getMap(key, StringCodec.INSTANCE).readAllMap();
	}

	private static boolean isEnabled(Map<String, String> metadata) {
		return metadata != null && !metadata.isEmpty() && !"0".equals(metadata.get("enable"));
	}

	private static long parseLong(String value) {
		return StrUtil.isBlank(value) ? 0 : Long.parseLong(value.trim());
	}

	private static String valueOrNull(String value) {
		return StrUtil.isBlank(value) ? null : value;
	}

	private static Set<String> fields(String... values) {
		return Collections.unmodifiableSet(new HashSet<>(Arrays.asList(values)));
	}

	private static List<String> sorted(Set<String> values) {
		List<String> result = new ArrayList<>(values);
		Collections.sort(result);
		return result;
	}

	private static void validateSeqContinuity(List<ChangeRecord> changes, long currentSeq) {
		long expected = currentSeq + 1;
		long previous = Long.MIN_VALUE;
		for (ChangeRecord change : changes) {
			long value = change.getSeq();
			if (value <= currentSeq || value == previous) {
				continue;
			}
			if (value != expected) {
				throw new SeqGapException("redis changelog internal gap: since=" + currentSeq
						+ " expected=" + expected + " actual=" + value);
			}
			previous = value;
			expected = value + 1;
		}
	}
}

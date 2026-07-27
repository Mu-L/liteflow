package com.yomahub.liteflow.repository.zk;

import com.yomahub.liteflow.publisher.exception.RuleStorageException;
import com.yomahub.liteflow.repository.RuleRepository;
import com.yomahub.liteflow.repository.vo.ChainMeta;
import com.yomahub.liteflow.repository.vo.ChainRecord;
import com.yomahub.liteflow.repository.vo.RuleManifest;
import com.yomahub.liteflow.repository.vo.ScriptMeta;
import com.yomahub.liteflow.repository.vo.ScriptRecord;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class ZkRuleRepository implements RuleRepository {

	private final CuratorFramework client;
	private final ZkPaths paths;
	private final ZkRecordCodec codec;

	ZkRuleRepository(CuratorFramework client, ZkPaths paths, ZkRecordCodec codec) {
		this.client = client;
		this.paths = paths;
		this.codec = codec;
	}

	@Override
	public RuleManifest fetchManifest() {
		RootData chains = readRoot(paths.chainMetaRoot(), true);
		RootData scripts = readRoot(paths.scriptMetaRoot(), false);
		RuleManifest manifest = new RuleManifest();
		manifest.setChains(chains.chains);
		manifest.setScripts(scripts.scripts);
		manifest.setLatestSeq(Math.max(chains.revision, scripts.revision));
		return manifest;
	}

	@Override
	public ChainRecord fetchChain(String chainId) {
		Data first = get(paths.chainMeta(chainId));
		if (first == null || !codec.enabled(first.value)) { return null; }
		Data content = get(paths.chainContent(chainId));
		Data second = get(paths.chainMeta(chainId));
		ensureStable("chain", chainId, first, second);
		if (content == null) { throw missing("chain", chainId); }
		return codec.decodeChain(chainId, second.value, content.value);
	}

	@Override
	public ChainMeta fetchChainMeta(String chainId) {
		Data data = get(paths.chainMeta(chainId));
		return data == null || !codec.enabled(data.value) ? null : codec.decodeChainMeta(chainId, data.value);
	}

	@Override
	public ScriptRecord fetchScript(String nodeId) {
		Data first = get(paths.scriptMeta(nodeId));
		if (first == null || !codec.enabled(first.value)) { return null; }
		Data content = get(paths.scriptContent(nodeId));
		Data second = get(paths.scriptMeta(nodeId));
		ensureStable("script", nodeId, first, second);
		if (content == null) { throw missing("script", nodeId); }
		return codec.decodeScript(nodeId, second.value, content.value);
	}

	@Override
	public ScriptMeta fetchScriptMeta(String nodeId) {
		Data data = get(paths.scriptMeta(nodeId));
		return data == null || !codec.enabled(data.value) ? null : codec.decodeScriptMeta(nodeId, data.value);
	}

	private RootData readRoot(String root, boolean chains) {
		try {
			Stat rootStat = client.checkExists().forPath(root);
			if (rootStat == null) { return new RootData(); }
			RootData result = new RootData();
			result.revision = Math.max(rootStat.getMzxid(), rootStat.getPzxid());
			List<String> children = new ArrayList<>(client.getChildren().forPath(root));
			Collections.sort(children);
			for (String id : children) {
				Stat stat = new Stat();
				byte[] value;
				try {
					value = client.getData().storingStatIn(stat).forPath(root + "/" + id);
				}
				catch (KeeperException.NoNodeException ignored) {
					continue;
				}
				result.revision = Math.max(result.revision, stat.getMzxid());
				if (!codec.enabled(value)) { continue; }
				if (chains) { result.chains.add(codec.decodeChainMeta(id, value)); }
				else { result.scripts.add(codec.decodeScriptMeta(id, value)); }
			}
			return result;
		}
		catch (Exception e) {
			throw storage("read metadata root " + root, e);
		}
	}

	private Data get(String path) {
		try {
			Stat stat = new Stat();
			byte[] value = client.getData().storingStatIn(stat).forPath(path);
			return new Data(value, stat);
		}
		catch (KeeperException.NoNodeException e) {
			return null;
		}
		catch (Exception e) {
			throw storage("read " + path, e);
		}
	}

	private void ensureStable(String type, String id, Data first, Data second) {
		if (second == null || first.stat.getVersion() != second.stat.getVersion()
				|| codec.version(first.value) != codec.version(second.value)) {
			throw new RuleStorageException("ZooKeeper " + type + "[" + id + "] changed while reading content");
		}
	}

	private RuleStorageException missing(String type, String id) {
		return new RuleStorageException("ZooKeeper " + type + "[" + id + "] content is missing");
	}

	private RuleStorageException storage(String operation, Exception e) {
		if (e instanceof RuleStorageException) { return (RuleStorageException) e; }
		return new RuleStorageException("ZooKeeper " + operation + " failed: " + e.getMessage(), e);
	}

	private static final class Data {
		private final byte[] value;
		private final Stat stat;
		private Data(byte[] value, Stat stat) { this.value = value; this.stat = stat; }
	}

	private static final class RootData {
		private final List<ChainMeta> chains = new ArrayList<>();
		private final List<ScriptMeta> scripts = new ArrayList<>();
		private long revision;
	}
}

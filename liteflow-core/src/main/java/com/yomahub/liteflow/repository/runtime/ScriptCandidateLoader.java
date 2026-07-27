package com.yomahub.liteflow.repository.runtime;

import cn.hutool.core.util.StrUtil;
import com.yomahub.liteflow.enums.NodeTypeEnum;
import com.yomahub.liteflow.exception.ChainLoadException;
import com.yomahub.liteflow.flow.FlowBus;
import com.yomahub.liteflow.flow.element.Node;
import com.yomahub.liteflow.repository.RuleRepository;
import com.yomahub.liteflow.repository.vo.ScriptMeta;
import com.yomahub.liteflow.repository.vo.ScriptRecord;
import com.yomahub.liteflow.script.ScriptExecutorFactory;

import java.util.Objects;
import java.util.UUID;

/** Loads and compiles one stable script generation under an unpublished internal ID. */
public final class ScriptCandidateLoader {

	private final RuleRepository repository;
	private final int retryTimes;

	public ScriptCandidateLoader(RuleRepository repository, int retryTimes) {
		this.repository = repository;
		this.retryTimes = retryTimes;
	}

	public Candidate load(String nodeId, long expectedVersion, String expectedMd5) {
		ScriptMeta before = fetchMeta(nodeId);
		assertExpectedGeneration(nodeId, before, expectedVersion, expectedMd5);

		ScriptRecord record = fetchScript(nodeId);
		assertContentGeneration(nodeId, before, record);

		ScriptMeta after = fetchMeta(nodeId);
		if (!sameGeneration(before, after)) {
			throw changed(nodeId);
		}

		String candidateId = nodeId + "@ruleDbCandidate@" + expectedVersion + "@" + UUID.randomUUID();
		Node candidate = new Node(candidateId, record.getName(), scriptType(record),
				record.getScript(), record.getLanguage());
		try {
			FlowBus.compileUnpublishedScriptNode(candidate);
			return new Candidate(record, candidateId);
		}
		catch (RuntimeException e) {
			unload(candidateId, record.getLanguage());
			throw e;
		}
	}

	private void assertExpectedGeneration(String nodeId, ScriptMeta meta, long version, String md5) {
		if (meta == null || meta.getVersion() != version
				|| (md5 != null && !Objects.equals(md5, meta.getMd5()))) {
			throw changed(nodeId);
		}
	}

	private void assertContentGeneration(String nodeId, ScriptMeta meta, ScriptRecord record) {
		if (record == null || !record.isEnable() || !nodeId.equals(record.getNodeId())
				|| record.getVersion() != meta.getVersion()
				|| !Objects.equals(record.getMd5(), meta.getMd5())
				|| !Objects.equals(record.getType(), meta.getType())
				|| !Objects.equals(record.getLanguage(), meta.getLanguage())
				|| !Objects.equals(record.getName(), meta.getName())) {
			throw changed(nodeId);
		}
		scriptType(record);
	}

	private NodeTypeEnum scriptType(ScriptRecord record) {
		NodeTypeEnum type = NodeTypeEnum.getEnumByCode(record.getType());
		if (type == null || !type.isScript()) {
			throw new ChainLoadException(StrUtil.format("script node[{}] has invalid type[{}]",
					record.getNodeId(), record.getType()));
		}
		return type;
	}

	private boolean sameGeneration(ScriptMeta left, ScriptMeta right) {
		return left != null && right != null
				&& left.getVersion() == right.getVersion()
				&& Objects.equals(left.getMd5(), right.getMd5())
				&& Objects.equals(left.getType(), right.getType())
				&& Objects.equals(left.getLanguage(), right.getLanguage())
				&& Objects.equals(left.getName(), right.getName());
	}

	private ScriptMeta fetchMeta(String nodeId) {
		RuntimeException last = null;
		for (int i = 0; i <= retryTimes; i++) {
			try {
				return repository.fetchScriptMeta(nodeId);
			}
			catch (RuntimeException e) {
				last = e;
			}
		}
		throw fetchFailed("metadata", nodeId, last);
	}

	private ScriptRecord fetchScript(String nodeId) {
		RuntimeException last = null;
		for (int i = 0; i <= retryTimes; i++) {
			try {
				return repository.fetchScript(nodeId);
			}
			catch (RuntimeException e) {
				last = e;
			}
		}
		throw fetchFailed("content", nodeId, last);
	}

	private ChainLoadException fetchFailed(String part, String nodeId, RuntimeException error) {
		return new ChainLoadException(StrUtil.format("fetch script {}[{}] failed after {} retries: {}",
				part, nodeId, retryTimes, error == null ? StrUtil.EMPTY : error.getMessage()));
	}

	private ChainLoadException changed(String nodeId) {
		return new ChainLoadException(StrUtil.format("script node[{}] changed or was deleted while loading", nodeId));
	}

	private static void unload(String nodeId, String language) {
		try {
			ScriptExecutorFactory.loadInstance().getScriptExecutor(language).unLoad(nodeId);
		}
		catch (RuntimeException ignored) {
		}
	}

	public static final class Candidate implements AutoCloseable {

		private final ScriptRecord record;
		private final String candidateId;

		private Candidate(ScriptRecord record, String candidateId) {
			this.record = record;
			this.candidateId = candidateId;
		}

		public ScriptRecord getRecord() {
			return record;
		}

		@Override
		public void close() {
			unload(candidateId, record.getLanguage());
		}
	}
}

package com.yomahub.liteflow.repository.etcd;

import cn.hutool.core.util.StrUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.yomahub.liteflow.publisher.exception.RuleStorageException;
import com.yomahub.liteflow.repository.vo.ChainMeta;
import com.yomahub.liteflow.repository.vo.ChainRecord;
import com.yomahub.liteflow.repository.vo.ScriptMeta;
import com.yomahub.liteflow.repository.vo.ScriptRecord;

import java.io.IOException;

final class EtcdRecordCodec {

	private final ObjectMapper mapper = new ObjectMapper();

	String encodeChainMeta(ChainRecord record) {
		ObjectNode node = commonMeta(record.getVersion(), record.getMd5(), record.isEnable());
		putNullable(node, "route", record.getRoute());
		putNullable(node, "namespace", record.getNamespace());
		return write(node);
	}

	String encodeScriptMeta(ScriptRecord record) {
		ObjectNode node = commonMeta(record.getVersion(), record.getMd5(), record.isEnable());
		putNullable(node, "type", record.getType());
		putNullable(node, "language", record.getLanguage());
		putNullable(node, "name", record.getName());
		return write(node);
	}

	String encodeContent(long version, String content) {
		ObjectNode node = mapper.createObjectNode();
		node.put("version", version);
		node.put("content", content);
		return write(node);
	}

	ChainMeta decodeChainMeta(String chainId, String value) {
		JsonNode node = read(value);
		validateMeta(node, "chain", chainId);
		return new ChainMeta(chainId, node.get("version").asLong(), node.get("md5").asText());
	}

	ScriptMeta decodeScriptMeta(String nodeId, String value) {
		JsonNode node = read(value);
		validateMeta(node, "script", nodeId);
		String type = text(node, "type");
		if (StrUtil.isBlank(type)) {
			throw invalid("script", nodeId, "type is missing");
		}
		return new ScriptMeta(nodeId, node.get("version").asLong(), node.get("md5").asText(),
				type, text(node, "language"), text(node, "name"));
	}

	ChainRecord decodeChain(String chainId, String metadata, String content) {
		JsonNode meta = read(metadata);
		validateMeta(meta, "chain", chainId);
		Content decoded = decodeContent("chain", chainId, content);
		ensureSameVersion("chain", chainId, meta.get("version").asLong(), decoded.version);
		ChainRecord record = new ChainRecord();
		record.setChainId(chainId);
		record.setVersion(decoded.version);
		record.setMd5(meta.get("md5").asText());
		record.setEnable(enabled(meta));
		record.setRoute(text(meta, "route"));
		record.setNamespace(text(meta, "namespace"));
		record.setEl(decoded.value);
		return record;
	}

	ScriptRecord decodeScript(String nodeId, String metadata, String content) {
		JsonNode meta = read(metadata);
		validateMeta(meta, "script", nodeId);
		Content decoded = decodeContent("script", nodeId, content);
		ensureSameVersion("script", nodeId, meta.get("version").asLong(), decoded.version);
		ScriptRecord record = new ScriptRecord();
		record.setNodeId(nodeId);
		record.setVersion(decoded.version);
		record.setMd5(meta.get("md5").asText());
		record.setEnable(enabled(meta));
		record.setType(text(meta, "type"));
		record.setLanguage(text(meta, "language"));
		record.setName(text(meta, "name"));
		record.setScript(decoded.value);
		return record;
	}

	boolean enabled(String metadata) {
		return enabled(read(metadata));
	}

	long version(String metadata) {
		JsonNode node = read(metadata);
		JsonNode version = node.get("version");
		return version == null ? 0 : version.asLong();
	}

	private ObjectNode commonMeta(long version, String md5, boolean enabled) {
		ObjectNode node = mapper.createObjectNode();
		node.put("version", version);
		node.put("md5", md5);
		node.put("enable", enabled);
		return node;
	}

	private Content decodeContent(String type, String id, String value) {
		JsonNode node = read(value);
		if (!node.has("version") || node.get("version").asLong() <= 0 || !node.has("content")) {
			throw invalid(type, id, "content version or body is missing");
		}
		return new Content(node.get("version").asLong(), node.get("content").asText());
	}

	private void validateMeta(JsonNode node, String type, String id) {
		if (!node.has("version") || node.get("version").asLong() <= 0
				|| !node.has("md5") || StrUtil.isBlank(node.get("md5").asText())) {
			throw invalid(type, id, "metadata version or md5 is missing");
		}
	}

	private void ensureSameVersion(String type, String id, long metadataVersion, long contentVersion) {
		if (metadataVersion != contentVersion) {
			throw invalid(type, id, "metadata/content version mismatch: "
					+ metadataVersion + "/" + contentVersion);
		}
	}

	private boolean enabled(JsonNode node) {
		return !node.has("enable") || node.get("enable").asBoolean();
	}

	private JsonNode read(String value) {
		if (value == null) {
			throw new RuleStorageException("etcd rule value is missing");
		}
		try {
			return mapper.readTree(value);
		}
		catch (IOException e) {
			throw new RuleStorageException("invalid etcd rule JSON: " + e.getMessage(), e);
		}
	}

	private String write(JsonNode node) {
		try {
			return mapper.writeValueAsString(node);
		}
		catch (IOException e) {
			throw new RuleStorageException("cannot encode etcd rule JSON", e);
		}
	}

	private void putNullable(ObjectNode node, String field, String value) {
		if (value == null) {
			node.putNull(field);
		}
		else {
			node.put(field, value);
		}
	}

	private String text(JsonNode node, String field) {
		JsonNode value = node.get(field);
		return value == null || value.isNull() ? null : value.asText();
	}

	private RuleStorageException invalid(String type, String id, String message) {
		return new RuleStorageException("etcd " + type + "[" + id + "] " + message);
	}

	private static final class Content {
		private final long version;
		private final String value;
		private Content(long version, String value) {
			this.version = version;
			this.value = value;
		}
	}
}

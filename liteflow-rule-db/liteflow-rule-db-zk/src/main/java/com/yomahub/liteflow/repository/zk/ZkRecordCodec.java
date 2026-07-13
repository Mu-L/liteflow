package com.yomahub.liteflow.repository.zk;

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
import java.nio.charset.StandardCharsets;

final class ZkRecordCodec {

	private final ObjectMapper mapper = new ObjectMapper();

	byte[] encodeChainMeta(ChainRecord record) {
		ObjectNode node = commonMeta(record.getVersion(), record.getMd5(), record.isEnable());
		putNullable(node, "route", record.getRoute());
		putNullable(node, "namespace", record.getNamespace());
		return write(node);
	}

	byte[] encodeScriptMeta(ScriptRecord record) {
		ObjectNode node = commonMeta(record.getVersion(), record.getMd5(), record.isEnable());
		putNullable(node, "type", record.getType());
		putNullable(node, "language", record.getLanguage());
		putNullable(node, "name", record.getName());
		return write(node);
	}

	byte[] encodeContent(long version, String content) {
		ObjectNode node = mapper.createObjectNode();
		node.put("version", version);
		node.put("content", content);
		return write(node);
	}

	ChainMeta decodeChainMeta(String id, byte[] value) {
		JsonNode node = read(value);
		validateMeta(node, "chain", id);
		return new ChainMeta(id, node.get("version").asLong(), node.get("md5").asText());
	}

	ScriptMeta decodeScriptMeta(String id, byte[] value) {
		JsonNode node = read(value);
		validateMeta(node, "script", id);
		String type = text(node, "type");
		if (StrUtil.isBlank(type)) { throw invalid("script", id, "type is missing"); }
		return new ScriptMeta(id, node.get("version").asLong(), node.get("md5").asText(),
				type, text(node, "language"), text(node, "name"));
	}

	ChainRecord decodeChain(String id, byte[] metadata, byte[] content) {
		JsonNode meta = read(metadata);
		validateMeta(meta, "chain", id);
		Content body = content("chain", id, content);
		ensureSameVersion("chain", id, meta.get("version").asLong(), body.version);
		ChainRecord record = new ChainRecord();
		record.setChainId(id);
		record.setVersion(body.version);
		record.setMd5(meta.get("md5").asText());
		record.setEnable(enabled(meta));
		record.setRoute(text(meta, "route"));
		record.setNamespace(text(meta, "namespace"));
		record.setEl(body.value);
		return record;
	}

	ScriptRecord decodeScript(String id, byte[] metadata, byte[] content) {
		JsonNode meta = read(metadata);
		validateMeta(meta, "script", id);
		Content body = content("script", id, content);
		ensureSameVersion("script", id, meta.get("version").asLong(), body.version);
		ScriptRecord record = new ScriptRecord();
		record.setNodeId(id);
		record.setVersion(body.version);
		record.setMd5(meta.get("md5").asText());
		record.setEnable(enabled(meta));
		record.setType(text(meta, "type"));
		record.setLanguage(text(meta, "language"));
		record.setName(text(meta, "name"));
		record.setScript(body.value);
		return record;
	}

	boolean enabled(byte[] metadata) { return enabled(read(metadata)); }
	long version(byte[] metadata) {
		JsonNode node = read(metadata);
		return node.has("version") ? node.get("version").asLong() : 0;
	}

	private ObjectNode commonMeta(long version, String md5, boolean enabled) {
		ObjectNode node = mapper.createObjectNode();
		node.put("version", version);
		node.put("md5", md5);
		node.put("enable", enabled);
		return node;
	}

	private Content content(String type, String id, byte[] value) {
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

	private boolean enabled(JsonNode node) { return !node.has("enable") || node.get("enable").asBoolean(); }

	private JsonNode read(byte[] value) {
		if (value == null) { throw new RuleStorageException("ZooKeeper rule value is missing"); }
		try { return mapper.readTree(value); }
		catch (IOException e) { throw new RuleStorageException("invalid ZooKeeper rule JSON: " + e.getMessage(), e); }
	}

	private byte[] write(JsonNode node) { return node.toString().getBytes(StandardCharsets.UTF_8); }

	private void putNullable(ObjectNode node, String field, String value) {
		if (value == null) { node.putNull(field); }
		else { node.put(field, value); }
	}

	private String text(JsonNode node, String field) {
		JsonNode value = node.get(field);
		return value == null || value.isNull() ? null : value.asText();
	}

	private RuleStorageException invalid(String type, String id, String message) {
		return new RuleStorageException("ZooKeeper " + type + "[" + id + "] " + message);
	}

	private static final class Content {
		private final long version;
		private final String value;
		private Content(long version, String value) { this.version = version; this.value = value; }
	}
}

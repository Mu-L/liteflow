package com.yomahub.liteflow.repository.nacos;

import cn.hutool.core.util.StrUtil;
import cn.hutool.crypto.SecureUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.yomahub.liteflow.publisher.exception.RuleStorageException;
import com.yomahub.liteflow.repository.vo.ChainRecord;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import com.yomahub.liteflow.repository.vo.ScriptRecord;

import java.util.LinkedHashMap;
import java.util.Map;

/** Strict JSON codec for the atomically replaced Nacos catalog document. */
final class NacosCatalogCodec {

	private static final int SCHEMA_VERSION = 1;

	private final ObjectMapper mapper = new ObjectMapper();

	NacosCatalog decode(String content) {
		if (content == null) {
			return NacosCatalog.empty();
		}
		if (content.trim().isEmpty()) {
			throw invalid("catalog content is blank");
		}
		try {
			JsonNode root = mapper.readTree(content);
			if (root == null || !root.isObject()) {
				throw invalid("catalog root must be a JSON object");
			}
			long schemaVersion = requiredLong(root, "schemaVersion", 1);
			if (schemaVersion != SCHEMA_VERSION) {
				throw invalid("unsupported schemaVersion[" + schemaVersion + "]");
			}
			long sequence = requiredLong(root, "sequence", 0);
			Map<String, ChainRecord> chains = decodeChains(requiredArray(root, "chains"));
			Map<String, ScriptRecord> scripts = decodeScripts(requiredArray(root, "scripts"));
			ChangeRecord change = decodeChange(root.get("lastChange"));
			validateChange(sequence, change, chains, scripts);
			return new NacosCatalog(sequence, chains, scripts, change);
		}
		catch (RuleStorageException e) {
			throw e;
		}
		catch (JsonProcessingException e) {
			throw new RuleStorageException("invalid Nacos rule catalog JSON: " + e.getOriginalMessage(), e);
		}
	}

	String encode(NacosCatalog catalog) {
		ObjectNode root = mapper.createObjectNode();
		root.put("schemaVersion", SCHEMA_VERSION);
		root.put("sequence", catalog.sequence());
		ArrayNode chains = root.putArray("chains");
		for (ChainRecord record : catalog.chains().values()) {
			ObjectNode node = chains.addObject();
			node.put("chainId", record.getChainId());
			node.put("el", record.getEl());
			putNullable(node, "route", record.getRoute());
			putNullable(node, "namespace", record.getNamespace());
			node.put("version", record.getVersion());
			node.put("md5", record.getMd5());
			node.put("enable", record.isEnable());
		}
		ArrayNode scripts = root.putArray("scripts");
		for (ScriptRecord record : catalog.scripts().values()) {
			ObjectNode node = scripts.addObject();
			node.put("nodeId", record.getNodeId());
			node.put("script", record.getScript());
			putNullable(node, "name", record.getName());
			node.put("type", record.getType());
			putNullable(node, "language", record.getLanguage());
			node.put("version", record.getVersion());
			node.put("md5", record.getMd5());
			node.put("enable", record.isEnable());
		}
		ChangeRecord change = catalog.lastChange();
		if (change == null) {
			root.putNull("lastChange");
		}
		else {
			ObjectNode node = root.putObject("lastChange");
			node.put("seq", change.getSeq());
			node.put("targetType", change.getTargetType().name());
			node.put("targetId", change.getTargetId());
			node.put("op", change.getOp().name());
			node.put("version", change.getVersion());
		}
		return root.toString();
	}

	String md5(String content) {
		return SecureUtil.md5(content == null ? "" : content);
	}

	private Map<String, ChainRecord> decodeChains(ArrayNode nodes) {
		Map<String, ChainRecord> result = new LinkedHashMap<>();
		for (JsonNode node : nodes) {
			if (!node.isObject()) { throw invalid("chain entry must be an object"); }
			ChainRecord record = new ChainRecord();
			record.setChainId(requiredText(node, "chainId"));
			record.setEl(requiredText(node, "el"));
			record.setRoute(nullableText(node, "route"));
			record.setNamespace(nullableText(node, "namespace"));
			record.setVersion(requiredLong(node, "version", 1));
			record.setMd5(requiredText(node, "md5"));
			record.setEnable(enabled(node));
			if (!record.isEnable()) { throw invalid("chain[" + record.getChainId() + "] is disabled"); }
			if (!record.getMd5().equals(SecureUtil.md5(record.getEl()))) {
				throw invalid("chain[" + record.getChainId() + "] md5 does not match EL content");
			}
			if (result.put(record.getChainId(), record) != null) {
				throw invalid("duplicate chain[" + record.getChainId() + "]");
			}
		}
		return result;
	}

	private Map<String, ScriptRecord> decodeScripts(ArrayNode nodes) {
		Map<String, ScriptRecord> result = new LinkedHashMap<>();
		for (JsonNode node : nodes) {
			if (!node.isObject()) { throw invalid("script entry must be an object"); }
			ScriptRecord record = new ScriptRecord();
			record.setNodeId(requiredText(node, "nodeId"));
			record.setScript(requiredText(node, "script"));
			record.setName(nullableText(node, "name"));
			record.setType(requiredText(node, "type"));
			record.setLanguage(nullableText(node, "language"));
			record.setVersion(requiredLong(node, "version", 1));
			record.setMd5(requiredText(node, "md5"));
			record.setEnable(enabled(node));
			if (!record.isEnable()) { throw invalid("script[" + record.getNodeId() + "] is disabled"); }
			if (!record.getMd5().equals(SecureUtil.md5(record.getScript()))) {
				throw invalid("script[" + record.getNodeId() + "] md5 does not match content");
			}
			if (result.put(record.getNodeId(), record) != null) {
				throw invalid("duplicate script[" + record.getNodeId() + "]");
			}
		}
		return result;
	}

	private ChangeRecord decodeChange(JsonNode node) {
		if (node == null || node.isNull()) { return null; }
		if (!node.isObject()) { throw invalid("lastChange must be an object or null"); }
		try {
			return new ChangeRecord(requiredLong(node, "seq", 1),
					ChangeRecord.TargetType.valueOf(requiredText(node, "targetType")),
					requiredText(node, "targetId"), ChangeRecord.Op.valueOf(requiredText(node, "op")),
					requiredLong(node, "version", 0));
		}
		catch (IllegalArgumentException e) {
			throw invalid("lastChange contains an unknown targetType or operation");
		}
	}

	private void validateChange(long sequence, ChangeRecord change,
			Map<String, ChainRecord> chains, Map<String, ScriptRecord> scripts) {
		if (sequence == 0) {
			if (change != null) { throw invalid("sequence zero must not have lastChange"); }
			return;
		}
		if (change == null || change.getSeq() != sequence) {
			throw invalid("lastChange sequence does not match catalog sequence[" + sequence + "]");
		}
		if (change.getOp() == ChangeRecord.Op.UPSERT && change.getVersion() <= 0) {
			throw invalid("lastChange UPSERT version must be positive");
		}
		long storedVersion;
		if (change.getTargetType() == ChangeRecord.TargetType.CHAIN) {
			ChainRecord record = chains.get(change.getTargetId());
			storedVersion = record == null ? 0 : record.getVersion();
		}
		else {
			ScriptRecord record = scripts.get(change.getTargetId());
			storedVersion = record == null ? 0 : record.getVersion();
		}
		if (change.getOp() == ChangeRecord.Op.UPSERT && storedVersion != change.getVersion()) {
			throw invalid("lastChange UPSERT does not match its catalog record");
		}
		if (change.getOp() == ChangeRecord.Op.DELETE && storedVersion != 0) {
			throw invalid("lastChange DELETE target still exists in the catalog");
		}
	}

	private ArrayNode requiredArray(JsonNode node, String field) {
		JsonNode value = node.get(field);
		if (value == null || !value.isArray()) {
			throw invalid(field + " must be an array");
		}
		return (ArrayNode) value;
	}

	private long requiredLong(JsonNode node, String field, long minimum) {
		JsonNode value = node.get(field);
		if (value == null || !value.isIntegralNumber() || !value.canConvertToLong()
				|| value.asLong() < minimum) {
			throw invalid(field + " must be an integer greater than or equal to " + minimum);
		}
		return value.asLong();
	}

	private String requiredText(JsonNode node, String field) {
		String value = nullableText(node, field);
		if (StrUtil.isBlank(value)) {
			throw invalid(field + " must not be blank");
		}
		return value;
	}

	private String nullableText(JsonNode node, String field) {
		JsonNode value = node.get(field);
		if (value == null || value.isNull()) { return null; }
		if (!value.isTextual()) { throw invalid(field + " must be a string or null"); }
		return value.asText();
	}

	private boolean enabled(JsonNode node) {
		JsonNode value = node.get("enable");
		if (value == null) { return true; }
		if (!value.isBoolean()) { throw invalid("enable must be a boolean"); }
		return value.asBoolean();
	}

	private void putNullable(ObjectNode node, String field, String value) {
		if (value == null) { node.putNull(field); }
		else { node.put(field, value); }
	}

	private RuleStorageException invalid(String message) {
		return new RuleStorageException("invalid Nacos rule catalog: " + message);
	}
}

package com.yomahub.liteflow.repository.mongodb;

import com.mongodb.ClientSessionOptions;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.yomahub.liteflow.exception.ConfigErrorException;
import com.yomahub.liteflow.exception.SeqGapException;
import com.yomahub.liteflow.repository.RuleRepository;
import com.yomahub.liteflow.repository.vo.ChainMeta;
import com.yomahub.liteflow.repository.vo.ChainRecord;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import com.yomahub.liteflow.repository.vo.RuleManifest;
import com.yomahub.liteflow.repository.vo.ScriptMeta;
import com.yomahub.liteflow.repository.vo.ScriptRecord;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Sorts.ascending;

/** MongoDB authoritative repository using projected manifest reads. */
public final class MongoRuleRepository implements RuleRepository {

	private static final TransactionOptions SNAPSHOT_OPTIONS = TransactionOptions.builder()
			.readConcern(ReadConcern.SNAPSHOT).readPreference(ReadPreference.primary())
			.writeConcern(WriteConcern.MAJORITY).build();

	private final MongoClient client;
	private final MongoDatabase database;
	private final MongoCollections names;
	private final String applicationName;

	MongoRuleRepository(MongoClient client, MongoDatabase database, MongoCollections names, String applicationName) {
		this.client = client;
		this.database = database;
		this.names = names;
		this.applicationName = MongoStorageValidator.applicationNameOrDefault(applicationName);
	}

	@Override
	public RuleManifest fetchManifest() {
		try (ClientSession session = client.startSession(ClientSessionOptions.builder().causallyConsistent(true).build())) {
			return session.withTransaction(() -> readManifest(session), SNAPSHOT_OPTIONS);
		}
	}

	private RuleManifest readManifest(ClientSession session) {
		List<ChainMeta> chainMetas = new ArrayList<>();
		chains().find(session, and(eq("applicationName", applicationName), eq("enable", true)))
				.projection(include("chainId", "version", "contentMd5"))
				.forEach(document -> chainMetas.add(new ChainMeta(document.getString("chainId"),
						number(document, "version"), document.getString("contentMd5"))));

		List<ScriptMeta> scriptMetas = new ArrayList<>();
		scripts().find(session, and(eq("applicationName", applicationName), eq("enable", true)))
				.projection(include("nodeId", "version", "contentMd5", "type", "language", "name"))
				.forEach(document -> scriptMetas.add(new ScriptMeta(document.getString("nodeId"),
						number(document, "version"), document.getString("contentMd5"), document.getString("type"),
						document.getString("language"), document.getString("name"))));

		RuleManifest manifest = new RuleManifest();
		manifest.setChains(chainMetas);
		manifest.setScripts(scriptMetas);
		manifest.setLatestSeq(latestSeq(session));
		return manifest;
	}

	@Override
	public ChainRecord fetchChain(String chainId) {
		MongoStorageValidator.validateTargetId("chainId", chainId);
		Document document = chains().find(idFilter(chainId)).first();
		if (document == null) { return null; }
		ChainRecord record = new ChainRecord();
		record.setChainId(document.getString("chainId"));
		record.setEl(document.getString("el"));
		record.setRoute(document.getString("route"));
		record.setNamespace(document.getString("namespace"));
		record.setVersion(number(document, "version"));
		record.setMd5(document.getString("contentMd5"));
		record.setEnable(Boolean.TRUE.equals(document.getBoolean("enable")));
		return record;
	}

	@Override
	public ChainMeta fetchChainMeta(String chainId) {
		MongoStorageValidator.validateTargetId("chainId", chainId);
		Document document = chains().find(and(idFilter(chainId), eq("enable", true)))
				.projection(include("chainId", "version", "contentMd5")).first();
		return document == null ? null : new ChainMeta(document.getString("chainId"),
				number(document, "version"), document.getString("contentMd5"));
	}

	@Override
	public ScriptRecord fetchScript(String nodeId) {
		MongoStorageValidator.validateTargetId("nodeId", nodeId);
		Document document = scripts().find(idFilter(nodeId)).first();
		if (document == null) { return null; }
		ScriptRecord record = new ScriptRecord();
		record.setNodeId(document.getString("nodeId"));
		record.setScript(document.getString("script"));
		record.setName(document.getString("name"));
		record.setType(document.getString("type"));
		record.setLanguage(document.getString("language"));
		record.setVersion(number(document, "version"));
		record.setMd5(document.getString("contentMd5"));
		record.setEnable(Boolean.TRUE.equals(document.getBoolean("enable")));
		return record;
	}

	@Override
	public ScriptMeta fetchScriptMeta(String nodeId) {
		MongoStorageValidator.validateTargetId("nodeId", nodeId);
		Document document = scripts().find(and(idFilter(nodeId), eq("enable", true)))
				.projection(include("nodeId", "version", "contentMd5", "type", "language", "name")).first();
		return document == null ? null : new ScriptMeta(document.getString("nodeId"),
				number(document, "version"), document.getString("contentMd5"), document.getString("type"),
				document.getString("language"), document.getString("name"));
	}

	public long fetchLatestSeq() {
		Document document = sequences().find(eq("_id", applicationName)).projection(include("seq")).first();
		return document == null ? 0 : number(document, "seq");
	}

	public List<ChangeRecord> fetchChangesSince(long sequence, int limit) {
		if (limit <= 0) { throw new ConfigErrorException("rule-db mongodb change-log batch size must be positive"); }
		List<Document> documents = changes().find(and(eq("applicationName", applicationName), gt("seq", sequence)))
				.sort(ascending("seq")).limit(limit).into(new ArrayList<>());
		List<ChangeRecord> result = new ArrayList<>();
		long expected = sequence + 1;
		for (Document document : documents) {
			long seq = number(document, "seq");
			if (seq != expected) {
				throw new SeqGapException("mongodb changelog gap: since=" + sequence + " expected=" + expected
						+ " actual=" + seq);
			}
			try {
				result.add(new ChangeRecord(seq, ChangeRecord.TargetType.valueOf(document.getString("targetType")),
						document.getString("targetId"), ChangeRecord.Op.valueOf(document.getString("operation")),
						number(document, "version")));
			}
			catch (RuntimeException e) {
				throw new MongoChangeLogCorruptionException("rule-db mongodb change-log document[" + seq + "] is invalid", e);
			}
			expected++;
		}
		return result;
	}

	private long latestSeq(ClientSession session) {
		Document document = sequences().find(session, eq("_id", applicationName)).projection(include("seq")).first();
		return document == null ? 0 : number(document, "seq");
	}

	private Document id(String targetId) {
		return new Document("applicationName", applicationName).append("targetId", targetId);
	}

	private org.bson.conversions.Bson idFilter(String targetId) { return eq("_id", id(targetId)); }
	private MongoCollection<Document> chains() { return database.getCollection(names.chains()); }
	private MongoCollection<Document> scripts() { return database.getCollection(names.scripts()); }
	private MongoCollection<Document> changes() { return database.getCollection(names.changes()); }
	private MongoCollection<Document> sequences() { return database.getCollection(names.sequences()); }

	private static long number(Document document, String key) {
		Object value = document.get(key);
		return value instanceof Number ? ((Number) value).longValue() : 0;
	}
}

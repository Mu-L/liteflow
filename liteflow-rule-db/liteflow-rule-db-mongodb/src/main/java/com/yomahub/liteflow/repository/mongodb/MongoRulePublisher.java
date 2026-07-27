package com.yomahub.liteflow.repository.mongodb;

import cn.hutool.crypto.SecureUtil;
import com.mongodb.MongoException;
import com.mongodb.MongoWriteException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.yomahub.liteflow.log.LFLog;
import com.yomahub.liteflow.log.LFLoggerManager;
import com.yomahub.liteflow.publisher.PublishChainRequest;
import com.yomahub.liteflow.publisher.PublishResult;
import com.yomahub.liteflow.publisher.PublishScriptRequest;
import com.yomahub.liteflow.publisher.RemoveRuleRequest;
import com.yomahub.liteflow.publisher.RulePublisher;
import com.yomahub.liteflow.publisher.exception.RuleStorageException;
import com.yomahub.liteflow.publisher.exception.VersionConflictException;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import org.bson.Document;

import java.util.Date;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Updates.inc;

/** MongoDB transaction implementation of the unified publisher API. */
final class MongoRulePublisher implements RulePublisher {

	private static final LFLog LOG = LFLoggerManager.getLogger(MongoRulePublisher.class);

	private static final int MAX_CONCURRENT_RETRIES = 8;
	private static final TransactionOptions TRANSACTION_OPTIONS = TransactionOptions.builder()
			.readConcern(ReadConcern.SNAPSHOT).readPreference(ReadPreference.primary())
			.writeConcern(WriteConcern.MAJORITY).build();

	private final MongoConnectionManager connection;
	private final MongoDatabase database;
	private final MongoCollections names;
	private final String applicationName;

	MongoRulePublisher(MongoPublisherConfig config) {
		this.connection = new MongoConnectionManager(config);
		try {
			this.database = connection.database();
			this.names = new MongoCollections(config.getCollectionPrefix());
			this.applicationName = MongoStorageValidator.applicationNameOrDefault(config.applicationName());
			MongoSchema.ensureIndexes(database, names);
		}
		catch (RuntimeException e) {
			connection.close();
			throw e;
		}
	}

	@Override
	public PublishResult publishChain(PublishChainRequest request) {
		MongoStorageValidator.validateChainRequest(request);
		return inTransaction("publish chain", session -> publish(session, chains(), request.getChainId(),
				request.getExpectedVersion(), ChangeRecord.TargetType.CHAIN, version -> new Document("_id", id(request.getChainId()))
						.append("applicationName", applicationName).append("chainId", request.getChainId())
						.append("el", request.getEl()).append("route", request.getRoute())
						.append("namespace", request.getNamespace()).append("version", version)
						.append("contentMd5", SecureUtil.md5(request.getEl())).append("enable", true)
						.append("updatedAt", new Date())));
	}

	@Override
	public PublishResult publishScript(PublishScriptRequest request) {
		MongoStorageValidator.validateScriptRequest(request);
		return inTransaction("publish script", session -> publish(session, scripts(), request.getNodeId(),
				request.getExpectedVersion(), ChangeRecord.TargetType.SCRIPT, version -> new Document("_id", id(request.getNodeId()))
						.append("applicationName", applicationName).append("nodeId", request.getNodeId())
						.append("script", request.getScript()).append("name", request.getName())
						.append("type", request.getType()).append("language", request.getLanguage())
						.append("version", version).append("contentMd5", SecureUtil.md5(request.getScript()))
						.append("enable", true).append("updatedAt", new Date())));
	}

	@Override
	public PublishResult removeChain(RemoveRuleRequest request) {
		MongoStorageValidator.validateRemoveRequest(request);
		return inTransaction("remove chain", session -> remove(session, chains(), request,
				ChangeRecord.TargetType.CHAIN));
	}

	@Override
	public PublishResult removeScript(RemoveRuleRequest request) {
		MongoStorageValidator.validateRemoveRequest(request);
		return inTransaction("remove script", session -> remove(session, scripts(), request,
				ChangeRecord.TargetType.SCRIPT));
	}

	@Override public void close() { connection.close(); }

	private PublishResult publish(ClientSession session, MongoCollection<Document> collection, String targetId,
			Long expected, ChangeRecord.TargetType targetType, Encoder encoder) {
		Document current = collection.find(session, eq("_id", id(targetId))).projection(include("version")).first();
		long currentVersion = number(current, "version");
		if (expected != null && expected != currentVersion) {
			throw conflict(targetType, targetId, expected, currentVersion);
		}
		long nextVersion = currentVersion + 1;
		Document replacement = encoder.encode(nextVersion);
		if (current == null) {
			try { collection.insertOne(session, replacement); }
			catch (MongoWriteException e) {
				if (!isDuplicateKey(e)) { throw e; }
				if (expected != null) { throw conflict(targetType, targetId, expected, 1); }
				throw new RetryConcurrentPublishException();
			}
		}
		else {
			UpdateResult update = collection.replaceOne(session,
					and(eq("_id", id(targetId)), eq("version", currentVersion)), replacement);
			if (update.getModifiedCount() != 1) {
				if (expected != null) { throw conflict(targetType, targetId, expected, currentVersion); }
				throw new RetryConcurrentPublishException();
			}
		}
		long sequence = nextSequence(session);
		insertChange(session, sequence, targetType, targetId, ChangeRecord.Op.UPSERT, nextVersion);
		return result(targetId, targetType, ChangeRecord.Op.UPSERT, nextVersion, sequence);
	}

	private PublishResult remove(ClientSession session, MongoCollection<Document> collection,
			RemoveRuleRequest request, ChangeRecord.TargetType targetType) {
		Document current = collection.find(session, eq("_id", id(request.getTargetId())))
				.projection(include("version")).first();
		long currentVersion = number(current, "version");
		Long expected = request.getExpectedVersion();
		if (expected != null && (expected == 0 || expected != currentVersion)) {
			throw conflict(targetType, request.getTargetId(), expected, currentVersion);
		}
		if (current != null) {
			DeleteResult delete = collection.deleteOne(session,
					and(eq("_id", id(request.getTargetId())), eq("version", currentVersion)));
			if (delete.getDeletedCount() != 1) {
				if (expected != null) { throw conflict(targetType, request.getTargetId(), expected, currentVersion); }
				throw new RetryConcurrentPublishException();
			}
		}
		long sequence = nextSequence(session);
		insertChange(session, sequence, targetType, request.getTargetId(), ChangeRecord.Op.DELETE, currentVersion);
		return result(request.getTargetId(), targetType, ChangeRecord.Op.DELETE, currentVersion, sequence);
	}

	private long nextSequence(ClientSession session) {
		Document sequence = sequences().findOneAndUpdate(session, eq("_id", applicationName), inc("seq", 1L),
				new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER));
		if (sequence == null) { throw new RuleStorageException("MongoDB sequence update returned no document"); }
		return number(sequence, "seq");
	}

	private void insertChange(ClientSession session, long sequence, ChangeRecord.TargetType type,
			String targetId, ChangeRecord.Op operation, long version) {
		Document changeId = new Document("applicationName", applicationName).append("seq", sequence);
		changes().insertOne(session, new Document("_id", changeId).append("applicationName", applicationName)
				.append("seq", sequence).append("targetType", type.name()).append("targetId", targetId)
				.append("operation", operation.name()).append("version", version).append("createdAt", new Date()));
	}

	private PublishResult inTransaction(String operation, Work work) {
		for (int attempt = 0; attempt < MAX_CONCURRENT_RETRIES; attempt++) {
			try (ClientSession session = connection.client().startSession()) {
				return session.withTransaction(() -> work.execute(session), TRANSACTION_OPTIONS);
			}
			catch (VersionConflictException e) { throw e; }
			catch (RetryConcurrentPublishException e) {
				if (attempt + 1 == MAX_CONCURRENT_RETRIES) {
					throw new RuleStorageException("MongoDB " + operation + " exceeded concurrent retry limit");
				}
			}
			catch (MongoException e) {
				String hint = MongoErrors.isTransactionUnsupported(e)
						? "; MongoDB Rule-DB publishing requires a replica set or sharded cluster" : "";
				throw new RuleStorageException("MongoDB " + operation + " failed: " + e.getMessage() + hint, e);
			}
		}
		throw new RuleStorageException("MongoDB " + operation + " failed");
	}

	private boolean isDuplicateKey(MongoWriteException e) { return e.getError().getCode() == 11000; }

	private Document id(String targetId) {
		return new Document("applicationName", applicationName).append("targetId", targetId);
	}

	private long number(Document document, String key) {
		if (document == null) { return 0; }
		Object value = document.get(key);
		if (value instanceof Number) { return ((Number) value).longValue(); }
		LOG.debug("rule-db mongodb field[{}] is missing or not numeric, defaulting to 0", key);
		return 0;
	}

	private MongoCollection<Document> chains() { return database.getCollection(names.chains()); }
	private MongoCollection<Document> scripts() { return database.getCollection(names.scripts()); }
	private MongoCollection<Document> changes() { return database.getCollection(names.changes()); }
	private MongoCollection<Document> sequences() { return database.getCollection(names.sequences()); }

	private PublishResult result(String targetId, ChangeRecord.TargetType type, ChangeRecord.Op operation,
			long version, long sequence) {
		return PublishResult.builder().targetId(targetId).targetType(type).operation(operation)
				.version(version).sequence(sequence).build();
	}

	private VersionConflictException conflict(ChangeRecord.TargetType type, String id, long expected, long current) {
		return new VersionConflictException(type.name().toLowerCase() + "[" + id + "] expected version["
				+ expected + "] but current version is[" + current + "]");
	}

	@FunctionalInterface private interface Encoder { Document encode(long version); }
	@FunctionalInterface private interface Work { PublishResult execute(ClientSession session); }
	private static final class RetryConcurrentPublishException extends RuntimeException { }
}

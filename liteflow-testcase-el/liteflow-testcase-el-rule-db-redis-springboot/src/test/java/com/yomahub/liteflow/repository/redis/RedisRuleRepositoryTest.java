package com.yomahub.liteflow.repository.redis;

import com.yomahub.liteflow.exception.ConfigErrorException;
import com.yomahub.liteflow.exception.SeqGapException;
import com.yomahub.liteflow.publisher.exception.RuleStorageException;
import com.yomahub.liteflow.repository.vo.ChainMeta;
import com.yomahub.liteflow.repository.vo.ChainRecord;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import com.yomahub.liteflow.repository.vo.RuleManifest;
import com.yomahub.liteflow.repository.vo.ScriptMeta;
import com.yomahub.liteflow.repository.vo.ScriptRecord;
import org.junit.jupiter.api.Test;
import org.redisson.api.RBatch;
import org.redisson.api.RBucket;
import org.redisson.api.RBucketAsync;
import org.redisson.api.RFuture;
import org.redisson.api.RMap;
import org.redisson.api.RMapAsync;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RSet;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.client.protocol.ScoredEntry;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

@SuppressWarnings({ "unchecked", "rawtypes" })
class RedisRuleRepositoryTest {

	private final RedisConnectionManager connectionManager = mock(RedisConnectionManager.class);
	private final RedissonClient client = mock(RedissonClient.class);
	private final RedisKeys keys = new RedisKeys("lf", "app");
	private final RedisRuleRepository repository = new RedisRuleRepository(connectionManager, keys);

	RedisRuleRepositoryTest() {
		doReturn(client).when(connectionManager).getClient();
	}

	@Test
	void manifestMergesSortedIdsWithBatchedMetadata() {
		stubCurrentSequence("42");
		RSet<String> chainSet = mock(RSet.class);
		doReturn(chainSet).when(client).getSet("lf:app:chain-ids", StringCodec.INSTANCE);
		doReturn(new HashSet<>(Arrays.asList("c2", "c1"))).when(chainSet).readAll();
		RSet<String> scriptSet = mock(RSet.class);
		doReturn(scriptSet).when(client).getSet("lf:app:script-ids", StringCodec.INSTANCE);
		doReturn(new HashSet<>(Collections.singletonList("s1"))).when(scriptSet).readAll();

		RBatch batch = mock(RBatch.class);
		doReturn(batch).when(client).createBatch();
		stubBatchMap(batch, "lf:app:chain:c1", meta("2", "m1", "1"));
		stubBatchMap(batch, "lf:app:chain:c2", meta("3", "m2", "0"));
		stubBatchMap(batch, "lf:app:script:s1", scriptMeta("5", "m3", "1"));
		RBucketAsync<String> seqBucket = mock(RBucketAsync.class);
		doReturn(seqBucket).when(batch).getBucket("lf:app:seq", StringCodec.INSTANCE);
		RFuture<String> seqFuture = mock(RFuture.class);
		doReturn(seqFuture).when(seqBucket).getAsync();
		doReturn("42").when(seqFuture).getNow();

		RuleManifest manifest = repository.fetchManifest();
		assertEquals(1, manifest.getChains().size());
		ChainMeta chain = manifest.getChains().get(0);
		assertEquals("c1", chain.getChainId());
		assertEquals(2, chain.getVersion());
		assertEquals("m1", chain.getMd5());
		assertEquals(1, manifest.getScripts().size());
		ScriptMeta script = manifest.getScripts().get(0);
		assertEquals("s1", script.getNodeId());
		assertEquals(5, script.getVersion());
		assertEquals("boolean_script", script.getType());
		assertEquals("groovy", script.getLanguage());
		assertEquals("guard", script.getName());
		assertEquals(42, manifest.getLatestSeq());
		verify(batch).execute();
	}

	@Test
	void manifestRetriesUntilTheSequenceBoundsAStableSnapshot() {
		RBucket<String> current = stubCurrentSequence("41", "42");
		RSet<String> chainSet = mock(RSet.class);
		doReturn(chainSet).when(client).getSet("lf:app:chain-ids", StringCodec.INSTANCE);
		doReturn(Collections.emptySet()).when(chainSet).readAll();
		RSet<String> scriptSet = mock(RSet.class);
		doReturn(scriptSet).when(client).getSet("lf:app:script-ids", StringCodec.INSTANCE);
		doReturn(Collections.emptySet()).when(scriptSet).readAll();
		RBatch batch = mock(RBatch.class);
		doReturn(batch).when(client).createBatch();
		RBucketAsync<String> seqBucket = mock(RBucketAsync.class);
		doReturn(seqBucket).when(batch).getBucket("lf:app:seq", StringCodec.INSTANCE);
		RFuture<String> seqFuture = mock(RFuture.class);
		doReturn(seqFuture).when(seqBucket).getAsync();
		doReturn("42", "42").when(seqFuture).getNow();

		RuleManifest manifest = repository.fetchManifest();

		assertEquals(42, manifest.getLatestSeq());
		verify(current, times(2)).get();
		verify(batch, times(2)).execute();
	}

	@Test
	void manifestFailsInsteadOfReturningAnUnstableCursor() {
		stubCurrentSequence("1", "2", "3", "4", "5");
		RSet<String> empty = mock(RSet.class);
		doReturn(Collections.emptySet()).when(empty).readAll();
		doReturn(empty).when(client).getSet("lf:app:chain-ids", StringCodec.INSTANCE);
		doReturn(empty).when(client).getSet("lf:app:script-ids", StringCodec.INSTANCE);
		RBatch batch = mock(RBatch.class);
		doReturn(batch).when(client).createBatch();
		RBucketAsync<String> seqBucket = mock(RBucketAsync.class);
		doReturn(seqBucket).when(batch).getBucket("lf:app:seq", StringCodec.INSTANCE);
		RFuture<String> seqFuture = mock(RFuture.class);
		doReturn(seqFuture).when(seqBucket).getAsync();
		doReturn("2", "3", "4", "5", "6").when(seqFuture).getNow();

		assertThrows(RuleStorageException.class, repository::fetchManifest);
		verify(batch, times(RedisRuleRepository.MANIFEST_SNAPSHOT_ATTEMPTS)).execute();
	}

	@Test
	void fetchChainMapsAllFieldsAndHonorsEnableFlag() {
		RMap<String, String> map = mock(RMap.class);
		doReturn(map).when(client).getMap("lf:app:chain:c1", StringCodec.INSTANCE);
		Map<String, String> content = new HashMap<>();
		content.put("el", "THEN(a)");
		content.put("route", "r1");
		content.put("namespace", "ns");
		content.put("version", " 7 ");
		content.put("md5", "m");
		content.put("enable", "0");
		doReturn(content).when(map).readAllMap();

		ChainRecord record = repository.fetchChain("c1");
		assertEquals("c1", record.getChainId());
		assertEquals("THEN(a)", record.getEl());
		assertEquals("r1", record.getRoute());
		assertEquals("ns", record.getNamespace());
		assertEquals(7, record.getVersion());
		assertEquals("m", record.getMd5());
		assertFalse(record.isEnable());

		RMap<String, String> empty = mock(RMap.class);
		doReturn(empty).when(client).getMap("lf:app:chain:missing", StringCodec.INSTANCE);
		doReturn(Collections.emptyMap()).when(empty).readAllMap();
		assertNull(repository.fetchChain("missing"));
	}

	@Test
	void fetchChainMetaReturnsNullWhenDisabledOrAbsent() {
		RMap<String, String> map = mock(RMap.class);
		doReturn(map).when(client).getMap("lf:app:chain:c1", StringCodec.INSTANCE);
		doReturn(meta("2", "m1", "1")).when(map).getAll(anySet());
		ChainMeta meta = repository.fetchChainMeta("c1");
		assertEquals("c1", meta.getChainId());
		assertEquals(2, meta.getVersion());

		doReturn(meta("2", "m1", "0")).when(map).getAll(anySet());
		assertNull(repository.fetchChainMeta("c1"));

		doReturn(Collections.emptyMap()).when(map).getAll(anySet());
		assertNull(repository.fetchChainMeta("c1"));
	}

	@Test
	void fetchScriptMapsAllFields() {
		RMap<String, String> map = mock(RMap.class);
		doReturn(map).when(client).getMap("lf:app:script:s1", StringCodec.INSTANCE);
		Map<String, String> content = new HashMap<>();
		content.put("script", "return true");
		content.put("name", "guard");
		content.put("type", "boolean_script");
		content.put("language", "groovy");
		content.put("version", "3");
		content.put("md5", "m");
		content.put("enable", "1");
		doReturn(content).when(map).readAllMap();

		ScriptRecord record = repository.fetchScript("s1");
		assertEquals("s1", record.getNodeId());
		assertEquals("return true", record.getScript());
		assertEquals("guard", record.getName());
		assertEquals("boolean_script", record.getType());
		assertEquals("groovy", record.getLanguage());
		assertEquals(3, record.getVersion());
		assertTrue(record.isEnable());

		RMap<String, String> empty = mock(RMap.class);
		doReturn(empty).when(client).getMap("lf:app:script:missing", StringCodec.INSTANCE);
		doReturn(Collections.emptyMap()).when(empty).readAllMap();
		assertNull(repository.fetchScript("missing"));
	}

	@Test
	void fetchScriptMetaReturnsNullWhenDisabled() {
		RMap<String, String> map = mock(RMap.class);
		doReturn(map).when(client).getMap("lf:app:script:s1", StringCodec.INSTANCE);
		doReturn(scriptMeta("5", "m3", "1")).when(map).getAll(anySet());
		ScriptMeta meta = repository.fetchScriptMeta("s1");
		assertEquals("s1", meta.getNodeId());
		assertEquals("guard", meta.getName());

		doReturn(scriptMeta("5", "m3", "0")).when(map).getAll(anySet());
		assertNull(repository.fetchScriptMeta("s1"));
	}

	@Test
	void fetchLatestSeqParsesBucketAndDefaultsToZero() {
		RBucket<String> bucket = mock(RBucket.class);
		doReturn(bucket).when(client).getBucket("lf:app:seq", StringCodec.INSTANCE);
		doReturn("9").when(bucket).get();
		assertEquals(9, repository.fetchLatestSeq());
		doReturn(null).when(bucket).get();
		assertEquals(0, repository.fetchLatestSeq());
	}

	@Test
	void fetchChangesSinceAppliesDefaultLimitAndParsesEntries() {
		RScoredSortedSet<String> log = mock(RScoredSortedSet.class);
		doReturn(log).when(client).getScoredSortedSet("lf:app:changelog", StringCodec.INSTANCE);
		doReturn(Collections.singletonList(new ScoredEntry<>(1.0, "x"))).when(log).entryRange(0, 0);
		doReturn(Arrays.asList(json(1, "c1"), json(2, "c2"))).when(log)
				.valueRange(0.0, false, Double.POSITIVE_INFINITY, true, 0, 1000);

		java.util.List<ChangeRecord> changes = repository.fetchChangesSince(0);
		assertEquals(2, changes.size());
		assertEquals(1, changes.get(0).getSeq());
		assertEquals(ChangeRecord.TargetType.CHAIN, changes.get(0).getTargetType());
		assertEquals("c2", changes.get(1).getTargetId());
		verify(log).valueRange(0.0, false, Double.POSITIVE_INFINITY, true, 0, 1000);
	}

	@Test
	void fetchChangesSinceRejectsNonPositiveLimit() {
		assertThrows(ConfigErrorException.class, () -> repository.fetchChangesSince(0, 0));
	}

	@Test
	void fetchChangesSinceDetectsTrimmedGap() {
		RScoredSortedSet<String> log = mock(RScoredSortedSet.class);
		doReturn(log).when(client).getScoredSortedSet("lf:app:changelog", StringCodec.INSTANCE);
		doReturn(Collections.singletonList(new ScoredEntry<>(5.0, "x"))).when(log).entryRange(0, 0);
		assertThrows(SeqGapException.class, () -> repository.fetchChangesSince(2));
	}

	@Test
	void fetchChangesSinceDetectsInternalGap() {
		RScoredSortedSet<String> log = mock(RScoredSortedSet.class);
		doReturn(log).when(client).getScoredSortedSet("lf:app:changelog", StringCodec.INSTANCE);
		doReturn(Collections.singletonList(new ScoredEntry<>(1.0, "x"))).when(log).entryRange(0, 0);
		doReturn(Arrays.asList(json(1, "c1"), json(3, "c3"))).when(log)
				.valueRange(eq(0.0), eq(false), eq(Double.POSITIVE_INFINITY), eq(true), eq(0), eq(3));
		assertThrows(SeqGapException.class, () -> repository.fetchChangesSince(0, 3));
	}

	@Test
	void closeShutsDownConnectionManager() {
		repository.close();
		verify(connectionManager).shutdown();
	}

	private void stubBatchMap(RBatch batch, String key, Map<String, String> metadata) {
		RMapAsync<String, String> map = mock(RMapAsync.class);
		doReturn(map).when(batch).getMap(key, StringCodec.INSTANCE);
		RFuture<Map<String, String>> future = mock(RFuture.class);
		doReturn(future).when(map).getAllAsync(anySet());
		doReturn(metadata).when(future).getNow();
	}

	private RBucket<String> stubCurrentSequence(String first, String... rest) {
		RBucket<String> bucket = mock(RBucket.class);
		doReturn(bucket).when(client).getBucket("lf:app:seq", StringCodec.INSTANCE);
		doReturn(first, rest).when(bucket).get();
		return bucket;
	}

	private static Map<String, String> meta(String version, String md5, String enable) {
		Map<String, String> map = new HashMap<>();
		map.put("version", version);
		map.put("md5", md5);
		map.put("enable", enable);
		return map;
	}

	private static Map<String, String> scriptMeta(String version, String md5, String enable) {
		Map<String, String> map = meta(version, md5, enable);
		map.put("type", "boolean_script");
		map.put("language", "groovy");
		map.put("name", "guard");
		return map;
	}

	private static String json(long seq, String targetId) {
		return "{\"seq\":" + seq + ",\"targetType\":\"CHAIN\",\"targetId\":\"" + targetId
				+ "\",\"op\":\"UPSERT\",\"version\":1}";
	}
}

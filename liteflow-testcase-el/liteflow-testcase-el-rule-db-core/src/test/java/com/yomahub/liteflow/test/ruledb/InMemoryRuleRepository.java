package com.yomahub.liteflow.test.ruledb;

import cn.hutool.crypto.SecureUtil;
import com.yomahub.liteflow.exception.SeqGapException;
import com.yomahub.liteflow.repository.RuleRepository;
import com.yomahub.liteflow.repository.vo.ChainMeta;
import com.yomahub.liteflow.repository.vo.ChainRecord;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import com.yomahub.liteflow.repository.vo.RuleManifest;
import com.yomahub.liteflow.repository.vo.ScriptMeta;
import com.yomahub.liteflow.repository.vo.ScriptRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** 测试用内存权威源。静态状态便于测试内"另一个节点发布"式操作 */
public class InMemoryRuleRepository implements RuleRepository {

    public static final Map<String, ChainRecord> CHAINS = new ConcurrentHashMap<>();
    public static final Map<String, ScriptRecord> SCRIPTS = new ConcurrentHashMap<>();
    public static final List<ChangeRecord> CHANGES = new CopyOnWriteArrayList<>();
    public static final AtomicLong SEQ = new AtomicLong(0);
    public static final AtomicInteger FETCH_CHAIN_COUNT = new AtomicInteger(0);
    public static final AtomicInteger FETCH_CHAIN_META_COUNT = new AtomicInteger(0);
    public static final AtomicInteger FETCH_SCRIPT_COUNT = new AtomicInteger(0);
    public static final AtomicInteger FETCH_SCRIPT_META_COUNT = new AtomicInteger(0);
    public static final AtomicInteger FETCH_MANIFEST_COUNT = new AtomicInteger(0);
    public static volatile boolean DOWN = false;
    public static volatile long MIN_SEQ = 0;
    private static volatile Runnable afterNextChainFetch;
    private static volatile Runnable afterNextScriptFetch;

    public static void reset() {
        CHAINS.clear(); SCRIPTS.clear(); CHANGES.clear();
        SEQ.set(0); FETCH_CHAIN_COUNT.set(0); FETCH_CHAIN_META_COUNT.set(0);
        FETCH_SCRIPT_COUNT.set(0); FETCH_SCRIPT_META_COUNT.set(0); FETCH_MANIFEST_COUNT.set(0);
        DOWN = false; MIN_SEQ = 0; afterNextChainFetch = null; afterNextScriptFetch = null;
    }

    public static void afterNextChainFetch(Runnable callback) {
        afterNextChainFetch = callback;
    }

    public static void afterNextScriptFetch(Runnable callback) {
        afterNextScriptFetch = callback;
    }

    /** 只放数据不记变更（模拟绕过发布规范的脏写/初始数据） */
    public static void putChain(String chainId, String el) {
        ChainRecord old = CHAINS.get(chainId);
        long version = old == null ? 1 : old.getVersion() + 1;
        ChainRecord r = new ChainRecord();
        r.setChainId(chainId); r.setEl(el); r.setVersion(version);
        r.setMd5(SecureUtil.md5(el)); r.setEnable(true);
        CHAINS.put(chainId, r);
    }

    public static void putChain(String chainId, String el, String namespace) {
        putChain(chainId, el);
        CHAINS.get(chainId).setNamespace(namespace);
    }

    /** 直接把行置为停用（不记变更），模拟运营台直改 enable=0 */
    public static void disableChain(String chainId) {
        CHAINS.get(chainId).setEnable(false);
    }

    public static void putScript(String nodeId, String script, String type, String language) {
        ScriptRecord old = SCRIPTS.get(nodeId);
        long version = old == null ? 1 : old.getVersion() + 1;
        ScriptRecord r = new ScriptRecord();
        r.setNodeId(nodeId); r.setScript(script); r.setType(type); r.setLanguage(language);
        r.setVersion(version); r.setMd5(SecureUtil.md5(script)); r.setEnable(true);
        SCRIPTS.put(nodeId, r);
    }

    /** 规范发布：内容 + 版本 + 变更序号 */
    public static void publishChain(String chainId, String el) {
        putChain(chainId, el);
        CHANGES.add(new ChangeRecord(SEQ.incrementAndGet(), ChangeRecord.TargetType.CHAIN,
                chainId, ChangeRecord.Op.UPSERT, CHAINS.get(chainId).getVersion()));
    }

    public static void publishScript(String nodeId, String script, String type, String language) {
        putScript(nodeId, script, type, language);
        CHANGES.add(new ChangeRecord(SEQ.incrementAndGet(), ChangeRecord.TargetType.SCRIPT,
                nodeId, ChangeRecord.Op.UPSERT, SCRIPTS.get(nodeId).getVersion()));
    }

    /** 脏写：改内容并重算 md5，但版本号不动（模拟自算 md5 却忘了 version+1 的管理后台） */
    public static void dirtyWriteChainSameVersion(String chainId, String el) {
        ChainRecord old = CHAINS.get(chainId);
        ChainRecord r = new ChainRecord();
        r.setChainId(chainId); r.setEl(el); r.setVersion(old.getVersion());
        r.setMd5(SecureUtil.md5(el)); r.setEnable(true);
        CHAINS.put(chainId, r);
    }

    public static void dirtyWriteScriptSameVersion(String nodeId, String script) {
        ScriptRecord old = SCRIPTS.get(nodeId);
        ScriptRecord r = new ScriptRecord();
        r.setNodeId(nodeId); r.setScript(script); r.setType(old.getType()); r.setLanguage(old.getLanguage());
        r.setVersion(old.getVersion()); r.setMd5(SecureUtil.md5(script)); r.setEnable(true);
        SCRIPTS.put(nodeId, r);
    }

    public static void deleteChain(String chainId) {
        ChainRecord old = CHAINS.remove(chainId);
        long version = old == null ? 0 : old.getVersion();
        CHANGES.add(new ChangeRecord(SEQ.incrementAndGet(), ChangeRecord.TargetType.CHAIN,
                chainId, ChangeRecord.Op.DELETE, version));
    }

    public static void deleteScript(String nodeId) {
        ScriptRecord old = SCRIPTS.remove(nodeId);
        long version = old == null ? 0 : old.getVersion();
        CHANGES.add(new ChangeRecord(SEQ.incrementAndGet(), ChangeRecord.TargetType.SCRIPT,
                nodeId, ChangeRecord.Op.DELETE, version));
    }

    private void checkDown() {
        if (DOWN) {
            throw new RuntimeException("in-memory rule repository is down");
        }
    }

    @Override
    public RuleManifest fetchManifest() {
        // 先计数再判宕机：计数语义为"尝试次数"，供重试次数断言
        FETCH_MANIFEST_COUNT.incrementAndGet();
        checkDown();
        RuleManifest m = new RuleManifest();
        List<ChainMeta> chains = new ArrayList<>();
        for (ChainRecord r : CHAINS.values()) {
            if (r.isEnable()) {
                chains.add(new ChainMeta(r.getChainId(), r.getVersion(), r.getMd5()));
            }
        }
        List<ScriptMeta> scripts = new ArrayList<>();
        for (ScriptRecord r : SCRIPTS.values()) {
            if (r.isEnable()) {
                scripts.add(new ScriptMeta(r.getNodeId(), r.getVersion(), r.getMd5(),
                        r.getType(), r.getLanguage(), r.getName()));
            }
        }
        m.setChains(chains); m.setScripts(scripts); m.setLatestSeq(SEQ.get());
        return m;
    }

    @Override
    public ChainRecord fetchChain(String chainId) {
        // 先计数再判宕机：计数语义为"尝试次数"，供重试次数断言
        FETCH_CHAIN_COUNT.incrementAndGet();
        checkDown();
        ChainRecord record = CHAINS.get(chainId);
        Runnable callback = afterNextChainFetch;
        afterNextChainFetch = null;
        if (callback != null) {
            callback.run();
        }
        return record;
    }

    @Override
    public ChainMeta fetchChainMeta(String chainId) {
        FETCH_CHAIN_META_COUNT.incrementAndGet();
        checkDown();
        ChainRecord r = CHAINS.get(chainId);
        if (r == null || !r.isEnable()) {
            return null;
        }
        return new ChainMeta(r.getChainId(), r.getVersion(), r.getMd5());
    }

    @Override
    public ScriptRecord fetchScript(String nodeId) {
        FETCH_SCRIPT_COUNT.incrementAndGet();
        checkDown();
        ScriptRecord record = SCRIPTS.get(nodeId);
        Runnable callback = afterNextScriptFetch;
        afterNextScriptFetch = null;
        if (callback != null) {
            callback.run();
        }
        return record;
    }

    @Override
    public ScriptMeta fetchScriptMeta(String nodeId) {
        FETCH_SCRIPT_META_COUNT.incrementAndGet();
        checkDown();
        ScriptRecord r = SCRIPTS.get(nodeId);
        if (r == null || !r.isEnable()) {
            return null;
        }
        return new ScriptMeta(r.getNodeId(), r.getVersion(), r.getMd5(),
                r.getType(), r.getLanguage(), r.getName());
    }

    @Override
    public long fetchLatestSeq() {
        checkDown();
        return SEQ.get();
    }

    @Override
    public List<ChangeRecord> fetchChangesSince(long seq) {
        checkDown();
        if (seq + 1 < MIN_SEQ) {
            throw new SeqGapException("change log has been cleaned, since=" + seq + " min=" + MIN_SEQ);
        }
        List<ChangeRecord> result = new ArrayList<>();
        for (ChangeRecord c : CHANGES) {
            if (c.getSeq() > seq) {
                result.add(c);
            }
        }
        return result;
    }
}

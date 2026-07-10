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
    public static final AtomicInteger FETCH_SCRIPT_COUNT = new AtomicInteger(0);
    public static volatile boolean DOWN = false;
    public static volatile long MIN_SEQ = 0;

    public static void reset() {
        CHAINS.clear(); SCRIPTS.clear(); CHANGES.clear();
        SEQ.set(0); FETCH_CHAIN_COUNT.set(0); FETCH_SCRIPT_COUNT.set(0);
        DOWN = false; MIN_SEQ = 0;
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

    public static void deleteChain(String chainId) {
        ChainRecord old = CHAINS.remove(chainId);
        long version = old == null ? 0 : old.getVersion();
        CHANGES.add(new ChangeRecord(SEQ.incrementAndGet(), ChangeRecord.TargetType.CHAIN,
                chainId, ChangeRecord.Op.DELETE, version));
    }

    private void checkDown() {
        if (DOWN) {
            throw new RuntimeException("in-memory rule repository is down");
        }
    }

    @Override
    public RuleManifest fetchManifest() {
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
        checkDown();
        FETCH_CHAIN_COUNT.incrementAndGet();
        return CHAINS.get(chainId);
    }

    @Override
    public ScriptRecord fetchScript(String nodeId) {
        checkDown();
        FETCH_SCRIPT_COUNT.incrementAndGet();
        return SCRIPTS.get(nodeId);
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

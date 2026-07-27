package com.yomahub.liteflow.test.nodeexecute;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/** 测试用静态收集器，记录节点执行钩子的回调 */
public class NodeExecuteCollector {

    public static class Record {
        public final String nodeId;
        public final long timeSpent;
        public final Exception exception;
        public Record(String nodeId, long timeSpent, Exception exception) {
            this.nodeId = nodeId;
            this.timeSpent = timeSpent;
            this.exception = exception;
        }
    }

    public static final List<String> BEFORE = new CopyOnWriteArrayList<>();
    public static final List<Record> AFTER = new CopyOnWriteArrayList<>();

    public static void clear() {
        BEFORE.clear();
        AFTER.clear();
    }

    public static List<Record> afters() {
        return Collections.unmodifiableList(AFTER);
    }
}

package com.yomahub.liteflow.test.ruledb;

import com.yomahub.liteflow.repository.ChangeSourceHealth;
import com.yomahub.liteflow.repository.RuleChangeListener;
import com.yomahub.liteflow.repository.RuleChangeSource;
import com.yomahub.liteflow.repository.RuleDbProvider;
import com.yomahub.liteflow.repository.RuleRepository;
import com.yomahub.liteflow.repository.vo.ChangeRecord;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/** In-memory provider fixture used by the Rule-DB contract tests. */
public class InMemoryRuleDbProvider implements RuleDbProvider {

    private final InMemoryRuleRepository repository = new InMemoryRuleRepository();
    private final InMemoryChangeSource changeSource = new InMemoryChangeSource();

    @Override
    public RuleRepository repository() {
        return repository;
    }

    @Override
    public RuleChangeSource changeSource() {
        return changeSource;
    }

    public void emit(ChangeRecord change) {
        changeSource.emit(change);
    }

    @Override
    public void close() {
        changeSource.close();
    }

    static final class InMemoryChangeSource implements RuleChangeSource {
        private final Object monitor = new Object();
        private final List<ChangeRecord> buffered = new ArrayList<>();
        private RuleChangeListener listener;
        private long baselineSeq;
        private long cursor;
        private boolean activated;
        private boolean closed;
        private ChangeSourceHealth health = ChangeSourceHealth.starting();
        private int openCalls;
        private int activateCalls;

        @Override
        public void open(RuleChangeListener listener) {
            synchronized (monitor) {
                if (closed) {
                    return;
                }
                this.listener = listener;
                openCalls++;
            }
        }

        @Override
        public void activate(long baselineSeq) {
            synchronized (monitor) {
                if (closed) {
                    return;
                }
                this.baselineSeq = baselineSeq;
                this.cursor = baselineSeq;
                this.activated = true;
                activateCalls++;
                buffered.sort(Comparator.comparingLong(ChangeRecord::getSeq));
                List<ChangeRecord> replay = drainAfterBaseline();
                deliver(replay);
                health = ChangeSourceHealth.up(cursor);
            }
        }

        /** Emit a change, buffering it until activation. */
        public void emit(ChangeRecord change) {
            synchronized (monitor) {
                if (closed) {
                    return;
                }
                if (!activated) {
                    buffered.add(change);
                    return;
                }
                if (change.getSeq() <= baselineSeq) {
                    return;
                }
                deliver(singleton(change));
            }
        }

        private List<ChangeRecord> drainAfterBaseline() {
            List<ChangeRecord> replay = new ArrayList<>();
            for (ChangeRecord change : buffered) {
                if (change.getSeq() > baselineSeq) {
                    replay.add(change);
                }
            }
            buffered.clear();
            return replay;
        }

        private List<ChangeRecord> singleton(ChangeRecord change) {
            List<ChangeRecord> one = new ArrayList<>(1);
            one.add(change);
            return one;
        }

        private void deliver(List<ChangeRecord> changes) {
            if (changes.isEmpty() || listener == null || closed) {
                return;
            }
            List<ChangeRecord> deliverable = new ArrayList<>();
            for (ChangeRecord change : changes) {
                if (change.getSeq() > baselineSeq) {
                    deliverable.add(change);
                }
            }
            if (!deliverable.isEmpty()) {
                listener.onChanges(deliverable);
                for (ChangeRecord change : deliverable) {
                    cursor = Math.max(cursor, change.getSeq());
                }
            }
        }

        @Override
        public ChangeSourceHealth health() {
            synchronized (monitor) {
                return health;
            }
        }

        public int getOpenCalls() {
            synchronized (monitor) {
                return openCalls;
            }
        }

        public int getActivateCalls() {
            synchronized (monitor) {
                return activateCalls;
            }
        }

        @Override
        public void close() {
            synchronized (monitor) {
                if (closed) {
                    return;
                }
                closed = true;
                listener = null;
                buffered.clear();
                health = ChangeSourceHealth.down(null, cursor);
            }
        }
    }
}

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
        private boolean delivering;
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
            boolean startDelivery;
            synchronized (monitor) {
                if (closed) {
                    return;
                }
                this.baselineSeq = baselineSeq;
                this.cursor = baselineSeq;
                this.activated = true;
                activateCalls++;
                buffered.sort(Comparator.comparingLong(ChangeRecord::getSeq));
                drainAfterBaseline();
                health = health.successful(cursor);
                startDelivery = !buffered.isEmpty() && !delivering;
                if (startDelivery) {
                    delivering = true;
                }
            }
            if (startDelivery) {
                drain();
            }
        }

        /** Emit a change, buffering it until activation. */
        public void emit(ChangeRecord change) {
            boolean startDelivery;
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
                buffered.add(change);
                startDelivery = !delivering;
                if (startDelivery) {
                    delivering = true;
                }
            }
            if (startDelivery) {
                drain();
            }
        }

        private void drainAfterBaseline() {
            buffered.removeIf(change -> change.getSeq() <= baselineSeq);
        }

        private void drain() {
            while (true) {
                List<ChangeRecord> batch;
                RuleChangeListener callback;
                synchronized (monitor) {
                    if (closed || listener == null) {
                        buffered.clear();
                        delivering = false;
                        return;
                    }
                    if (buffered.isEmpty()) {
                        delivering = false;
                        return;
                    }
                    buffered.sort(Comparator.comparingLong(ChangeRecord::getSeq));
                    batch = new ArrayList<>(buffered);
                    buffered.clear();
                    callback = listener;
                }

                try {
                    callback.onChanges(batch);
                } catch (RuntimeException e) {
                    synchronized (monitor) {
                        if (!closed) {
                            health = health.degraded(e.getMessage());
                        }
                        delivering = false;
                    }
                    throw e;
                }

                synchronized (monitor) {
                    if (!closed) {
                        for (ChangeRecord change : batch) {
                            cursor = Math.max(cursor, change.getSeq());
                        }
                        health = health.successful(cursor);
                    }
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
                delivering = false;
                health = health.down(null);
            }
        }
    }
}

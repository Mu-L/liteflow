package com.yomahub.liteflow.test.ruledb;

import com.yomahub.liteflow.repository.ChangeSourceHealth;
import com.yomahub.liteflow.repository.ManualPollingChangeSourceAdapter;
import com.yomahub.liteflow.repository.RuleChangeListener;
import com.yomahub.liteflow.repository.RuleChangeSource;
import com.yomahub.liteflow.repository.RuleDbProvider;
import com.yomahub.liteflow.repository.RuleRepository;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import com.yomahub.liteflow.repository.vo.RuleManifest;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/** In-memory provider fixture used by the Rule-DB contract tests. */
public class InMemoryRuleDbProvider implements RuleDbProvider {

    private final InMemoryChangeSource changeSource = new InMemoryChangeSource();
    private final RuleChangeSource exposedChangeSource =
            new ManualPollingChangeSourceAdapter(changeSource, this::pollOnce);
    private final InMemoryRuleRepository repository = new InMemoryRuleRepository() {
        @Override
        public RuleManifest fetchManifest() {
            RuleManifest manifest = super.fetchManifest();
            Runnable callback = afterManifestSnapshot;
            afterManifestSnapshot = null;
            if (callback != null) {
                callback.run();
            }
            return manifest;
        }
    };
    private volatile Runnable afterManifestSnapshot;
    private int providerCloseCalls;
    private boolean throwOnClose;

    @Override
    public RuleRepository repository() {
        return repository;
    }

    @Override
    public RuleChangeSource changeSource() {
        return exposedChangeSource;
    }

    public void emit(ChangeRecord change) {
        changeSource.emit(change);
    }

    public void emitBatch(List<ChangeRecord> changes) {
        changeSource.emitBatch(changes);
    }

    public void requestReconcile() {
        changeSource.requestReconcile();
    }

    public void emitLate(ChangeRecord change) {
        changeSource.emitLate(change);
    }

    public void afterManifestSnapshot(Runnable callback) {
        this.afterManifestSnapshot = callback;
    }

    public int getOpenCalls() {
        return changeSource.getOpenCalls();
    }

    public int getActivateCalls() {
        return changeSource.getActivateCalls();
    }

    public int getCloseCalls() {
        return changeSource.getCloseCalls();
    }

    public int getProviderCloseCalls() {
        return providerCloseCalls;
    }

    public void throwOnClose() {
        throwOnClose = true;
    }

    @Override
    public void close() {
        providerCloseCalls++;
        exposedChangeSource.close();
        if (throwOnClose) {
            throw new RuntimeException("in-memory provider close failure");
        }
    }

    private void pollOnce() {
        long cursor = changeSource.getCursor();
        if (repository.fetchLatestSeq() <= cursor) {
            return;
        }
        try {
            for (ChangeRecord change : repository.fetchChangesSince(cursor)) {
                changeSource.emit(change);
            }
        } catch (com.yomahub.liteflow.exception.SeqGapException gap) {
            changeSource.requestReconcile();
        }
    }

    static final class InMemoryChangeSource implements RuleChangeSource {
        private final Object monitor = new Object();
        private final List<ChangeRecord> buffered = new ArrayList<>();
        private RuleChangeListener listener;
        private RuleChangeListener lateListener;
        private long baselineSeq;
        private long cursor;
        private boolean activated;
        private boolean closed;
        private boolean delivering;
        private ChangeSourceHealth health = ChangeSourceHealth.starting();
        private int openCalls;
        private int activateCalls;
        private int closeCalls;

        @Override
        public void open(RuleChangeListener listener) {
            synchronized (monitor) {
                if (closed) {
                    return;
                }
                this.listener = listener;
                this.lateListener = listener;
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

        /** Deliver a backend-produced batch without imposing source-side ordering. */
        public void emitBatch(List<ChangeRecord> changes) {
            RuleChangeListener callback;
            synchronized (monitor) {
                if (closed || !activated || listener == null) {
                    return;
                }
                callback = listener;
            }
            callback.onChanges(new ArrayList<>(changes));
        }

        public void requestReconcile() {
            RuleChangeListener callback;
            synchronized (monitor) {
                if (closed || !activated || listener == null) {
                    return;
                }
                callback = listener;
            }
            callback.onReconcileRequired();
        }

        /** Simulate a callback already queued by a backend before close completed. */
        public void emitLate(ChangeRecord change) {
            RuleChangeListener callback;
            synchronized (monitor) {
                callback = lateListener;
            }
            if (callback != null) {
                List<ChangeRecord> changes = new ArrayList<>(1);
                changes.add(change);
                callback.onChanges(changes);
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

        public int getCloseCalls() {
            synchronized (monitor) {
                return closeCalls;
            }
        }

        public long getCursor() {
            synchronized (monitor) {
                return cursor;
            }
        }

        @Override
        public void close() {
            synchronized (monitor) {
                if (closed) {
                    return;
                }
                closed = true;
                closeCalls++;
                listener = null;
                buffered.clear();
                delivering = false;
                health = health.down(null);
            }
        }
    }
}

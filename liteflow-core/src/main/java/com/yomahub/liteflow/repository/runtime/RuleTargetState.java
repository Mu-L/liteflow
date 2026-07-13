package com.yomahub.liteflow.repository.runtime;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/** Desired and active metadata for one Rule-DB target. */
public final class RuleTargetState {

	private final AtomicLong desiredVersion = new AtomicLong();
	private final AtomicLong activeVersion = new AtomicLong();
	private final ReentrantLock loadLock = new ReentrantLock();
	private volatile String desiredMd5;
	private volatile String activeMd5;
	private volatile long loadedVersion;
	private volatile String loadedMd5;
	private volatile RuleTargetStatus status = RuleTargetStatus.SHADOW;
	private volatile Throwable lastError;

	public RuleTargetState() {
	}

	public RuleTargetState(long desiredVersion, String desiredMd5) {
		this.desiredVersion.set(desiredVersion);
		this.desiredMd5 = desiredMd5;
	}

	/**
	 * Advances desired metadata without regressing its version. A null md5 on a
	 * newer event means that the change source did not carry content metadata.
	 */
	public synchronized boolean updateDesired(long version, String md5) {
		if (status == RuleTargetStatus.DELETED) {
			return false;
		}
		long current = desiredVersion.get();
		if (version < current || (version == current && md5 == null)) {
			return false;
		}
		boolean changed = version != current || !Objects.equals(desiredMd5, md5);
		if (!changed) {
			return false;
		}
		desiredVersion.set(version);
		desiredMd5 = md5;
		status = activeVersion.get() == 0 ? RuleTargetStatus.SHADOW : readyOrStale();
		return true;
	}

	public synchronized boolean markLoaded(long version, String md5) {
		if (!isGenerationCurrent(version, md5)) {
			return false;
		}
		loadedVersion = version;
		loadedMd5 = md5;
		status = RuleTargetStatus.LOADING;
		lastError = null;
		return true;
	}

	public synchronized boolean isDesiredLoaded() {
		return loadedVersion == desiredVersion.get()
				&& (desiredMd5 == null || Objects.equals(loadedMd5, desiredMd5));
	}

	public synchronized boolean activateLoaded() {
		if (loadedVersion == 0 || !isGenerationCurrent(loadedVersion, loadedMd5)) {
			return false;
		}
		activeVersion.set(loadedVersion);
		activeMd5 = loadedMd5;
		if (desiredVersion.get() == loadedVersion && desiredMd5 == null) {
			desiredMd5 = loadedMd5;
		}
		loadedVersion = 0;
		loadedMd5 = null;
		lastError = null;
		status = readyOrStale();
		return true;
	}

	public synchronized boolean activate(long version, String md5) {
		if (!isGenerationCurrent(version, md5)) {
			return false;
		}
		activeVersion.set(version);
		activeMd5 = md5;
		if (desiredVersion.get() == version && desiredMd5 == null) {
			desiredMd5 = md5;
		}
		lastError = null;
		status = readyOrStale();
		return true;
	}

	public synchronized void clearActive() {
		if (status == RuleTargetStatus.DELETED) {
			return;
		}
		activeVersion.set(0);
		activeMd5 = null;
		loadedVersion = 0;
		loadedMd5 = null;
		lastError = null;
		status = RuleTargetStatus.SHADOW;
	}

	public synchronized void markDeleted() {
		status = RuleTargetStatus.DELETED;
		lastError = null;
	}

	public synchronized void markLoading() {
		if (status == RuleTargetStatus.DELETED) {
			return;
		}
		status = RuleTargetStatus.LOADING;
		lastError = null;
	}

	public synchronized boolean markFailed(Throwable error) {
		if (status == RuleTargetStatus.DELETED) {
			return false;
		}
		status = RuleTargetStatus.FAILED;
		lastError = error;
		return true;
	}

	public synchronized boolean markStale() {
		if (status == RuleTargetStatus.DELETED || activeVersion.get() == 0) {
			return false;
		}
		status = RuleTargetStatus.STALE;
		return true;
	}

	public synchronized boolean isGenerationCurrent(long version, String md5) {
		return status != RuleTargetStatus.DELETED
				&& desiredVersion.get() == version
				&& (desiredMd5 == null || Objects.equals(desiredMd5, md5));
	}

	public boolean isDeleted() {
		return status == RuleTargetStatus.DELETED;
	}

	private RuleTargetStatus readyOrStale() {
		if (activeVersion.get() != desiredVersion.get()) {
			return RuleTargetStatus.STALE;
		}
		if (activeMd5 != null && desiredMd5 != null && !activeMd5.equals(desiredMd5)) {
			return RuleTargetStatus.STALE;
		}
		return RuleTargetStatus.READY;
	}

	public long getDesiredVersion() {
		return desiredVersion.get();
	}

	public long getActiveVersion() {
		return activeVersion.get();
	}

	public String getDesiredMd5() {
		return desiredMd5;
	}

	public String getActiveMd5() {
		return activeMd5;
	}

	public RuleTargetStatus getStatus() {
		return status;
	}

	public Throwable getLastError() {
		return lastError;
	}

	public ReentrantLock getLoadLock() {
		return loadLock;
	}

}

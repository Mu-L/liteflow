package com.yomahub.liteflow.repository;

/** Immutable health snapshot for a rule change source. */
public final class ChangeSourceHealth {

	public enum Status {
		STARTING,
		UP,
		DEGRADED,
		DOWN
	}

	private final Status status;
	private final String recentError;
	private final long lastSuccessTime;
	private final long cursor;

	public ChangeSourceHealth(Status status, String recentError, long lastSuccessTime, long cursor) {
		this.status = status;
		this.recentError = recentError;
		this.lastSuccessTime = lastSuccessTime;
		this.cursor = cursor;
	}

	public static ChangeSourceHealth starting() {
		return new ChangeSourceHealth(Status.STARTING, null, 0L, 0L);
	}

	public static ChangeSourceHealth up(long cursor) {
		return new ChangeSourceHealth(Status.UP, null, System.currentTimeMillis(), cursor);
	}

	public static ChangeSourceHealth degraded(String error, long cursor) {
		return new ChangeSourceHealth(Status.DEGRADED, error, 0L, cursor);
	}

	public static ChangeSourceHealth down(String error, long cursor) {
		return new ChangeSourceHealth(Status.DOWN, error, 0L, cursor);
	}

	public Status getStatus() {
		return status;
	}

	public String getRecentError() {
		return recentError;
	}

	public String getLastError() {
		return recentError;
	}

	public long getLastSuccessTime() {
		return lastSuccessTime;
	}

	public long getLastSuccessfulAt() {
		return lastSuccessTime;
	}

	public long getCursor() {
		return cursor;
	}
}

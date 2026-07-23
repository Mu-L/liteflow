package com.yomahub.liteflow.repository.etcd;

/**
 * Module-internal signal that an etcd watch failed because its start revision
 * was compacted away. Raised by {@link JetcdWatchFacade} after unwrapping the
 * jetcd cause chain, so {@link EtcdWatchChangeSource} can branch on the type
 * instead of matching error message strings.
 */
final class EtcdCompactionException extends RuntimeException {

	EtcdCompactionException(Throwable cause) {
		super(cause == null ? "etcd watch revision compacted" : String.valueOf(cause.getMessage()), cause);
	}
}

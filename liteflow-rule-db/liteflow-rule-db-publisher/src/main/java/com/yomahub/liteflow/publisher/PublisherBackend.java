package com.yomahub.liteflow.publisher;

/** Supported Rule-DB publisher backends. */
public enum PublisherBackend {

	SQL,

	REDIS,

	ETCD,

	ZK
}

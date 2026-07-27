package com.yomahub.liteflow.publisher;

import com.yomahub.liteflow.publisher.exception.RuleStorageException;
import com.yomahub.liteflow.publisher.exception.RuleValidationException;
import com.yomahub.liteflow.publisher.exception.VersionConflictException;

/** Backend-neutral API for atomically publishing Rule-DB records. */
public interface RulePublisher extends AutoCloseable {

	/**
	 * Creates or updates a chain record.
	 *
	 * @param request chain publication request, must not be {@code null}
	 * @return committed position of the chain record
	 * @throws RuleValidationException if the request misses required metadata or content
	 * @throws VersionConflictException if the stored version conflicts with the expectation
	 * @throws RuleStorageException if the backend fails to commit the record
	 */
	PublishResult publishChain(PublishChainRequest request);

	/**
	 * Creates or updates a script record.
	 *
	 * @param request script publication request, must not be {@code null}
	 * @return committed position of the script record
	 * @throws RuleValidationException if the request misses required metadata or content
	 * @throws VersionConflictException if the stored version conflicts with the expectation
	 * @throws RuleStorageException if the backend fails to commit the record
	 */
	PublishResult publishScript(PublishScriptRequest request);

	/**
	 * Removes a chain record.
	 *
	 * @param request removal request identifying the chain, must not be {@code null}
	 * @return committed position of the removal
	 * @throws RuleValidationException if the request misses required metadata
	 * @throws VersionConflictException if the stored version conflicts with the expectation
	 * @throws RuleStorageException if the backend fails to commit the removal
	 */
	PublishResult removeChain(RemoveRuleRequest request);

	/**
	 * Removes a script record.
	 *
	 * @param request removal request identifying the script, must not be {@code null}
	 * @return committed position of the removal
	 * @throws RuleValidationException if the request misses required metadata
	 * @throws VersionConflictException if the stored version conflicts with the expectation
	 * @throws RuleStorageException if the backend fails to commit the removal
	 */
	PublishResult removeScript(RemoveRuleRequest request);

	/**
	 * Releases backend resources held by this publisher. Implementations must be
	 * idempotent: closing an already closed publisher has no effect and must not
	 * raise an error.
	 */
	@Override
	void close();
}

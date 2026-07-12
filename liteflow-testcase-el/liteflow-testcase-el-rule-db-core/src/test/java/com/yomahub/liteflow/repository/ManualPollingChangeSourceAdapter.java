package com.yomahub.liteflow.repository;

/** Test adapter that exposes deterministic polling without widening the production SPI. */
public final class ManualPollingChangeSourceAdapter implements RuleChangeSource, ManualPollingChangeSource {

	private final RuleChangeSource delegate;
	private final Runnable pollAction;

	public ManualPollingChangeSourceAdapter(RuleChangeSource delegate, Runnable pollAction) {
		this.delegate = delegate;
		this.pollAction = pollAction;
	}

	@Override
	public void open(RuleChangeListener listener) {
		delegate.open(listener);
	}

	@Override
	public void activate(long baselineSeq) {
		delegate.activate(baselineSeq);
	}

	@Override
	public ChangeSourceHealth health() {
		return delegate.health();
	}

	@Override
	public void pollOnce() {
		pollAction.run();
	}

	@Override
	public void close() {
		delegate.close();
	}
}

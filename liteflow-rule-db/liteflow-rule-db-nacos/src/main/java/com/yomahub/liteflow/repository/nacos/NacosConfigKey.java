package com.yomahub.liteflow.repository.nacos;

import com.yomahub.liteflow.exception.ConfigErrorException;

/** Resolves and validates the single catalog key owned by one application. */
final class NacosConfigKey {

	private final String dataId;
	private final String group;

	NacosConfigKey(String dataIdPrefix, String applicationName, String group) {
		requireValid("data-id-prefix", dataIdPrefix);
		requireValid("application-name", applicationName);
		requireValid("group", group);
		this.dataId = dataIdPrefix + "." + applicationName + ".catalog.json";
		this.group = group;
	}

	String dataId() {
		return dataId;
	}

	String group() {
		return group;
	}

	private static void requireValid(String name, String value) {
		if (value == null || value.trim().isEmpty()) {
			throw new ConfigErrorException("rule-db nacos " + name + " must not be blank");
		}
		for (int i = 0; i < value.length(); i++) {
			char ch = value.charAt(i);
			if (!Character.isLetterOrDigit(ch) && ch != '_' && ch != '-' && ch != '.' && ch != ':') {
				throw new ConfigErrorException("rule-db nacos " + name
						+ " contains an unsupported character: " + ch);
			}
		}
	}
}

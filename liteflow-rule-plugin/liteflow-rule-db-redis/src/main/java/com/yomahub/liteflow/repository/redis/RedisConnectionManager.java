package com.yomahub.liteflow.repository.redis;

import cn.hutool.core.util.StrUtil;
import com.yomahub.liteflow.exception.ConfigErrorException;
import com.yomahub.liteflow.property.LiteflowConfigGetter;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.spi.holder.ContextAwareHolder;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.ClusterServersConfig;
import org.redisson.config.Config;
import org.redisson.config.SentinelServersConfig;
import org.redisson.config.SingleServerConfig;

/**
 * Redisson 连接：优先复用容器 RedissonClient（redisson-bean-name 指定或按类型），
 * 否则用 address（+ master-name）自动识别 single/sentinel/cluster 自建。
 *
 * @author Bryan.Zhang
 * @since 2.16.2
 */
public class RedisConnectionManager {

	private volatile RedissonClient client;

	private volatile boolean selfCreated = false;

	public RedissonClient getClient() {
		if (client != null) {
			return client;
		}
		synchronized (this) {
			if (client != null) {
				return client;
			}
			RuleDbConfig cfg = LiteflowConfigGetter.get().getRuleDb();
			RedissonClient bean = lookupBean(cfg);
			if (bean != null) {
				client = bean;
				selfCreated = false;
				return client;
			}
			if (StrUtil.isBlank(cfg.getAddress())) {
				throw new ConfigErrorException("rule-db redis: neither a RedissonClient bean nor liteflow.rule-db.address is available");
			}
			client = Redisson.create(buildConfig(cfg));
			selfCreated = true;
			return client;
		}
	}

	private RedissonClient lookupBean(RuleDbConfig cfg) {
		try {
			if (StrUtil.isNotBlank(cfg.getRedissonBeanName())) {
				return ContextAwareHolder.loadContextAware().getBean(cfg.getRedissonBeanName());
			}
			return ContextAwareHolder.loadContextAware().getBean(RedissonClient.class);
		} catch (Exception e) {
			return null;
		}
	}

	private Config buildConfig(RuleDbConfig cfg) {
		Config config = new Config();
		String[] addresses = cfg.getAddress().split(",");
		int db = cfg.getDatabase() == null ? 0 : cfg.getDatabase();
		if (addresses.length > 1) {
			if (StrUtil.isNotBlank(cfg.getMasterName())) {
				// 哨兵
				SentinelServersConfig s = config.useSentinelServers()
						.setMasterName(cfg.getMasterName())
						.setDatabase(db);
				for (String a : addresses) {
					s.addSentinelAddress(normalize(a));
				}
				if (StrUtil.isNotBlank(cfg.getPassword())) {
					s.setPassword(cfg.getPassword());
				}
				if (StrUtil.isNotBlank(cfg.getUsername())) {
					s.setUsername(cfg.getUsername());
				}
			} else {
				// 集群
				ClusterServersConfig c = config.useClusterServers();
				for (String a : addresses) {
					c.addNodeAddress(normalize(a));
				}
				if (StrUtil.isNotBlank(cfg.getPassword())) {
					c.setPassword(cfg.getPassword());
				}
				if (StrUtil.isNotBlank(cfg.getUsername())) {
					c.setUsername(cfg.getUsername());
				}
			}
		} else {
			// 单机
			SingleServerConfig single = config.useSingleServer()
					.setAddress(normalize(addresses[0]))
					.setDatabase(db);
			if (StrUtil.isNotBlank(cfg.getPassword())) {
				single.setPassword(cfg.getPassword());
			}
			if (StrUtil.isNotBlank(cfg.getUsername())) {
				single.setUsername(cfg.getUsername());
			}
		}
		return config;
	}

	/** 允许用户写 host:port 或 redis://host:port，统一补协议 */
	private String normalize(String address) {
		String a = address.trim();
		return a.startsWith("redis://") || a.startsWith("rediss://") ? a : "redis://" + a;
	}

	public synchronized void shutdown() {
		if (client != null && selfCreated) {
			client.shutdown();
		}
		client = null;
		selfCreated = false;
	}
}

package com.yomahub.liteflow.repository.sql;

import cn.hutool.core.util.StrUtil;
import com.yomahub.liteflow.exception.ConfigErrorException;
import com.yomahub.liteflow.property.LiteflowConfigGetter;
import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.spi.holder.ContextAwareHolder;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * SQL 连接获取：优先复用容器 DataSource（datasource-bean-name 指定或自动查找），
 * 否则用 url/username/password 直连。
 *
 * @author Bryan.Zhang
 * @since 2.16.2
 */
public class SqlConnectionManager {

	private volatile DataSource dataSource;

	private volatile boolean resolved = false;

	public Connection getConnection() throws SQLException {
		RuleDbConfig cfg = LiteflowConfigGetter.get().getRuleDb();
		DataSource ds = resolveDataSource(cfg);
		if (ds != null) {
			return ds.getConnection();
		}
		if (StrUtil.isBlank(cfg.getUrl())) {
			throw new ConfigErrorException("rule-db sql: neither a DataSource bean nor liteflow.rule-db.url is available");
		}
		loadDriverIfNeeded(cfg);
		return DriverManager.getConnection(cfg.getUrl(), cfg.getUsername(), cfg.getPassword());
	}

	private DataSource resolveDataSource(RuleDbConfig cfg) {
		if (resolved) {
			return dataSource;
		}
		synchronized (this) {
			if (resolved) {
				return dataSource;
			}
			// url 显式配置时优先直连，不夺容器 DataSource
			if (StrUtil.isBlank(cfg.getUrl())) {
				dataSource = lookupDataSourceBean(cfg.getDatasourceBeanName());
			}
			resolved = true;
			return dataSource;
		}
	}

	private DataSource lookupDataSourceBean(String beanName) {
		try {
			if (StrUtil.isNotBlank(beanName)) {
				return ContextAwareHolder.loadContextAware().getBean(beanName);
			}
			return ContextAwareHolder.loadContextAware().getBean(DataSource.class);
		} catch (Exception e) {
			return null;
		}
	}

	private void loadDriverIfNeeded(RuleDbConfig cfg) {
		String driver = cfg.getDriverClassName();
		if (StrUtil.isBlank(driver)) {
			driver = guessDriver(cfg.getUrl());
		}
		if (StrUtil.isNotBlank(driver)) {
			try {
				Class.forName(driver);
			} catch (ClassNotFoundException e) {
				throw new ConfigErrorException("rule-db sql: driver class not found: " + driver);
			}
		}
	}

	private String guessDriver(String url) {
		if (url == null) {
			return null;
		}
		if (url.startsWith("jdbc:mysql")) {
			return "com.mysql.cj.jdbc.Driver";
		}
		if (url.startsWith("jdbc:h2")) {
			return "org.h2.Driver";
		}
		if (url.startsWith("jdbc:postgresql")) {
			return "org.postgresql.Driver";
		}
		return null; // 交给 DriverManager 的 SPI 自发现
	}
}

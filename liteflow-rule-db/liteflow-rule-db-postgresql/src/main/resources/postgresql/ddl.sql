CREATE TABLE IF NOT EXISTS ${prefix}chain (
  application_name VARCHAR(64) NOT NULL,
  chain_id VARCHAR(128) NOT NULL,
  namespace VARCHAR(64),
  el_data TEXT NOT NULL,
  route_data TEXT,
  version BIGINT NOT NULL DEFAULT 1,
  content_md5 CHAR(32) NOT NULL,
  enable BOOLEAN NOT NULL DEFAULT TRUE,
  gmt_create TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  gmt_modified TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (application_name, chain_id)
);
CREATE TABLE IF NOT EXISTS ${prefix}script (
  application_name VARCHAR(64) NOT NULL,
  node_id VARCHAR(128) NOT NULL,
  script_name VARCHAR(128),
  script_type VARCHAR(32) NOT NULL,
  script_language VARCHAR(32),
  script_data TEXT NOT NULL,
  version BIGINT NOT NULL DEFAULT 1,
  content_md5 CHAR(32) NOT NULL,
  enable BOOLEAN NOT NULL DEFAULT TRUE,
  gmt_create TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  gmt_modified TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (application_name, node_id)
);
CREATE TABLE IF NOT EXISTS ${prefix}change_log (
  seq BIGSERIAL PRIMARY KEY,
  application_name VARCHAR(64) NOT NULL,
  target_type VARCHAR(16) NOT NULL,
  target_id VARCHAR(128) NOT NULL,
  op VARCHAR(16) NOT NULL,
  version BIGINT NOT NULL,
  gmt_create TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS ${prefix}idx_app_seq ON ${prefix}change_log (application_name, seq);

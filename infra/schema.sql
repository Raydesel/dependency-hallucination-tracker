-- One row per (repo, commit, package) observation. The UNIQUE constraint makes
-- the sink idempotent under Kafka's at-least-once delivery.
CREATE TABLE IF NOT EXISTS dependency_observations (
    id              BIGSERIAL PRIMARY KEY,
    repo            TEXT NOT NULL,
    commit_sha      TEXT NOT NULL,
    ecosystem       TEXT NOT NULL,
    package_name    TEXT NOT NULL,
    version_spec    TEXT,
    exists_in_reg   BOOLEAN,
    suspicion_score REAL,
    flags           JSONB,
    validated_at    TIMESTAMPTZ,
    UNIQUE (repo, commit_sha, ecosystem, package_name)
);

CREATE INDEX IF NOT EXISTS idx_obs_ecosystem   ON dependency_observations (ecosystem);
CREATE INDEX IF NOT EXISTS idx_obs_exists       ON dependency_observations (exists_in_reg);
CREATE INDEX IF NOT EXISTS idx_obs_validated_at ON dependency_observations (validated_at);

-- Canonical record per package, aggregated across all observations.
CREATE TABLE IF NOT EXISTS packages (
    ecosystem         TEXT NOT NULL,
    package_name      TEXT NOT NULL,
    exists_in_reg     BOOLEAN,
    first_published   TIMESTAMPTZ,
    latest_version    TEXT,
    max_suspicion     REAL,
    observation_count INT DEFAULT 0,
    last_seen         TIMESTAMPTZ,
    PRIMARY KEY (ecosystem, package_name)
);

CREATE INDEX IF NOT EXISTS idx_pkg_exists ON packages (exists_in_reg);

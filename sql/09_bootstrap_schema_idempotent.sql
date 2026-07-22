-- ============================================================================
-- 09_bootstrap_schema_idempotent.sql
-- Safe to run on every deploy, unlike the original setup script -- uses
-- CREATE TABLE IF NOT EXISTS instead of CREATE OR REPLACE, so re-running
-- this never wipes existing data. This is what gets wired into the bundle
-- as a bootstrap task, closing the gap where DDL wasn't part of CI/CD.
-- ============================================================================

CREATE CATALOG IF NOT EXISTS cdc_demo;
USE CATALOG cdc_demo;

CREATE SCHEMA IF NOT EXISTS source_sim;
CREATE SCHEMA IF NOT EXISTS control;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS control.pipeline_control (
  source_table    STRING,
  target_table    STRING,
  tier            INT,
  watermark_col   STRING,
  primary_key     STRING,
  last_loaded_ts  TIMESTAMP,
  is_active       BOOLEAN
);

-- Seed rows ONLY if the table is genuinely empty -- this is what makes the
-- insert idempotent. Without this guard, every deploy would duplicate the
-- 10 seed rows. The notebook running this file (see below) checks row
-- count in Python and skips this block entirely on subsequent runs. This
-- comment marks where that seed logic lives if you're reading the .sql
-- directly rather than through the bootstrap notebook.
-- INSERT INTO control.pipeline_control VALUES (...)  -- see bootstrap notebook
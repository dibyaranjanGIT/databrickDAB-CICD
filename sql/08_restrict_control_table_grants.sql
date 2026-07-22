-- ============================================================================
-- 08_restrict_control_table_grants.sql
-- The control table governs WHAT gets loaded, in WHAT order, and tracks
-- watermark state -- tampering with it (even accidentally) can silently
-- skip data or corrupt tier ordering. It should have narrower access than
-- the data schemas (bronze/silver/gold), which are comparatively safe to
-- let more people read/query.
-- ============================================================================

USE CATALOG cdc_demo;

-- Start clean: remove any broad grants that may have been applied earlier
-- (e.g. if de_grp was given blanket MODIFY across the whole catalog).
REVOKE ALL PRIVILEGES ON SCHEMA cdc_demo.control FROM `de_grp`;

-- General engineers: read-only. They can inspect watermarks/tiers for
-- debugging, but cannot change what the pipeline will do next.
GRANT USE SCHEMA, SELECT ON SCHEMA cdc_demo.control TO `de_grp`;

-- The pipeline's own identity: needs to READ tier assignments and WRITE
-- watermark updates -- but notice it does NOT get CREATE TABLE here. It
-- should never need to create or drop the control table itself, only
-- read/update rows in the one that already exists.
GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA cdc_demo.control TO `github-actions-cdc-pipeline`;

-- Only a small admin group can alter table structure, add columns, or
-- drop/recreate the control table entirely. Nobody else -- not even the
-- pipeline's service principal -- should have this.
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA cdc_demo.control TO `data-platform-admins`;

-- Optional, tighter still: grant at the TABLE level instead of schema level,
-- if you ever add other tables to the control schema that shouldn't share
-- the same access as pipeline_control specifically.
-- GRANT SELECT, MODIFY ON TABLE cdc_demo.control.pipeline_control TO `github-actions-cdc-pipeline`;

-- Sanity check -- confirm the grants landed as expected
SHOW GRANTS ON SCHEMA cdc_demo.control;
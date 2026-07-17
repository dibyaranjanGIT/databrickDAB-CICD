-- ============================================================================
-- 05_create_silver_gold_schemas.sql
-- Run once before the silver/gold orchestration notebooks.
-- ============================================================================

USE CATALOG cdc_demo;

CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA cdc_demo.silver TO `de_grp`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA cdc_demo.gold   TO `de_grp`;

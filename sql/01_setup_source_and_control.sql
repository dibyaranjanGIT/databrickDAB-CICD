-- ============================================================================
-- 01_setup_source_and_control.sql
-- Run this ONCE to set up the demo environment:
--   - source_sim schema: stands in for your external JDBC source system
--   - control schema: the pipeline_control table (tiered metadata)
--   - bronze schema: ingestion targets (created empty, loader fills them)
-- ============================================================================

-- CREATE CATALOG IF NOT EXISTS cdc_demo;
USE CATALOG cdc_demo;

CREATE SCHEMA IF NOT EXISTS source_sim;   -- pretend this is your SQL Server / Oracle source
CREATE SCHEMA IF NOT EXISTS control;      -- pipeline_control lives here
CREATE SCHEMA IF NOT EXISTS bronze;       -- ingestion targets

-- ============================================================
-- TIER 1: reference / dimension-ish tables (load first)
-- ============================================================
CREATE OR REPLACE TABLE cdc_demo.source_sim.customers (
  customer_id INT, name STRING, city STRING, updated_at TIMESTAMP
);
CREATE OR REPLACE TABLE cdc_demo.source_sim.products (
  product_id INT, product_name STRING, category_id INT, updated_at TIMESTAMP
);
CREATE OR REPLACE TABLE cdc_demo.source_sim.categories (
  category_id INT, category_name STRING, updated_at TIMESTAMP
);

-- ============================================================
-- TIER 2: transactional tables (depend on tier 1 being fresh)
-- ============================================================
CREATE OR REPLACE TABLE cdc_demo.source_sim.orders (
  order_id INT, customer_id INT, order_date DATE, status STRING, updated_at TIMESTAMP
);
CREATE OR REPLACE TABLE cdc_demo.source_sim.order_items (
  order_item_id INT, order_id INT, product_id INT, qty INT, price DECIMAL(10,2), updated_at TIMESTAMP
);
CREATE OR REPLACE TABLE cdc_demo.source_sim.payments (
  payment_id INT, order_id INT, amount DECIMAL(10,2), payment_method STRING, updated_at TIMESTAMP
);

-- ============================================================
-- TIER 3: downstream / operational tables (depend on tier 2)
-- ============================================================
CREATE OR REPLACE TABLE cdc_demo.source_sim.shipments (
  shipment_id INT, order_id INT, carrier STRING, shipped_at TIMESTAMP, updated_at TIMESTAMP
);
CREATE OR REPLACE TABLE cdc_demo.source_sim.returns (
  return_id INT, order_id INT, reason STRING, updated_at TIMESTAMP
);
CREATE OR REPLACE TABLE cdc_demo.source_sim.refunds (
  refund_id INT, return_id INT, amount DECIMAL(10,2), updated_at TIMESTAMP
);
CREATE OR REPLACE TABLE cdc_demo.source_sim.inventory_log (
  log_id INT, product_id INT, change_qty INT, updated_at TIMESTAMP
);

-- seed a little data so the first run has something to pull
INSERT INTO cdc_demo.source_sim.customers VALUES
 (1,'Asha Rao','Bengaluru', now() - INTERVAL 2 HOURS),
 (2,'Ravi Nair','Chennai',  now() - INTERVAL 2 HOURS);

INSERT INTO cdc_demo.source_sim.products VALUES
 (101,'Wireless Mouse',1, now() - INTERVAL 2 HOURS),
 (102,'Mechanical Keyboard',1, now() - INTERVAL 2 HOURS);

INSERT INTO cdc_demo.source_sim.categories VALUES
 (1,'Electronics', now() - INTERVAL 2 HOURS);

INSERT INTO cdc_demo.source_sim.orders VALUES
 (5001,1,current_date(),'PLACED', now() - INTERVAL 1 HOURS),
 (5002,2,current_date(),'PLACED', now() - INTERVAL 1 HOURS);

INSERT INTO cdc_demo.source_sim.order_items VALUES
 (9001,5001,101,1,1499.00, now() - INTERVAL 1 HOURS),
 (9002,5002,102,2,3499.00, now() - INTERVAL 1 HOURS);

INSERT INTO cdc_demo.source_sim.payments VALUES
 (7001,5001,1499.00,'UPI', now() - INTERVAL 1 HOURS);

-- tier 3 tables left empty on purpose -- simulates "not created yet until tier 2 lands"

-- ============================================================
-- CONTROL TABLE
-- ============================================================
CREATE OR REPLACE TABLE cdc_demo.control.pipeline_control (
  source_table    STRING,
  target_table    STRING,
  tier            INT,
  watermark_col   STRING,
  primary_key     STRING,
  last_loaded_ts  TIMESTAMP,
  is_active       BOOLEAN
);

INSERT INTO cdc_demo.control.pipeline_control VALUES
 ('cdc_demo.source_sim.customers',     'cdc_demo.bronze.customers',     1, 'updated_at', 'customer_id',   TIMESTAMP('1900-01-01'), true),
 ('cdc_demo.source_sim.products',      'cdc_demo.bronze.products',      1, 'updated_at', 'product_id',    TIMESTAMP('1900-01-01'), true),
 ('cdc_demo.source_sim.categories',    'cdc_demo.bronze.categories',    1, 'updated_at', 'category_id',   TIMESTAMP('1900-01-01'), true),
 ('cdc_demo.source_sim.orders',        'cdc_demo.bronze.orders',        2, 'updated_at', 'order_id',      TIMESTAMP('1900-01-01'), true),
 ('cdc_demo.source_sim.order_items',   'cdc_demo.bronze.order_items',   2, 'updated_at', 'order_item_id', TIMESTAMP('1900-01-01'), true),
 ('cdc_demo.source_sim.payments',      'cdc_demo.bronze.payments',      2, 'updated_at', 'payment_id',    TIMESTAMP('1900-01-01'), true),
 ('cdc_demo.source_sim.shipments',     'cdc_demo.bronze.shipments',     3, 'updated_at', 'shipment_id',   TIMESTAMP('1900-01-01'), true),
 ('cdc_demo.source_sim.returns',       'cdc_demo.bronze.returns',       3, 'updated_at', 'return_id',     TIMESTAMP('1900-01-01'), true),
 ('cdc_demo.source_sim.refunds',       'cdc_demo.bronze.refunds',       3, 'updated_at', 'refund_id',     TIMESTAMP('1900-01-01'), true),
 ('cdc_demo.source_sim.inventory_log', 'cdc_demo.bronze.inventory_log', 3, 'updated_at', 'log_id',        TIMESTAMP('1900-01-01'), true);

-- sanity check
SELECT * FROM cdc_demo.control.pipeline_control;

-- truncate table cdc_demo.control.pipeline_control;

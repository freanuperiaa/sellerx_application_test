
-- setting up the warehouse first

use role accountadmin;


CREATE WAREHOUSE test_company_wh with warehouse_size = 'x-small';

CREATE DATABASE test_company_db;

CREATE SCHEMA test_company_db.dbo;

-- creating a role for that particular wh, db
CREATE ROLE company_role;

SHOW GRANTS ON WAREHOUSE test_company_wh;
SHOW GRANTS ON DATABASE test_company_db;
SHOW GRANTS ON SCHEMA dbo;

GRANT ALL ON WAREHOUSE test_company_wh TO ROLE company_role;
GRANT ALL ON DATABASE test_company_db TO ROLE company_role;
GRANT CREATE TABLE ON SCHEMA test_company_db.dbo TO ROLE company_role;

GRANT ROLE company_role TO USER freanuperia;


-- using that role, let's make our stage and tables
use role company_role;

CREATE OR REPLACE TABLE test_company_db.dbo.device (
    id NUMBER, -- AUTOINCREMENT?
    type NUMBER,
    store_id NUMBER
);


CREATE OR REPLACE TABLE test_company_db.dbo.store (
    id NUMBER,
    name VARCHAR,
    address VARCHAR,
    city VARCHAR,
    country VARCHAR,
    created_at TIMESTAMP,
    typology VARCHAR,
    customer_id NUMBER
);


CREATE OR REPLACE TABLE test_company_db.dbo.transaction (
    id NUMBER,
    device_id NUMBER,
    product_name VARCHAR,
    product_sku VARCHAR,
    product_name_name VARCHAR,
    amount DECIMAL(25, 10),
    status VARCHAR,
    card_number VARCHAR,
    cvv VARCHAR(3),
    created_at TIMESTAMP,
    happened_at TIMESTAMP
);


CREATE OR REPLACE STAGE company_stage;


list @company_stage;

-- COPY files into the tables
-- COPY INTO my_table
-- FROM @~/staged_file
-- FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO test_company_db.dbo.device
FROM @company_stage/device.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO test_company_db.dbo.store
FROM @company_stage/store.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);


COPY INTO test_company_db.dbo.transaction
FROM @company_stage/transaction.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);


-- let's view those tables

SELECT * FROM test_company_db.dbo.device limit 100;
SELECT * FROM test_company_db.dbo.store limit 100;
SELECT * FROM test_company_db.dbo.transaction limit 100;

select * from test_company_db.dbo.top_products_sold;
select * from test_company_db.dbo.top_stores_per_trans_amt;
select * from test_company_db.dbo.avg_trans_amt_per_store;
select * from test_company_db.dbo.trans_per_device_type;
select * from test_company_db.dbo.avg_trans_time_per_store;
DESC test_company_db.dbo.avg_trans_time_per_store


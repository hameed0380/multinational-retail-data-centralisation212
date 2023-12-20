
---------- orders_table
-- Get Max length
SELECT MAX(LENGTH(card_number)) AS max_length
FROM orders_table;

ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID,
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
ALTER COLUMN product_quantity TYPE SMALLINT;

-- Essentially using this as a template for max length
-- For card_number, store_code, product_code
SELECT MAX(LENGTH(card_number)) AS max_length
INTO TEMPORARY TABLE temp_max_length
FROM orders_table;

-- Alter the table to set the maximum length of the card_number column
DO $$ 
BEGIN
    EXECUTE 'ALTER TABLE orders_table ALTER COLUMN card_number TYPE VARCHAR(' || (SELECT max_length FROM temp_max_length) || ')';
END $$;

-- Drop the temporary table
DROP TABLE temp_max_length;



--------- dim_users_table
ALTER TABLE dim_users
ALTER COLUMN first_name TYPE VARCHAR(255),
ALTER COLUMN last_name TYPE VARCHAR(255),
ALTER COLUMN date_of_birth TYPE DATE,
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
ALTER COLUMN join_date TYPE DATE

-- Essentially using this as a template for max length
-- For country_code
SELECT MAX(LENGTH(country_code)) AS max_length
INTO TEMPORARY TABLE temp_max_length
FROM dim_users;

-- Alter the table to set the maximum length of the column
DO $$ 
BEGIN
    EXECUTE 'ALTER TABLE dim_users ALTER COLUMN country_code TYPE VARCHAR(' || (SELECT max_length FROM temp_max_length) || ')';
END $$;

-- Drop the temporary table
DROP TABLE temp_max_length;



--------- dim_store_details
ALTER TABLE dim_store_details
ALTER COLUMN longitude TYPE FLOAT,
ALTER COLUMN locality TYPE VARCHAR(255),
ALTER COLUMN staff_numbers TYPE SMALLINT,
ALTER COLUMN opening_date TYPE DATE,
ALTER COLUMN store_type TYPE VARCHAR(255),
ALTER COLUMN latitude TYPE FLOAT,
ALTER COLUMN continent TYPE VARCHAR(255)

-- change the location column values where they're null to N/A (already done in cleaning)
UPDATE dim_store_details
SET locality = COALESCE(locality, 'N/A')
WHERE locality IS NULL;

-- Essentially using this as a template for max length
-- For store_code
SELECT MAX(LENGTH(store_code)) AS max_length
INTO TEMPORARY TABLE temp_max_length
FROM dim_store_details;

-- Alter the table to set the maximum length of the column
DO $$ 
BEGIN
    EXECUTE 'ALTER TABLE dim_store_details ALTER COLUMN store_code TYPE VARCHAR(' || (SELECT max_length FROM temp_max_length) || ')';
END $$;

-- Drop the temporary table
DROP TABLE temp_max_length;


--------- dim_store_details
ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR(20);

-- removing £ from product price
UPDATE dim_products
-- Because product price has been changed to float prior in cleaning must be change to string
-- to apply replace the cast back to float
SET product_price = CAST(REPLACE(CAST(product_price AS TEXT), '£', '') AS FLOAT);

-- setting criteria for weight class
UPDATE dim_products
SET weight_class =
    CASE 
        WHEN weight < 2 THEN 'Light'
        WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
        WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
        ELSE 'Truck_Required'
    END;

-- Rename to still_available
ALTER TABLE dim_products
RENAME COLUMN Removed TO still_available;

-- Set the conditions
UPDATE dim_products
SET still_available =
CASE 
	WHEN still_available = 'Still_avaliable' THEN 'True'
	WHEN still_available = 'Removed' THEN 'False'
END;

ALTER TABLE dim_products
ALTER COLUMN weight TYPE FLOAT,
ALTER COLUMN date_added TYPE DATE USING date_added::date,
ALTER COLUMN uuid TYPE UUID USING uuid::uuid,
ALTER COLUMN still_available TYPE BOOL USING still_available::BOOLEAN

-- Essentially using this as a template for max length
-- For EAN, product_code, weight_class
SELECT MAX(LENGTH(EAN)) AS max_length
INTO TEMPORARY TABLE temp_max_length
FROM dim_products;

-- Alter the table to set the maximum length of the column
DO $$ 
BEGIN
    EXECUTE 'ALTER TABLE dim_products ALTER COLUMN EAN TYPE VARCHAR(' || (SELECT max_length FROM temp_max_length) || ')';
END $$;

-- Drop the temporary table
DROP TABLE temp_max_length;



-------- dim_date_times
ALTER TABLE dim_date_times
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid

-- Essentially using this as a template for max length
-- For month, year, day, time_period
SELECT MAX(LENGTH(time_period)) AS max_length
INTO TEMPORARY TABLE temp_max_length
FROM dim_date_times;

-- Alter the table to set the maximum length of the column
DO $$ 
BEGIN
    EXECUTE 'ALTER TABLE dim_date_times ALTER COLUMN time_period TYPE VARCHAR(' || (SELECT max_length FROM temp_max_length) || ')';
END $$;

-- Drop the temporary table
DROP TABLE temp_max_length;



--------- dim_card_details
ALTER TABLE dim_card_details
ALTER COLUMN date_payment_confirmed TYPE DATE

-- Essentially using this as a template for max length
-- For card_number, expiry_date
SELECT MAX(LENGTH(card_number)) AS max_length
INTO TEMPORARY TABLE temp_max_length
FROM dim_card_details;

-- Alter the table to set the maximum length of the column
DO $$ 
BEGIN
    EXECUTE 'ALTER TABLE dim_card_details ALTER COLUMN card_number TYPE VARCHAR(' || (SELECT max_length FROM temp_max_length) || ')';
END $$;

-- Drop the temporary table
DROP TABLE temp_max_length;

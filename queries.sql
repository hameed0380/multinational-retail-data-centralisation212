-- With the temp table code you want to substitue the column name and table name 


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
SELECT MAX(LENGTH(store_code)) AS max_length
INTO TEMPORARY TABLE temp_max_length
FROM orders_table;

-- Alter the table to set the maximum length of the card_number column
DO $$ 
BEGIN
    EXECUTE 'ALTER TABLE orders_table ALTER COLUMN store_code TYPE VARCHAR(' || (SELECT max_length FROM temp_max_length) || ')';
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
SELECT MAX(LENGTH(day)) AS max_length
INTO TEMPORARY TABLE temp_max_length
FROM dim_date_times;

-- Alter the table to set the maximum length of the column
DO $$ 
BEGIN
    EXECUTE 'ALTER TABLE dim_date_times ALTER COLUMN day TYPE VARCHAR(' || (SELECT max_length FROM temp_max_length) || ')';
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



-- Setting primary keys

ALTER TABLE dim_card_details
ADD CONSTRAINT pk_card_number PRIMARY KEY (card_number);

ALTER TABLE dim_store_details
ADD CONSTRAINT pk_store_code PRIMARY KEY (store_code);

ALTER TABLE dim_users
ADD CONSTRAINT pk_user_uuid PRIMARY KEY (user_uuid);

ALTER TABLE dim_products
ADD CONSTRAINT pk_product_code PRIMARY KEY (product_code);

ALTER TABLE dim_date_times
ADD CONSTRAINT pk_date_uuid PRIMARY KEY (date_uuid);



-- Setting foreign keys
ALTER TABLE orders_table
ADD CONSTRAINT fk_card_number FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number),
ADD CONSTRAINT fk_date_uuid FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid),
ADD CONSTRAINT fk_product_code FOREIGN KEY (product_code) REFERENCES dim_products(product_code),
ADD CONSTRAINT fk_store_code FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code),
ADD CONSTRAINT fk_user_uuid FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid);


-- Milestone 4
-- Task 1
SELECT country_code,
        COUNT(store_code)

FROM dim_store_details
GROUP BY country_code;

-- Task 2
SELECT locality,
        COUNT(store_code) as total_no_stores

FROM dim_store_details
GROUP BY locality
ORDER BY total_no_stores DESC;

-- Task 3
SELECT ROUND(CAST(SUM(orders_table.product_quantity * dim_products.product_price) AS NUMERIC), 2) as total_sales, 
            dim_date_times.month 
FROM orders_table
JOIN dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
JOIN dim_products ON orders_table.product_code = dim_products.product_code
GROUP BY dim_date_times.month
ORDER BY total_sales DESC LIMIT 6;

-- Task 4
SELECT COUNT(store_code) as store_num_code,
        SUM(product_quantity) as prod
FROM orders_table
WHERE store_code LIKE '%WEB%' OR store_code LIKE '%Web%';

SELECT COUNT(store_code) as store_num_code,
        SUM(product_quantity) as prod
FROM orders_table
WHERE store_code NOT LIKE '%WEB%' OR store_code NOT LIKE '%Web%';

-- Task 5
SELECT dim_store_details.store_type,
        SUM(orders_table.product_quantity * dim_products.product_price) AS total_sales,
        ROUND(SUM(orders_table.product_quantity) * 100.0 / SUM(SUM(orders_table.product_quantity)) OVER (), 2) 

FROM orders_table
INNER JOIN dim_store_details on dim_store_details.store_code=orders_table.store_code
INNER JOIN dim_products on dim_products.product_code=orders_table.product_code
GROUP BY dim_store_details.store_type


-- Task 6
SELECT SUM(orders_table.product_quantity * dim_products.product_price) AS total_sales,
        dim_date_times.year,
        dim_date_times.month

FROM orders_table
INNER JOIN dim_date_times on dim_date_times.date_uuid=orders_table.date_uuid
INNER JOIN dim_store_details on dim_store_details.store_code=orders_table.store_code
INNER JOIN dim_products on dim_products.product_code=orders_table.product_code
GROUP BY dim_date_times.year, dim_date_times.month
ORDER BY  total_sales DESC
LIMIT 10;

-- Task 7
SELECT country_code,
        SUM(staff_numbers) as num_staff
FROM dim_store_details
GROUP BY country_code
ORDER BY num_staff DESC

-- Task 8
SELECT dim_store_details.country_code,
        dim_store_details.store_type,
        SUM(orders_table.product_quantity * dim_products.product_price) AS total_sales

FROM orders_table
INNER JOIN dim_store_details on dim_store_details.store_code=orders_table.store_code
INNER JOIN dim_products on dim_products.product_code=orders_table.product_code

WHERE dim_store_details.country_code LIKE '%DE%' 
GROUP BY dim_store_details.country_code, dim_store_details.store_type;

-- Task 9
WITH cte AS(
    SELECT CAST(CONCAT(year, '-', month, '-', day, ' ', timestamp) AS TIMESTAMP) as datetimes, year FROM dim_date_times
    ORDER BY datetimes DESC
), cte2 AS(
    SELECT 
        year, 
        datetimes, 
        LEAD(datetimes, 1) OVER (ORDER BY datetimes DESC) as time_difference 
        FROM cte

) SELECT year, AVG((datetimes - time_difference)) as actual_time_taken FROM cte2
GROUP BY year
ORDER BY actual_time_taken DESC
-- Used 2 CTE tables
-- create a timestamp using the year, month, day and timestamp from date_time table and get year
-- used this to calculate the time difference between the current row and the next row in the result set
-- get avg of the datetimes - time_difference
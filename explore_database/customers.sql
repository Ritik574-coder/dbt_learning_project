--#############################################################################################
--########################## CUSTOEMR DATA PROFILING ##########################################
--#############################################################################################

--=============================================================================================
--=========================== customers table overview ========================================
--=============================================================================================
SELECT TOP (1000) [customer_id]
      ,[title]
      ,[first_name]
      ,[last_name]
      ,[full_name]
      ,[gender]
      ,[date_of_birth]
      ,[age]
      ,[email]
      ,[phone]
      ,[address]
      ,[city]
      ,[state]
      ,[state_abbr]
      ,[state_full]
      ,[zip_code]
      ,[country]
      ,[region]
      ,[customer_segment]
      ,[loyalty_points]
      ,[is_active]
      ,[account_created_date]
      ,[preferred_channel]
      ,[annual_income_usd]
      ,[company]
FROM [bronze].[customers]

--=============================================================================================
--========================= customers id NULL and buplicate count =============================
--=============================================================================================
SELECT 
    customer_id,
    COUNT(*) customer_count
FROM 
bronze.customers 
GROUP BY customer_id
HAVING COUNT(*) > 1 OR customer_id IS NULL ;

--=============================================================================================
--============================== null and duplicate hendling ==================================
--=============================================================================================
SELECT 
    *
FROM(
    SELECT 
        *,
    ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY customer_id DESC) AS last_flag
    FROM bronze.customers 
    WHERE customer_id IS NOT NULL 
)t WHERE last_flag = 1 ;

--=============================================================================================
--=========================== customers is_active cleaning ====================================
--=============================================================================================
-- unique value check 
SELECT DISTINCT 
    TRIM(LOWER(is_active)) as is_active
FROM bronze.customers ;


SELECT DISTINCT
CASE TRIM(LOWER(is_active))
    WHEN '0'        THEN 'False'
    WHEN '1'        THEN 'True'
    WHEN 'active'   THEN 'True'
    WHEN 'inactive' THEN 'False'
    WHEN 'false'    THEN 'False'
    WHEN 'true'     THEN 'True'
    WHEN 'n'        THEN 'False'
    WHEN 'y'        THEN 'True'
    WHEN 'no'       THEN 'False'
    WHEN 'yes'      THEN 'True'
    ELSE 'Unknown'
END AS is_active
FROM bronze.customers ;

--=============================================================================================
--=========================== customers preferred_channel cleaning ============================
--=============================================================================================
SELECT DISTINCT 
    TRIM(LOWER(preferred_channel)) AS preferred_channel
FROM bronze.customers ;

SELECT DISTINCT
    CASE TRIM(LOWER(preferred_channel))
        WHEN 'app'        THEN 'Mobile App'
        WHEN 'mobile app' THEN 'Mobile App'
        WHEN 'mobile'     THEN 'Mobile App'
        WHEN 'in store'   THEN 'In Store'
        WHEN 'in-store'   THEN 'In Store'
        WHEN 'store'      THEN 'In Store'
        WHEN 'catalog'    THEN 'Catalog'
        WHEN 'online'     THEN 'Website'
        WHEN 'web'        THEN 'Website'
        WHEN 'phone'      THEN 'Phone'
        ELSE 'Unknown'
    END as preferred_channel
FROM bronze.customers ;

--=============================================================================================
--=============================== customers gender column cleaning ============================
--=============================================================================================
SELECT DISTINCT 
    TRIM(LOWER(gender)) AS gender
FROM bronze.customers ;


SELECT 
    CASE TRIM(LOWER(gender))
        WHEN 'f' THEN 'Female'
        WHEN 'female' THEN 'Female'
        WHEN 'm' THEN 'Male'
        WHEN 'male' THEN 'Male'
        WHEN 'nb' THEN 'Non-Binary'
        WHEN 'non-binary' THEN 'Non-Binary'
        WHEN 'other' THEN 'Other'
        WHEN 'prefer not to say' THEN 'Other'
        ELSE 'Unknown'
    END as gender
FROM bronze.customers

--=============================================================================================
--=============================== customers company column cleaning ===========================
--=============================================================================================

SELECT DISTINCT
CASE 
    WHEN company IS NULL THEN 'Unknown'
    WHEN TRIM(REPLACE(REPLACE(company, CHAR(13), ''), CHAR(10), '')) = '' THEN 'Unknown'
    ELSE TRIM(REPLACE(REPLACE(company, CHAR(13), ''), CHAR(10), ''))
END AS company
FROM bronze.customers;

--=============================================================================================
--=============================== customers country column cleaning ===========================
--=============================================================================================
SELECT DISTINCT 
    TRIM(LOWER(country)) AS country
FROM bronze.customers ;

SELECT DISTINCT
CASE TRIM(LOWER(country))
    WHEN 'u.s.a'         THEN 'United States'
    WHEN 'us'            THEN 'United States'
    WHEN 'usa'           THEN 'United States'
    WHEN 'united states' THEN 'United States'
    ELSE 'Unknown'
END as country
FROM bronze.customers;

--=============================================================================================
--=============================== customers customer_segment cleaning =========================
--=============================================================================================
SELECT DISTINCT 
    customer_segment
FROM bronze.customers

SELECT DISTINCT
CASE 
    WHEN customer_segment IS NULL THEN 'Unknown'
    ELSE customer_segment
END as customer_segment
FROM bronze.customers

--=============================================================================================
--================================== customers rigion cleaning ================================
--=============================================================================================
SELECT DISTINCT
    region
FROM bronze.customers ;

SELECT DISTINCT
    CASE 
        WHEN TRIM(region) IS NULL THEN 'Unknown'
        ELSE TRIM(region)
    END as region
FROM bronze.customers ;

--=============================================================================================
--============================ customers annual_income_usd cleaning ===========================
--=============================================================================================
--total count
SELECT COUNT(*) FROM bronze.customers ;

-- null value count 
SELECT 
    annual_income_usd,
    COUNT(*) as null_count
FROM bronze.customers 
WHERE annual_income_usd IS NULL
GROUP BY annual_income_usd;

-- Diagnose NULLs
SELECT 
    COUNT(*) as total_row,
    COUNT(annual_income_usd) as non_null,
    COUNT(*) - COUNT(annual_income_usd) as total_null
FROM bronze.customers

--check pattern
SELECT * 
FROM bronze.customers
WHERE annual_income_usd IS NULL ;

-- handling null value with using median
SELECT 
customer_segment,
COALESCE(
    annual_income_usd,
    PERCENTILE_CONT(0.5)
    WITHIN GROUP (ORDER BY annual_income_usd)
    OVER(PARTITION BY customer_segment)
) as annual_income_usd
FROM bronze.customers ; 
--=============================================================================================
--============================ customers loyalty_points cleaning ==============================
--=============================================================================================

-- root cause analysis to understood why loyalty_points some value are null
SELECT 
    TOP 100 *
FROM bronze.customers 
WHERE loyalty_points IS NULL ;

-- loyalty_points data profiling
SELECT 
    customer_id ,
    TRIM(full_name) as full_name,
    TRIM(city) as city,
    TRIM(state_full) as state_name,
    zip_code,
    CASE TRIM(LOWER(gender))
        WHEN 'f' THEN 'Female'
        WHEN 'female' THEN 'Female'
        WHEN 'm' THEN 'Male'
        WHEN 'male' THEN 'Male'
        WHEN 'nb' THEN 'Non-Binary'
        WHEN 'non-binary' THEN 'Non-Binary'
        WHEN 'other' THEN 'Other'
        WHEN 'prefer not to say' THEN 'Other'
        ELSE 'Unknown'
    END as gender,
    CASE TRIM(LOWER(preferred_channel))
        WHEN 'app'        THEN 'Mobile App'
        WHEN 'mobile app' THEN 'Mobile App'
        WHEN 'mobile'     THEN 'Mobile App'
        WHEN 'in store'   THEN 'In Store'
        WHEN 'in-store'   THEN 'In Store'
        WHEN 'store'      THEN 'In Store'
        WHEN 'catalog'    THEN 'Catalog'
        WHEN 'online'     THEN 'Website'
        WHEN 'web'        THEN 'Website'
        WHEN 'phone'      THEN 'Phone Call'
        ELSE 'Unknown'
    END as preferred_channel,
    customer_segment,
    loyalty_points
FROM bronze.customers 
WHERE loyalty_points IS NULL;

-- Semantic validation: checking NULL loyalty points by customer segment
SELECT 
    customer_segment,
    COUNT(*) loyalty_points_null_count
FROM bronze.customers
WHERE loyalty_points IS NULL
GROUP BY customer_segment
ORDER BY customer_segment ;

-- Semantic validation: checking NULL loyalty points by region
SELECT 
    region,
    COUNT(*) loyalty_points_null_count
FROM bronze.customers
WHERE loyalty_points IS NULL 
GROUP BY region 
ORDER BY region ;

-- Semantic validation: checking NULL loyalty points by region
WITH cl_gender AS (
    SELECT 
        CASE TRIM(LOWER(gender))
            WHEN 'f' THEN 'Female'
            WHEN 'female' THEN 'Female'
            WHEN 'm' THEN 'Male'
            WHEN 'male' THEN 'Male'
            WHEN 'nb' THEN 'Non-Binary'
            WHEN 'non-binary' THEN 'Non-Binary'
            WHEN 'other' THEN 'Other'
            WHEN 'prefer not to say' THEN 'Other'
            ELSE 'Unknown'
        END as st_gender,
        loyalty_points
    FROM bronze.customers
)
SELECT 
    st_gender,
    COUNT(*) loyalty_points_null_count
FROM cl_gender 
WHERE loyalty_points IS NULL 
GROUP BY st_gender
ORDER BY st_gender ;

-- null percentage  check 
SELECT 
    customer_segment,
    COUNT(
        CASE 
            WHEN loyalty_points IS NULL THEN 1
        END
    ) AS null_count,
    COUNT(*) AS total_customers,
    ROUND(
        COUNT(
            CASE 
                WHEN loyalty_points IS NULL THEN 1
            END
        ) * 100.0 / COUNT(*),2
    ) AS null_percentage
FROM bronze.customers
GROUP BY customer_segment
ORDER BY null_percentage DESC;

-- this is only for gold layer
SELECT 
    customer_segment,
    CAST(COALESCE(
        loyalty_points,
        PERCENTILE_CONT(0.5)
        WITHIN GROUP(ORDER BY loyalty_points)
        OVER(PARTITION BY customer_segment)
    )AS INT) as loyalty_points
FROM bronze.customers ;

--=============================================================================================
--============================== customers zip_code cleaning ==================================
--=============================================================================================
-- customer zip_code data profiling
SELECT DISTINCT
    zip_code,
    LEN(zip_code) as zip_length
FROM bronze.customers 
WHERE LEN(zip_code) != 5 
OR zip_code IS NULL ;

-- customer zip_code data type profiling
SELECT 
    *
FROM bronze.customers
WHERE TRY_CAST(zip_code AS INT) IS NULL
    AND zip_code IS NOT NULL ;
    
-- data type check 
SELECT 
    COLUMN_NAME,
    DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE 
TABLE_SCHEMA = 'bronze'
    AND TABLE_NAME = 'customers'
    AND COLUMN_NAME = 'zip_code'
;

-- fineal query after Semantic validation
WITH clean_zip AS 
(
    SELECT 
        CASE 
            WHEN zip_code IS NULL OR zip_code = '' THEN 'Unknown'
            WHEN LEN(TRIM(zip_code)) != 5 THEN 'Invalid'
            WHEN TRY_CAST(zip_code AS INT) IS NULL THEN 'Invalid'
            ELSE TRIM(zip_code)
        END as zip_code
    FROM bronze.customers
)
SELECT 
    *
FROM clean_zip
WHERE zip_code IS NULL 
    OR zip_code = 'Unknown' 
    OR zip_code = 'Invalid';


--#############################################################################################
--############################## CUSTOEMR CLEAN DATA ##########################################
--#############################################################################################
SELECT TOP (1000) [customer_id]
      ,TRIM(title) as title
      ,TRIM(first_name) as first_name
      ,TRIM(last_name) as last_name
      ,TRIM(full_name) as full_name

      ,CASE TRIM(LOWER(gender))
            WHEN 'f' THEN 'Female'
            WHEN 'female' THEN 'Female'
            WHEN 'm' THEN 'Male'
            WHEN 'male' THEN 'Male'
            WHEN 'nb' THEN 'Non-Binary'
            WHEN 'non-binary' THEN 'Non-Binary'
            WHEN 'other' THEN 'Other'
            WHEN 'prefer not to say' THEN 'Other'
            ELSE 'Unknown'
        END as gender
        
      ,[date_of_birth]
      ,[age]
      ,[email]
      ,[phone]
      ,[address]
      ,[city]
      ,[state]
      ,[state_abbr]
      ,[state_full]

      ,CASE 
            WHEN zip_code IS NULL OR zip_code = '' THEN 'Unknown'
            WHEN LEN(TRIM(zip_code)) != 5 THEN 'Invalid'
            WHEN TRY_CAST(zip_code AS INT) IS NULL THEN 'Invalid'
            ELSE TRIM(zip_code)
       END as zip_code

      ,CASE TRIM(LOWER(country))
            WHEN 'u.s.a'         THEN 'United States'
            WHEN 'us'            THEN 'United States'
            WHEN 'usa'           THEN 'United States'
            WHEN 'united states' THEN 'United States'
            ELSE 'Unknown'
       END as country

      ,CASE 
            WHEN region IS NULL OR TRIM(region) = '' THEN 'Unknown'
            ELSE TRIM(region)
       END as region

      ,CASE 
            WHEN customer_segment IS NULL THEN 'Unknown'
            ELSE customer_segment
       END as customer_segment

      ,[loyalty_points]

      ,CASE TRIM(LOWER(is_active))
            WHEN '0'        THEN 'False'
            WHEN '1'        THEN 'True'
            WHEN 'active'   THEN 'True'
            WHEN 'inactive' THEN 'False'
            WHEN 'false'    THEN 'False'
            WHEN 'true'     THEN 'True'
            WHEN 'n'        THEN 'False'
            WHEN 'y'        THEN 'True'
            WHEN 'no'       THEN 'False'
            WHEN 'yes'      THEN 'True'
            ELSE 'Unknown'
        END AS is_active

      ,[account_created_date]

      ,CASE TRIM(LOWER(preferred_channel))
            WHEN 'app'        THEN 'Mobile App'
            WHEN 'mobile app' THEN 'Mobile App'
            WHEN 'mobile'     THEN 'Mobile App'
            WHEN 'in store'   THEN 'In Store'
            WHEN 'in-store'   THEN 'In Store'
            WHEN 'store'      THEN 'In Store'
            WHEN 'catalog'    THEN 'Catalog'
            WHEN 'online'     THEN 'Website'
            WHEN 'web'        THEN 'Website'
            WHEN 'phone'      THEN 'Phone Call'
            ELSE 'Unknown'
        END as preferred_channel
        
      ,COALESCE(
            annual_income_usd,
            PERCENTILE_CONT(0.5)
            WITHIN GROUP (ORDER BY annual_income_usd)
            OVER (PARTITION BY customer_segment)
      ) as annual_income_usd

      ,CASE 
            WHEN company IS NULL THEN 'Unknown'
            WHEN TRIM(REPLACE(REPLACE(company, CHAR(13), ''), CHAR(10), '')) = '' THEN 'Unknown'
            ELSE TRIM(REPLACE(REPLACE(company, CHAR(13), ''), CHAR(10), ''))
        END as company
FROM [bronze].[customers]

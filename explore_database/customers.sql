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
--#############################################################################################
--############################## CUSTOEMR CLEAN DATA ##########################################
--#############################################################################################
SELECT TOP (1000) [customer_id]
      ,[title]
      ,[first_name]
      ,[last_name]
      ,[full_name]
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
      ,[zip_code]
      ,[country]
      ,[region]
      ,[customer_segment]
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
      ,[annual_income_usd]
      ,CASE 
            WHEN company IS NULL THEN 'Unknown'
            WHEN TRIM(REPLACE(REPLACE(company, CHAR(13), ''), CHAR(10), '')) = '' THEN 'Unknown'
            ELSE TRIM(REPLACE(REPLACE(company, CHAR(13), ''), CHAR(10), ''))
        END as company
FROM [bronze].[customers]




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
FROM [Dbt_DB].[bronze].[customers]

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


SELECT 
DISTINCT
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
END AS is_active
FROM bronze.customers ;

--#############################################################################################
--############################## CUSTOEMR CLEAN DATA ##########################################
--#############################################################################################
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
      ,[preferred_channel]
      ,[annual_income_usd]
      ,[company]
FROM [Dbt_DB].[bronze].[customers]




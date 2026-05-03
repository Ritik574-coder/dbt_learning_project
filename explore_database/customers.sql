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
--=========================== customers id buplicate count ====================================
--=============================================================================================
SELECT 
    customer_id,
    COUNT(*) customer_count
FROM 
bronze.customers 
GROUP BY customer_id
HAVING COUNT(*) > 1 ;

--=============================================================================================
--================================ customers id NULL check ====================================
--=============================================================================================
SELECT 
    customer_id
FROM bronze.customers
WHERE customer_id IS NULL ;
 
--=============================================================================================
--=========================== customers id buplicate count ====================================
--=============================================================================================

SELECT 
    br.review_id,
    bc.customer_id,
    br.customer_id
FROM bronze.reviews as br   
INNER JOIN bronze.customers as bc  
ON br.customer_id = bc.customer_id 
ORDER BY bc.customer_id ASC ;


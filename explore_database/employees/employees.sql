--#############################################################################################
--#################################### EMPLOYEE DATA ##########################################
--#############################################################################################


--=============================================================================================
--=========================== employees table overview ========================================
--=============================================================================================
SELECT TOP (1000) [employee_id]
      ,[first_name]
      ,[last_name]
      ,[full_name]
      ,[email]
      ,[phone]
      ,[job_title]
      ,[department]
      ,[store_id]
      ,[store_name]
      ,[store_city]
      ,[hire_date]
      ,[years_employed]
      ,[annual_salary_usd]
      ,[commission_rate_pct]
      ,[is_active]
      ,[performance_rating]
      ,[manager_id]
  FROM [TestDB].[bronze].[employees]

--=============================================================================================
--=========================== employees_id cleaning ===========================================
--=============================================================================================
-- data profiling employee id  
SELECT 
    employee_id 
FROM bronze.employees 
WHERE employee_id IS NULL 
   OR employee_id < 0
   OR employee_id = '';

-- check those employee id they are successfully convert into int 
SELECT 
    employee_id 
FROM bronze.employees 
WHERE TRY_CONVERT(INT, employee_id) IS NOT NULL;

-- employee_id data type check 
SELECT 
    employee_id 
FROM bronze.employees 
WHERE TRY_CONVERT(INT, employee_id) IS NULL 
    AND employee_id IS NOT NULL; 

-- employee id duplicate check 
SELECT 
    * 
FROM
(
    SELECT 
        employee_id,
        ROW_NUMBER() OVER(PARTITION BY employee_id ORDER BY employee_id) as flag 
    FROM bronze.employees 
    WHERE employee_id IS NOT NULL 
)t WHERE flag != 1 

--=============================================================================================
--================================= name cleaning =============================================
--=============================================================================================
-- employee first_name overview 
SELECT TOP 100
    first_name
FROM bronze.employees ;

-- employee first_name data profiling
SELECT 
      first_name
FROM bronze.employees 
WHERE TRIM(first_name) != first_name 
   OR first_name = ''
   OR first_name IS NULL 

-- employee last_name overview 
SELECT TOP 100
    last_name
FROM bronze.employees ;

-- employee last name data profiling 
SELECT 
      last_name
FROM bronze.employees 
WHERE TRIM(last_name) != last_name 
   OR last_name = ''
   OR last_name IS NULL  ;


-- employee full_name overview 
SELECT TOP 100
    full_name
FROM bronze.employees ;
 
-- employee full_name data profiling 
SELECT 
      full_name 
FROM bronze.employees 
WHERE full_name != TRIM(full_name)
   OR full_name = ''
   OR full_name IS NULL ;

-- checking those first and last name they are not equel to full_name
SELECT 
    TRIM(LOWER(first_name)) as first_name ,
    TRIM(LOWER(last_name)) as last_name ,
    TRIM(LOWER(full_name)) as full_name ,
CONCAT(TRIM(LOWER(first_name)),' ', TRIM(LOWER(last_name))) as full_name_e
FROM bronze.employees
WHERE TRIM(LOWER(full_name)) != CONCAT(TRIM(LOWER(first_name)),' ', TRIM(LOWER(last_name))) ;

-- string parsing to get first and last name from full name
WITH clean_full_name AS 
(
    SELECT 
        CASE 
            WHEN LEN(TRIM(full_name)) - LEN(REPLACE(TRIM(full_name), ' ','')) = 1 THEN PARSENAME(REPLACE(TRIM(full_name), ' ', '.'), 2)
        END as first_name,
            PARSENAME(REPLACE(TRIM(full_name),' ','.'),1) as last_name
    FROM bronze.employees
)
SELECT 
    *
FROM clean_full_name
--#############################################################################################
--############################## EMPLOYEE CLEAN DATA ##########################################
--#############################################################################################
SELECT TOP (1000) 
       [employee_id]
    ,CASE 
        WHEN LEN(TRIM(full_name)) - LEN(REPLACE(TRIM(full_name), ' ','')) = 1 THEN PARSENAME(REPLACE(TRIM(full_name), ' ', '.'), 2)
    END as first_name,
        PARSENAME(REPLACE(TRIM(full_name),' ','.'),1) as last_name
      ,[email]
      ,[phone]
      ,[job_title]
      ,[department]
      ,[store_id]
      ,[store_name]
      ,[store_city]
      ,[hire_date]
      ,[years_employed]
      ,[annual_salary_usd]
      ,[commission_rate_pct]
      ,[is_active]
      ,[performance_rating]
      ,[manager_id]
  FROM [TestDB].[bronze].[employees]



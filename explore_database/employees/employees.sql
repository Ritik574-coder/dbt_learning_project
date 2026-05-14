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

--=============================================================================================
--================================= phone column cleaning =====================================
--=============================================================================================
-- employee phone overview
SELECT 
phone
FROM bronze.employees

-- employee phone data profiling
SELECT 
phone
FROM bronze.employees 
WHERE phone IS NULL 
   OR phone = '' 
   OR TRIM(phone) != phone 
   OR LEN(phone) < 10 ;

-- Performed phone number format profiling using pattern normalization and distribution analysis.
WITH phone_patterns AS 
(
    SELECT 
        TRANSLATE(
            phone,
            '0123456789',
            '9999999999'
        ) as patterns
    FROM bronze.employees
)
SELECT 
     patterns,
     LEN(patterns) AS len_count,
     COUNT(*) as pattern_count,
     CAST(ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS NVARCHAR) + '%' AS percentage 
FROM phone_patterns
     GROUP BY patterns
     ORDER BY pattern_count DESC ;


-- patterns        len_count   pattern_count  percentage      
-- --------------  ----------  -------------  ----------------
-- +99999999999    12          21             21.000000000000%
-- 999.999.9999    12          20             20.000000000000%
-- 9999999999      10          20             20.000000000000%
-- (999) 999-9999  14          20             20.000000000000%
-- 999-999-9999    12          19             19.000000000000%

-- Does the phone number start with '+' and contain exactly 11 characters after it?
SELECT 
    phone 
FROM bronze.employees 
WHERE phone LIKE '+___________' ;

-- Dot-Separated Phone Format
SELECT 
    phone 
FROM bronze.employees 
WHERE phone LIKE '___.___.____' ;

-- Plain 10-Digit Phone Format
SELECT 
    phone 
FROM bronze.employees 
WHERE phone LIKE '__________' ;

-- Parenthesized US Phone Format
SELECT 
    phone 
FROM bronze.employees 
WHERE phone LIKE '(___) ___-____' ;

-- Hyphen-Separated Phone Format
SELECT 
    phone 
FROM bronze.employees 
WHERE phone LIKE '___-___-____' ;

-- Phone Format Normalization and Standardization
SELECT 
    CASE 
        WHEN phone LIKE '+___________'   THEN  CONCAT('+1 (', SUBSTRING(phone, 3, 3), ') ', SUBSTRING(phone, 6, 3), '-', SUBSTRING(phone, 9,4))
        WHEN phone LIKE '___.___.____'   THEN  CONCAT('+1 (', SUBSTRING(phone, 1,3), ') ',  SUBSTRING(phone,5, 3), '-',  SUBSTRING(phone,9, 4))
        WHEN phone LIKE '__________'     THEN  CONCAT('+1 (', SUBSTRING(phone, 1,3), ') ',  SUBSTRING(phone, 4,3), '-',  SUBSTRING(phone,7,4))
        WHEN phone LIKE '___-___-____'   THEN  CONCAT('+1 (', SUBSTRING(phone,1, 3), ') ',  SUBSTRING(phone, 5,8))
        WHEN phone LIKE '(___) ___-____' THEN CONCAT('+1 ',   SUBSTRING(phone, 1, 14))
    END as phone
FROM bronze.employees  ;

--=============================================================================================
--================================= job_title column cleaning =================================
--=============================================================================================
-- employee job_title column overview
SELECT 
    job_title
FROM bronze.employees
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
    ,CASE 
        WHEN phone LIKE '+___________'   THEN  CONCAT('+1 (', SUBSTRING(phone, 3, 3), ') ', SUBSTRING(phone, 6, 3), '-', SUBSTRING(phone, 9,4))
        WHEN phone LIKE '___.___.____'   THEN  CONCAT('+1 (', SUBSTRING(phone, 1,3), ') ',  SUBSTRING(phone,5, 3), '-',  SUBSTRING(phone,9, 4))
        WHEN phone LIKE '__________'     THEN  CONCAT('+1 (', SUBSTRING(phone, 1,3), ') ',  SUBSTRING(phone, 4,3), '-',  SUBSTRING(phone,7,4))
        WHEN phone LIKE '___-___-____'   THEN  CONCAT('+1 (', SUBSTRING(phone,1, 3), ') ',  SUBSTRING(phone, 5,8))
        WHEN phone LIKE '(___) ___-____' THEN CONCAT('+1 ',   SUBSTRING(phone, 1, 14))
    END as phone
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


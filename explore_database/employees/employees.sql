
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
        WHEN phone LIKE '___.___.____'   THEN  CONCAT('+1 (', SUBSTRING(phone, 1,3), ') ' ,  SUBSTRING(phone,5, 3), '-',  SUBSTRING(phone,9,4))
        WHEN phone LIKE '__________'     THEN  CONCAT('+1 (', SUBSTRING(phone, 1,3), ') ' ,  SUBSTRING(phone, 4,3), '-',  SUBSTRING(phone,7,4))
        WHEN phone LIKE '___-___-____'   THEN  CONCAT('+1 (', SUBSTRING(phone,1, 3), ') ' ,  SUBSTRING(phone, 5,8))
        WHEN phone LIKE '(___) ___-____' THEN CONCAT('+1 ',   SUBSTRING(phone, 1, 14))
    END as phone
FROM bronze.employees  ;

--=============================================================================================
--================================= job_title column cleaning =================================
--=============================================================================================
-- No obvious spelling inconsistencies or naming mismatches detected in job titles.
SELECT 
    job_title,
    COUNT(*) as job_title_count
FROM bronze.employees
GROUP BY job_title
ORDER BY job_title_count DESC ;

-- -- Job Title Null Handling and Standardization
SELECT 
    CASE 
        WHEN job_title IS NULL OR job_title = '' THEN 'Unknown'
        ELSE TRIM(job_title) 
    END as job_title
FROM bronze.employees  ;

--=============================================================================================
--================================= job_title column cleaning =================================
--=============================================================================================
-- No department naming inconsistencies detected.
SELECT
     department ,
     COUNT(*) as department_count
FROM bronze.employees 
     GROUP BY department
     ORDER BY department_count DESC ;

-- Department Null Handling and Standardization
SELECT 
    CASE 
        WHEN department IS NULL OR department = '' THEN 'Unknown'
        ELSE TRIM(department)
    END as department
FROM bronze.employees ;

--=============================================================================================
--================================= department column cleaning ================================
--=============================================================================================
-- Store ID Data Validation
SELECT 
      store_id 
FROM  bronze.employees 
WHERE store_id IS NULL 
   OR store_id = ''
   OR TRY_CONVERT(INT, store_id) IS NULL ;

-- Store ID Distribution Analysis
SELECT 
      store_id ,
      COUNT(*) as store_id_count
FROM  bronze.employees 
GROUP BY store_id 
ORDER BY store_id_count DESC ;

-- Store ID Integer Conversion and Validation
SELECT 
    CASE 
        WHEN store_id < 0 OR TRY_CONVERT(INT, store_id) IS NULL THEN NULL
        ELSE TRY_CONVERT(INT, store_id) 
    END as store_id 
FROM bronze.employees ;

--=============================================================================================
--================================= store_name column cleaning ================================
--=============================================================================================

-- Store Name Data Validation
SELECT 
     store_name 
FROM bronze.employees 
WHERE store_name IS NULL 
   OR store_name = ''
   OR LEN(store_name) < 4
   OR TRIM(store_name) != store_name  ;

-- Store Name Distribution Analysis
SELECT 
    store_name,
    COUNT(*) as store_count,
    CAST(ROUND(COUNT(*) * 100/SUM(COUNT(*)) OVER(),2) AS NVARCHAR) + '%' as percentage 
FROM bronze.employees
    GROUP BY store_name
    ORDER BY store_count DESC ;

-- Store Name Cleaning and Standardization
SELECT
    CASE 
        WHEN store_name IS NULL OR store_name = '' THEN 'Unknown'
        ELSE TRIM(store_name)
    END as store_name
FROM bronze.employees ;

--=============================================================================================
--================================= store_city column cleaning ================================
--=============================================================================================
-- Store City Data Validation
SELECT 
     store_city
FROM bronze.employees 
WHERE store_city IS NULL 
   OR store_city = ''
   OR LEN(store_city) < 4 
   OR TRIM(store_city) != store_city ;

-- Store City Distribution Analysis
SELECT 
     store_city,
     COUNT(*) AS store_city_count,
     CAST(ROUND(COUNT(*)*100.0/SUM(COUNT(*)) OVER(), 2) AS NVARCHAR) + '%' as percentage 
FROM bronze.employees 
    GROUP BY store_city 
    ORDER BY store_city_count DESC ;

-- Store City Cleaning and Standardization
SELECT 
    CASE 
        WHEN store_city IS NULL OR LEN(store_city) < 4 OR store_city = '' THEN 'Unknown'
        ELSE TRIM(store_city)
    END store_city
FROM bronze.employees ;

--=============================================================================================
--================================= hire_date column cleaning =================================
--=============================================================================================
-- employee hire_date overview 
SELECT 
    hire_date 
FROM bronze.employees ;

-- employee hire_date Data Validation
SELECT
      hire_date 
FROM  bronze.employees 
WHERE hire_date IS NULL 
    OR hire_date = ''
    OR TRIM(hire_date) != hire_date 
    OR LEN(hire_date) < 8 ;

-- 
WITH date_pattern AS 
(
    SELECT
        TRANSLATE(
            TRIM(LOWER(hire_date)),
            '0123456789abcdefghijklmnopqrstuvwxyz',
            '9999999999aaaaaaaaaaaaaaaaaaaaaaaaaa'
        ) as pattern 
    FROM bronze.employees
)
SELECT 
    pattern ,
    COUNT(*) as pattern_count,
    CAST(ROUND(COUNT(*)*100.0/SUM(COUNT(*)) OVER(), 2)as NVARCHAR) + '%' as percentage 
FROM date_pattern 
    GROUP BY pattern
    ORDER BY pattern_count DESC ; 

-- 
SELECT 
    hire_date
FROM bronze.employees 
WHERE hire_date LIKE '[A-Z][a-z][a-z][a-z]% __, ____' ;

SELECT 
    CASE 
        WHEN hire_date LIKE '[A-Z][a-z][a-z][a-z]% __, ____' THEN TRY_CONVERT(DATE,hire_date)
    END hire_date
FROM bronze.employees 
WHERE hire_date LIKE '[A-Z][a-z][a-z][a-z]% __, ____' ;

--
SELECT 
    hire_date
FROM bronze.employees  
WHERE hire_date LIKE '[A-Z][a-z][a-z] __, ____' ;

SELECT 
    CASE 
        WHEN hire_date LIKE '[A-Z][a-z][a-z] __, ____' THEN TRY_CONVERT(DATE, hire_date)
    END hire_date
FROM bronze.employees  
WHERE hire_date LIKE '[A-Z][a-z][a-z] __, ____' ;

--
SELECT 
    hire_date
FROM bronze.employees 
WHERE hire_date LIKE '____-__-__' ;

SELECT 
    CASE 
        WHEN hire_date LIKE '____-__-__' THEN TRY_CONVERT(DATE, hire_date)
    END AS hire_date
FROM bronze.employees 
WHERE hire_date LIKE '____-__-__' ;

--
SELECT 
    hire_date
FROM bronze.employees 
WHERE hire_date LIKE '____/__/__' ;

--
SELECT 
    CASE 
        WHEN hire_date LIKE '____/__/__' THEN TRY_CONVERT(DATE, hire_date)
    END AS hire_date
FROM bronze.employees 
WHERE hire_date LIKE '____/__/__' ;

--
SELECT 
    hire_date
FROM bronze.employees 
WHERE hire_date LIKE '__/__/____' ;

SELECT 
    CASE 
        WHEN hire_date LIKE '__/__/____' AND TRY_CONVERT(INT, LEFT(hire_date, 2)) > 12 THEN  TRY_CONVERT(DATE, hire_date,103)
    END hire_date
FROM bronze.employees 
WHERE  hire_date LIKE '__/__/____' AND TRY_CONVERT(INT, LEFT(hire_date, 2)) > 12 ;

SELECT 
    CASE 
        WHEN hire_date LIKE '__/__/____' AND TRY_CONVERT(INT, SUBSTRING(hire_date, 4, 2)) > 12 THEN  TRY_CONVERT(DATE, hire_date,101)
    END hire_date
FROM bronze.employees 
WHERE  hire_date LIKE '__/__/____' AND TRY_CONVERT(INT, SUBSTRING(hire_date, 4, 2)) > 12 ;

--
SELECT 
    hire_date
FROM bronze.employees 
WHERE hire_date LIKE '__-__-____' ;

SELECT 
    CASE 
        WHEN hire_date LIKE '__-__-____' AND TRY_CONVERT(INT, LEFT(hire_date, 2)) > 12 THEN TRY_CONVERT(DATE, hire_date, 105)
    END hire_date
FROM bronze.employees 
WHERE  hire_date LIKE '__-__-____' AND TRY_CONVERT(INT, LEFT(hire_date, 2)) > 12 ;

SELECT 
    CASE 
        WHEN hire_date LIKE '__-__-____' AND TRY_CONVERT(INT, SUBSTRING(hire_date, 4, 2)) > 12 THEN TRY_CONVERT(DATE, hire_date, 110)
    END hire_date
FROM bronze.employees 
WHERE  hire_date LIKE '__-__-____' AND TRY_CONVERT(INT, SUBSTRING(hire_date, 4, 2)) > 12 ;



-- FINEAL  QUERY 
SELECT 
    CASE 
        WHEN hire_date LIKE '[A-Z][a-z][a-z][a-z]% __, ____' THEN TRY_CONVERT(DATE,hire_date )
        WHEN hire_date LIKE '[A-Z][a-z][a-z] __, ____'       THEN TRY_CONVERT(DATE, hire_date)
        WHEN hire_date LIKE '____-__-__'                     THEN TRY_CONVERT(DATE, hire_date)
        WHEN hire_date LIKE '____/__/__'                     THEN TRY_CONVERT(DATE, hire_date)

        WHEN hire_date LIKE '__/__/____' AND TRY_CONVERT(INT, LEFT(hire_date, 2)) > 12 THEN  TRY_CONVERT(DATE, hire_date,103)
        WHEN hire_date LIKE '__-__-____' AND TRY_CONVERT(INT, LEFT(hire_date, 2)) > 12 THEN TRY_CONVERT(DATE, hire_date, 105)

        WHEN hire_date LIKE '__/__/____' AND TRY_CONVERT(INT, SUBSTRING(hire_date, 4, 2)) > 12 THEN  TRY_CONVERT(DATE, hire_date,101)
        WHEN hire_date LIKE '__-__-____' AND TRY_CONVERT(INT, SUBSTRING(hire_date, 4, 2)) > 12 THEN TRY_CONVERT(DATE, hire_date, 110)
        ELSE TRY_CONVERT(DATE, hire_date)
    END hire_date
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
        WHEN phone LIKE '___.___.____'   THEN  CONCAT('+1 (', SUBSTRING(phone, 1,3), ') ' ,  SUBSTRING(phone,5, 3), '-',  SUBSTRING(phone,9,4))
        WHEN phone LIKE '__________'     THEN  CONCAT('+1 (', SUBSTRING(phone, 1,3), ') ' ,  SUBSTRING(phone, 4,3), '-',  SUBSTRING(phone,7,4))
        WHEN phone LIKE '___-___-____'   THEN  CONCAT('+1 (', SUBSTRING(phone,1, 3), ') ' ,  SUBSTRING(phone, 5,8))
        WHEN phone LIKE '(___) ___-____' THEN CONCAT('+1 ',   SUBSTRING(phone, 1,14))
    END as phone

    ,CASE 
        WHEN job_title IS NULL OR job_title = '' THEN 'Unknown'
        ELSE TRIM(job_title) 
    END as job_title

    ,CASE 
        WHEN department IS NULL OR department = '' THEN 'Unknown'
        ELSE TRIM(department)
    END as department

    ,CASE 
        WHEN store_id < 0 OR TRY_CONVERT(INT, store_id) IS NULL THEN NULL
        ELSE TRY_CONVERT(INT, store_id) 
    END as store_id 

    ,CASE 
        WHEN store_name IS NULL OR store_name = '' THEN 'Unknown'
        ELSE TRIM(store_name)
    END as store_name

    ,CASE 
        WHEN store_city IS NULL OR LEN(store_city) < 4 OR store_city = '' THEN 'Unknown'
        ELSE TRIM(store_city)
    END store_city

    ,CASE 
        WHEN hire_date LIKE '[A-Z][a-z][a-z][a-z]% __, ____' THEN TRY_CONVERT(DATE,hire_date )
        WHEN hire_date LIKE '[A-Z][a-z][a-z] __, ____'       THEN TRY_CONVERT(DATE, hire_date)
        WHEN hire_date LIKE '____-__-__'                     THEN TRY_CONVERT(DATE, hire_date)
        WHEN hire_date LIKE '____/__/__'                     THEN TRY_CONVERT(DATE, hire_date)

        WHEN hire_date LIKE '__/__/____' AND TRY_CONVERT(INT, LEFT(hire_date, 2)) > 12 THEN  TRY_CONVERT(DATE, hire_date,103)
        WHEN hire_date LIKE '__-__-____' AND TRY_CONVERT(INT, LEFT(hire_date, 2)) > 12 THEN TRY_CONVERT(DATE, hire_date, 105)
        
        WHEN hire_date LIKE '__/__/____' AND TRY_CONVERT(INT, SUBSTRING(hire_date, 4, 2)) > 12 THEN  TRY_CONVERT(DATE, hire_date,101)
        WHEN hire_date LIKE '__-__-____' AND TRY_CONVERT(INT, SUBSTRING(hire_date, 4, 2)) > 12 THEN TRY_CONVERT(DATE, hire_date, 110)
        ELSE TRY_CONVERT(DATE, hire_date)
    END hire_date

      ,[years_employed]

      ,[annual_salary_usd]

      ,[commission_rate_pct]

      ,[is_active]

      ,[performance_rating]

      ,[manager_id]

  FROM [TestDB].[bronze].[employees]


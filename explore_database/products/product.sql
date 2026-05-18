--#############################################################################################
--#################################### PRODUCTS DATA ##########################################
--#############################################################################################

--=============================================================================================
--================================== product table overview ===================================
--=============================================================================================

SELECT TOP (1000) 
       [product_id]
      ,[sku]
      ,[product_name]
      ,[brand]
      ,[category]
      ,[sub_category]
      ,[department]
      ,[base_price_usd]
      ,[cost_price_usd]
      ,[gross_margin_pct]
      ,[weight_kg]
      ,[is_available]
      ,[stock_quantity]
      ,[reorder_level]
      ,[supplier_name]
      ,[supplier_country]
      ,[warranty_years]
      ,[rating_avg]
      ,[review_count]
      ,[launched_date]
      ,[product_url]
  FROM [bronze].[products]

/*
-- creating custom function taht help me to convert string into title case 
CREATE FUNCTION dbo.TitleCase (@text VARCHAR(255))
RETURNS VARCHAR(255)
AS
BEGIN
    DECLARE @result VARCHAR(255) = ''
    DECLARE @word VARCHAR(255)

    ;WITH words AS (
        SELECT value
        FROM STRING_SPLIT(LOWER(@text), ' ')
    )
    SELECT @result = @result +
        UPPER(LEFT(value,1)) +
        SUBSTRING(value,2,LEN(value)) + ' '
    FROM words

    RETURN RTRIM(@result)
END;

SELECT dbo.TitleCase('hello world') AS TitleCasedText ; */

--=============================================================================================
--================================= product_id  column cleaning ===============================
--=============================================================================================
-- Check for duplicates in product_id
SELECT 
    product_id, 
    COUNT(*) AS count
FROM bronze.products 
GROUP BY product_id
HAVING COUNT(*) > 1 ;

-- product_id data profiling
SELECT 
    product_id 
FROM bronze.products 
WHERE product_id IS NULL 
    OR product_id = ''
    OR product_id LIKE '%[^0-9]%' ; 

-- product_id cleaning and standardization
SELECT 
    CASE 
        WHEN TRY_CONVERT(INT, product_id) IS NULL THEN NULL 
        ELSE TRY_CONVERT(INT, product_id)
    END as product_id
FROM bronze.products ;

--=============================================================================================
--=================================== sku  column cleaning ====================================
--=============================================================================================
-- sku data overview 
SELECT 
    sku
FROM bronze.products ; 

-- sku data profiling 
SELECT 
      sku 
FROM  bronze.products
WHERE sku IS NULL 
   OR sku = ''
   OR sku != TRIM(sku)
   OR sku != UPPER(sku)
   OR LEN(sku) != 13 ;

-- sku cleaning and standardization
SELECT 
    CASE 
        WHEN sku = '' OR sku IS NULL OR LEN(sku) != 13 THEN 'Unknown'
        ELSE TRIM(UPPER(sku))
    END as sku
FROM bronze.products ;

--=============================================================================================
--================================ product_name  column cleaning ==============================
--=============================================================================================
-- product_name data overview 
SELECT 
    product_name
FROM bronze.products ;

-- product_name data profiling 
SELECT 
      product_name
FROM  bronze.products 
WHERE product_name IS NULL 
   OR product_name = '' 
   OR product_name != TRIM(product_name) 
   OR product_name != dbo.TitleCase(product_name) ;

-- product_name distribution analysis
SELECT 
      product_name,
      COUNT(*) product_count
FROM  bronze.products 
    GROUP BY product_name
    HAVING COUNT(*) > 1 ;

-- product_name cleaning and standardization
SELECT 
    CASE 
        WHEN product_name = '' OR product_name IS NULL THEN 'Unknown'
        ELSE dbo.TitleCase(TRIM(product_name))
    END as product_name
FROM bronze.products ;

--=============================================================================================
--================================ brand  column cleaning =====================================
--=============================================================================================
-- brand data overview 
SELECT 
    brand
FROM bronze.products ;

-- brand data profiling 
SELECT 
      brand
FROM  bronze.products 
WHERE brand IS NULL 
   OR brand = '' 
   OR brand != TRIM(brand) 
   OR brand != dbo.TitleCase(brand) ;

-- brand distribution analysis
SELECT 
      brand,
      COUNT(*) brand_count
FROM  bronze.products 
    GROUP BY brand
    HAVING COUNT(*) > 1 ;

-- brand cleaning and standardization
SELECT 
    CASE 
        WHEN brand = '' OR brand IS NULL THEN 'Unknown'
        ELSE dbo.TitleCase(TRIM(brand))
    END as brand
FROM bronze.products ;

--=============================================================================================
--================================ category  column cleaning ==================================
--=============================================================================================
-- category data overview 
SELECT 
    category
FROM bronze.products ;

-- category data profiling 
SELECT 
      category
FROM  bronze.products 
WHERE category IS NULL 
   OR category = '' 
   OR category != TRIM(category) 
   OR category != dbo.TitleCase(category) ;

-- category unique value analysis
SELECT DISTINCT 
    category
FROM bronze.products ;

-- category case-sensitivity analysis
SELECT  
    category COLLATE Latin1_General_CI_AS as category,
    COUNT(*) category_count
FROM bronze.products 
GROUP BY category COLLATE Latin1_General_CI_AS
ORDER BY category_count DESC ;

-- category distribution analysis
SELECT 
      category,
      COUNT(*) category_count
FROM  bronze.products 
    GROUP BY category
    HAVING COUNT(*) >= 1 ;

-- category cleaning and standardization
SELECT 
    CASE 
        WHEN category IS NULL OR category = '' THEN 'Unknown'
        ELSE TRIM(dbo.titleCase(category))
    END AS category
FROM bronze.products ;

--=============================================================================================
--================================ sub_category  column cleaning ==============================
--=============================================================================================
--  sub_category data overview
SELECT  
    sub_category
FROM bronze.products ;

--  sub_category data profiling
SELECT 
      sub_category
FROM  bronze.products 
WHERE sub_category IS NULL 
   OR sub_category = ''
   OR sub_category != TRIM(sub_category) 
   OR sub_category != dbo.TitleCase(sub_category) ;

-- sub_category distribution analysis
SELECT 
      TRIM(LOWER(sub_category)) as sub_category,
      COUNT(*) sub_category_count
FROM  bronze.products 
    GROUP BY TRIM(LOWER(sub_category))
    HAVING COUNT(*) > 1 
    ORDER BY sub_category_count DESC ;

-- sub_category unique value analysis
SELECT DISTINCT 
    TRIM(LOWER(sub_category)) as sub_category
FROM bronze.products ;

-- sub_category cleaning and standardization
SELECT 
    CASE 
        WHEN sub_category IS NULL OR sub_category = '' THEN 'Unknown'
        ELSE dbo.TitleCase(TRIM(sub_category))
    END AS sub_category
FROM bronze.products ;

--=============================================================================================
--================================ department  column cleaning ================================
--=============================================================================================
-- department data overview
SELECT 
    department
FROM bronze.products ;

-- department data profiling
SELECT 
      department
FROM  bronze.products 
WHERE department IS NULL 
   OR department = ''
   OR department != TRIM(department)
   OR department != dbo.TitleCase(department) ;

-- department distribution analysis
SELECT 
      department,
      COUNT(*) department_count,
      CAST(ROUND(COUNT(*) * 100.0 /SUM(COUNT(*)) OVER(), 2) AS nvarchar) AS department_percentage
FROM  bronze.products 
    GROUP BY department
    ORDER BY department_count DESC ;

-- department cleaning and standardization
SELECT 
    CASE 
        WHEN department IS NULL OR department = '' THEN 'Unknown'
        ELSE dbo.TitleCase(TRIM(department))
    END AS department
FROM bronze.products ;

--=============================================================================================
--================================ base_price_usd  column cleaning ============================
--=============================================================================================
-- data profiling for base_price_usd
SELECT 
    base_price_usd 
FROM bronze.products 
WHERE base_price_usd IS NULL 
   OR TRY_CONVERT(DECIMAL(10,2), REPLACE(REPLACE(base_price_usd, ',', ''),'$', '')) IS NULL ;

-- base_price_usd cleaning and standardization
WITH CleanBasePrice AS 
(
    SELECT 
        CASE 
            WHEN base_price_usd IS NULL OR TRY_CONVERT(DECIMAL(10,2), REPLACE(REPLACE(base_price_usd, ',', ''),'$', '')) IS NULL THEN NULL
            WHEN TRY_CONVERT(DECIMAL(10,2), REPLACE(REPLACE(base_price_usd, ',', ''),'$', '')) < 0 THEN NULL 
            ELSE TRY_CONVERT(DECIMAL(10,2), REPLACE(REPLACE(base_price_usd, ',', ''),'$', ''))
        END AS base_price_usd
    FROM bronze.products 
)
SELECT 
    *
FROM CleanBasePrice
WHERE base_price_usd IS NULL ;

--=============================================================================================
--================================ base_price_usd  column cleaning ============================
--=============================================================================================


--#############################################################################################
--############################## PRODUCTS CLEAN DATA ##########################################
--#############################################################################################

SELECT TOP (1000) 
    CASE 
        WHEN TRY_CONVERT(INT, product_id) IS NULL THEN NULL 
        ELSE TRY_CONVERT(INT, product_id)
    END as product_id

    ,CASE 
        WHEN sku = '' OR sku IS NULL OR LEN(sku) != 13 THEN 'Unknown'
        ELSE TRIM(UPPER(sku))
    END as sku

    ,CASE 
        WHEN product_name = '' OR product_name IS NULL THEN 'Unknown'
        ELSE TRIM(dbo.TitleCase(product_name))
    END as product_name

    ,CASE 
        WHEN brand = '' OR brand IS NULL THEN 'Unknown'
        ELSE TRIM(dbo.TitleCase(brand))
    END as brand

    ,CASE 
        WHEN category IS NULL OR category = '' THEN 'Unknown'
        ELSE TRIM(dbo.titleCase(category))
    END AS category

    ,CASE 
        WHEN sub_category IS NULL OR sub_category = '' THEN 'Unknown'
        ELSE dbo.TitleCase(TRIM(sub_category))
    END AS sub_category

    ,CASE 
        WHEN department IS NULL OR department = '' THEN 'Unknown'
        ELSE dbo.TitleCase(TRIM(department))
    END AS department

    ,CASE 
        WHEN base_price_usd IS NULL OR TRY_CONVERT(DECIMAL(10,2), REPLACE(REPLACE(base_price_usd, ',', ''),'$', '')) IS NULL THEN NULL
        WHEN TRY_CONVERT(DECIMAL(10,2), REPLACE(REPLACE(base_price_usd, ',', ''),'$', '')) < 0 THEN NULL 
        ELSE TRY_CONVERT(DECIMAL(10,2), REPLACE(REPLACE(base_price_usd, ',', ''),'$', ''))
    END AS base_price_usd

      ,[cost_price_usd]

      ,[gross_margin_pct]

      ,[weight_kg]

      ,[is_available]

      ,[stock_quantity]

      ,[reorder_level]

      ,[supplier_name]

      ,[supplier_country]

      ,[warranty_years]

      ,[rating_avg]

      ,[review_count]

      ,[launched_date]

      ,[product_url]

  FROM [bronze].[products]


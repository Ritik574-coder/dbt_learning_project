--#############################################################################################
--#################################### EMPLOYEE DATA ##########################################
--#############################################################################################

--=============================================================================================
--=========================== inventory_snapshots table overview ==============================
--=============================================================================================
SELECT TOP (1000) [snapshot_date]
      ,[product_id]
      ,[product_name]
      ,[sku]
      ,[category]
      ,[stock_on_hand]
      ,[stock_reserved]
      ,[stock_available]
      ,[reorder_level]
      ,[unit_cost]
      ,[unit_price]
      ,[inventory_value]
      ,[warehouse_location]
      ,[store_id]
  FROM [bronze].[inventory_snapshots]  

--=============================================================================================
--============================== snapshot_date column cleaning ================================
--=============================================================================================
-- snapshot_date overview 
SELECT 
    snapshot_date
FROM bronze.inventory_snapshots ;

-- snapshot_date pattern analysis 
WITH date_pattern_analysis AS 
(
    SELECT 
        TRANSLATE(
            TRIM(LOWER(snapshot_date)), 
            '0123456789abcdefghijklmnopqrstuvwxyz',
            '9999999999aaaaaaaaaaaaaaaaaaaaaaaaaa'
        ) AS date_pattern 
    FROM bronze.inventory_snapshots 
)
SELECT 
    date_pattern,
    COUNT(*) AS pattern_count,
    CAST(
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS NVARCHAR
    ) + '%' AS percentage 
FROM date_pattern_analysis 
GROUP BY date_pattern
ORDER BY pattern_count DESC ;

-- MM/DD/YYYY format analysis 
SELECT
    snapshot_date
FROM bronze.inventory_snapshots
WHERE snapshot_date LIKE '__/__/____' ;

-- MM/DD/YYYY month validation check 
SELECT
    snapshot_date
FROM bronze.inventory_snapshots
WHERE snapshot_date LIKE '__/__/____' 
    AND SUBSTRING(snapshot_date, 4, 2) > 12;

-- DD/MM/YYYY day validation check 
SELECT
    snapshot_date
FROM bronze.inventory_snapshots
WHERE snapshot_date LIKE '__/__/____' 
    AND LEFT(snapshot_date, 2) > 12;

-- Mon DD, YYYY format analysis 
SELECT
    snapshot_date
FROM bronze.inventory_snapshots
WHERE snapshot_date LIKE '[A-Z][a-z][a-z] __, ____' ;  -- DONE

-- YYYY/MM/DD format analysis 
SELECT
    snapshot_date
FROM bronze.inventory_snapshots
WHERE snapshot_date LIKE '____/__/__' ;  -- DONE

-- MM-DD-YYYY format analysis 
SELECT
    snapshot_date
FROM bronze.inventory_snapshots
WHERE snapshot_date LIKE '__-__-____' ;

-- MM-DD-YYYY month validation check 
SELECT
    snapshot_date
FROM bronze.inventory_snapshots
WHERE snapshot_date LIKE '__-__-____' 
    AND SUBSTRING(snapshot_date, 4, 2) > 12;

-- DD-MM-YYYY day validation check 
SELECT
    snapshot_date
FROM bronze.inventory_snapshots
WHERE snapshot_date LIKE '__-__-____' 
    AND LEFT(snapshot_date, 2) > 12;

-- YYYY-MM-DD format analysis 
SELECT
    snapshot_date
FROM bronze.inventory_snapshots
WHERE snapshot_date LIKE '____-__-__' ;  -- DONE

-- Month DD, YYYY format analysis 
SELECT
    snapshot_date
FROM bronze.inventory_snapshots
WHERE snapshot_date LIKE '[A-Z][a-z][a-z][a-z] __, ____' ;  -- DONE

-- Final snapshot_date cleaning validation 
WITH snapshot_date_analysis AS 
(
    SELECT 
        CASE 
            WHEN snapshot_date LIKE '[A-Z][a-z][a-z][a-z] __, ____' THEN TRY_CONVERT(DATE, snapshot_date)
            WHEN snapshot_date LIKE '[A-Z][a-z][a-z] __, ____'      THEN TRY_CONVERT(DATE, snapshot_date)
            WHEN snapshot_date LIKE '____/__/__'                    THEN TRY_CONVERT(DATE, snapshot_date)
            WHEN snapshot_date LIKE '____-__-__'                    THEN TRY_CONVERT(DATE, snapshot_date)

            WHEN snapshot_date LIKE '__/__/____' AND SUBSTRING(snapshot_date, 4, 2) > 12 THEN TRY_CONVERT(DATE, snapshot_date, 101)
            WHEN snapshot_date LIKE '__/__/____' AND LEFT(snapshot_date, 2) > 12         THEN TRY_CONVERT(DATE, snapshot_date, 103)
            WHEN snapshot_date LIKE '__-__-____' AND SUBSTRING(snapshot_date, 4, 2) > 12 THEN TRY_CONVERT(DATE, snapshot_date, 110)
            WHEN snapshot_date LIKE '__-__-____' AND LEFT(snapshot_date, 2) > 12         THEN TRY_CONVERT(DATE, snapshot_date, 105)
            ELSE TRY_CONVERT(DATE, snapshot_date)
        END AS snapshot_date
    FROM bronze.inventory_snapshots
)
SELECT 
    snapshot_date
FROM snapshot_date_analysis
WHERE snapshot_date IS NULL ;

--=============================================================================================
--================================= product_id column cleaning ================================
--=============================================================================================
-- product_id data profiling
SELECT 
    product_id
FROM bronze.inventory_snapshots 
WHERE product_id IS NULL 
OR product_id = ''
OR TRY_CONVERT(INT, product_id) IS NULL ;

-- product_id distribution analysis 
SELECT 
    product_id,
    COUNT(*) product_id_count,
    CAST(ROUND(COUNT(*)*100.0/SUM(COUNT(*)) OVER(), 2) AS NVARCHAR) + '%' as percentages
FROM bronze.inventory_snapshots 
    GROUP BY product_id 
    ORDER BY product_id_count ;

-- Final product_id cleaning validation 
SELECT 
    CASE 
        WHEN TRY_CONVERT(INT, product_id) IS NULL THEN NULL 
        ELSE TRY_CONVERT(INT, product_id)
    END  as product_id
FROM bronze.inventory_snapshots  ;

--=============================================================================================
--================================= product_name column cleaning ==============================
--=============================================================================================
-- producnt_name data profiling 
SELECT 
      product_name 
FROM  bronze.inventory_snapshots 
WHERE product_name IS NULL 
   OR product_name = ''
   OR product_name != TRIM(product_name)
   OR LEN(product_name) < 4 ;

-- product_name distribution analysis 
SELECT 
    product_name,
    COUNT(*) product_name_count,
    CAST(ROUND(COUNT(*)*100.0/SUM(COUNT(*)) OVER(), 2) AS NVARCHAR) + '%' as percentages
FROM bronze.inventory_snapshots 
    GROUP BY product_name 
    ORDER BY product_name_count ;

-- Final Product_name cleaning validation
SELECT 
    CASE 
        WHEN product_name IS NULL OR product_name = '' THEN 'Unknown'
        ELSE TRIM(product_name)
    END as product_name
FROM bronze.inventory_snapshots 

--=============================================================================================
--==================================== sku column cleaning ====================================
--=============================================================================================
-- sku data profiling 
SELECT 
      sku
FROM  bronze.inventory_snapshots 
WHERE sku IS NULL 
   OR sku = ''
   OR sku != TRIM(product_name)
   OR LEN(sku) < 4 ;

-- product_name distribution analysis 
SELECT 
    sku,
    COUNT(*) sku_count,
    CAST(ROUND(COUNT(*)*100.0/SUM(COUNT(*)) OVER(), 2) AS NVARCHAR) + '%' as percentages
FROM bronze.inventory_snapshots 
    GROUP BY sku 
    ORDER BY sku_count ;

-- Final sku cleaning validation
SELECT 
    CASE 
        WHEN sku IS NULL OR sku = '' THEN 'Unknown'
        ELSE TRIM(sku)
    END as sku
FROM bronze.inventory_snapshots ;

--=============================================================================================
--=============================== category column cleaning ====================================
--=============================================================================================
-- category data profiling 
SELECT 
    category 
FROM bronze.inventory_snapshots 
WHERE category IS NULL 
OR category = ''
OR category != TRIM(category) ;

SELECT DISTINCT 
    category 
FROM bronze.inventory_snapshots

-- category distribution analysis 
SELECT 
    category,
    COUNT(*) category_count,
    CAST(ROUND(COUNT(*)*100.0/SUM(COUNT(*)) OVER(), 2) AS NVARCHAR) + '%' as percentages
FROM bronze.inventory_snapshots 
    GROUP BY category
    HAVING COUNT(*) > 1
    ORDER BY category_count DESC;

--#############################################################################################
--############################## EMPLOYEE CLEAN DATA ##########################################
--#############################################################################################

SELECT TOP (1000) 
    CASE 
        WHEN snapshot_date LIKE '[A-Z][a-z][a-z][a-z] __, ____' THEN TRY_CONVERT(DATE ,snapshot_date)
        WHEN snapshot_date LIKE '[A-Z][a-z][a-z] __, ____'      THEN TRY_CONVERT(DATE ,snapshot_date)
        WHEN snapshot_date LIKE '____/__/__'                    THEN TRY_CONVERT(DATE ,snapshot_date)
        WHEN snapshot_date LIKE '____-__-__'                    THEN TRY_CONVERT(DATE ,snapshot_date)
    
        WHEN snapshot_date LIKE '__/__/____' AND SUBSTRING(snapshot_date, 4, 2) > 12 THEN TRY_CONVERT(DATE, snapshot_date, 101)
        WHEN snapshot_date LIKE '__/__/____' AND LEFT(snapshot_date, 2) > 12         THEN TRY_CONVERT(DATE, snapshot_date, 103)
        WHEN snapshot_date LIKE '__-__-____' AND SUBSTRING(snapshot_date, 4, 2) > 12 THEN TRY_CONVERT(DATE, snapshot_date, 110)
        WHEN snapshot_date LIKE '__-__-____' AND LEFT(snapshot_date, 2) > 12         THEN TRY_CONVERT(DATE, snapshot_date, 105)
        ELSE TRY_CONVERT(DATE, snapshot_date)
    END as snapshot_date

    ,CASE 
        WHEN TRY_CONVERT(INT, product_id) IS NULL THEN NULL 
        ELSE TRY_CONVERT(INT, product_id)
    END  as product_id

    ,CASE 
        WHEN product_name IS NULL OR product_name = '' THEN 'Unknown'
        ELSE TRIM(product_name)
    END as product_name

    ,CASE 
        WHEN sku IS NULL OR sku = '' THEN 'Unknown'
        ELSE TRIM(sku)
    END as sku

      ,[category]
      ,[stock_on_hand]
      ,[stock_reserved]
      ,[stock_available]
      ,[reorder_level]
      ,[unit_cost]
      ,[unit_price]
      ,[inventory_value]
      ,[warehouse_location]
      ,[store_id]
  FROM [bronze].[inventory_snapshots]  
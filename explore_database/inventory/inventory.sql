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
-- snapshot_data overview 
SELECT 
    snapshot_date
FROM bronze.inventory_snapshots ;

--- snapshot_data pattern analysis 
WITH date_pattern_analysis AS 
(
    SELECT 
        TRANSLATE(
            TRIM(LOWER(snapshot_date)), 
            '0123456789abcdefghijklmnopqrstuvwxyz',
            '9999999999aaaaaaaaaaaaaaaaaaaaaaaaaa'
        ) as date_pattern 
    FROM bronze.inventory_snapshots 
)
SELECT 
    date_pattern,
    COUNT(*) as pattern_count,
    CAST(ROUND(COUNT(*)*100.0/SUM(COUNT(*)) OVER() ,2) AS NVARCHAR) + '%' as percentage 
FROM date_pattern_analysis 
    GROUP BY date_pattern
    ORDER BY pattern_count DESC ;

--
SELECT
    snapshot_date
FROM bronze.inventory_snapshots
WHERE snapshot_date LIKE '__/__/____' ;

SELECT
    snapshot_date
FROM bronze.inventory_snapshots
WHERE snapshot_date LIKE '__/__/____' AND SUBSTRING(snapshot_date, 4, 2) > 12;

SELECT
    snapshot_date
FROM bronze.inventory_snapshots
WHERE snapshot_date LIKE '__/__/____' AND LEFT(snapshot_date, 2) > 12;
--
SELECT
    snapshot_date
FROM bronze.inventory_snapshots
WHERE snapshot_date LIKE '[A-Z][a-z][a-z] __, ____' ;  -->> DONE
--
SELECT
    snapshot_date
FROM bronze.inventory_snapshots
WHERE snapshot_date LIKE '____/__/__' ;  -->> DONE
--
SELECT
    snapshot_date
FROM bronze.inventory_snapshots
WHERE snapshot_date LIKE '__-__-____' ;

SELECT
    snapshot_date
FROM bronze.inventory_snapshots
WHERE snapshot_date LIKE '__-__-____' AND SUBSTRING(snapshot_date, 4, 2) > 12;

SELECT
    snapshot_date
FROM bronze.inventory_snapshots
WHERE snapshot_date LIKE '__-__-____' AND LEFT(snapshot_date, 2) > 12;
--
SELECT
    snapshot_date
FROM bronze.inventory_snapshots
WHERE snapshot_date LIKE '____-__-__' ; -->> DONE
--
SELECT
    snapshot_date
FROM bronze.inventory_snapshots
WHERE snapshot_date LIKE '[A-Z][a-z][a-z][a-z] __, ____' ;  -->> DONE

-- date_pattern        pattern_count  percentage      
-- ------------------  -------------  ----------------
-- 99/99/9999          152            27.140000000000%  -->> DONE
-- aaa 99, 9999        96             17.140000000000%  -->> DONE
-- 9999/99/99          84             15.000000000000%  -->> DONE
-- 99-99-9999          77             13.750000000000%
-- 9999-99-99          70             12.500000000000%  -->> DONE
-- aaaaaaa 99, 9999    35             6.250000000000%   -->> DONE
-- aaaa 99, 9999       21             3.750000000000%   -->> DONE
-- aaaaaaaa 99, 9999   16             2.860000000000%   -->> DONE
-- aaaaaaaaa 99, 9999  9              1.610000000000%   -->> DONE

-- fineal query 
WITH snapshot_date_analysis AS 
(
    SELECT 
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
    FROM bronze.inventory_snapshots
)
SELECT 
    snapshot_date
FROM snapshot_date_analysis
WHERE snapshot_date IS NULL ;

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
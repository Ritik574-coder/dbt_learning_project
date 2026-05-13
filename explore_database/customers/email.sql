-- stand alone custoemr email query 
WITH standardized AS (
    SELECT
        email AS original_email,
        LOWER(TRIM(email)) AS cleaned_email
    FROM customers
),

fixed_at AS (
    SELECT
        original_email,
        REPLACE(REPLACE(cleaned_email,'@@@','@'),'@@','@') AS cleaned_email
    FROM standardized
),
split_email AS (
    SELECT
        original_email,
        cleaned_email,
        LEFT(cleaned_email,CHARINDEX('@', cleaned_email) - 1) AS local_part,
        RIGHT(cleaned_email,LEN(cleaned_email) - CHARINDEX('@', cleaned_email)) AS domain_part
    FROM fixed_at
    WHERE cleaned_email LIKE '%@%'
),
domain_fixed AS (
    SELECT
        original_email,
        CONCAT(local_part,'@',
            CASE
                WHEN domain_part = 'yahoocom' THEN 'yahoo.com'
                WHEN domain_part = 'iclod.com' THEN 'icloud.com'
                WHEN domain_part = 'outook.com' THEN 'outlook.com'
                WHEN domain_part = 'ahoo.com' THEN 'yahoo.com'
                WHEN domain_part = '@gmail.com' THEN 'gmail.com'
                WHEN domain_part = '@hotmail.com' THEN 'hotmail.com'
                WHEN domain_part = '@yahoo.com' THEN 'yahoo.com'
                ELSE domain_part
            END
        ) AS cleaned_email
    FROM split_email
)
SELECT
    original_email,
    cleaned_email

FROM domain_fixed;

WITH clean_email AS 
(
    SELECT 
    
        CASE
        WHEN PATINDEX('%@%@%', TRIM(LOWER(email))) > 0
            THEN LEFT(TRIM(LOWER(email)), CHARINDEX('@', TRIM(LOWER(email)))) + REPLACE(SUBSTRING(TRIM(LOWER(email)),
            CHARINDEX('@', TRIM(LOWER(email))) + 1,LEN(email)),'@','')

            WHEN email IS NULL OR TRIM(email) = '' THEN 'Unknown'
            ELSE
                CONCAT(
                    LEFT(TRIM(LOWER(email)), CHARINDEX('@', TRIM(LOWER(email))) - 1), '@',
                    CASE
                        WHEN RIGHT(TRIM(LOWER(email)),LEN(TRIM(email)) - CHARINDEX('@', TRIM(email))) = 'yahoocom' THEN 'yahoo.com'
                        WHEN RIGHT(TRIM(LOWER(email)),LEN(TRIM(email)) - CHARINDEX('@', TRIM(email))) = 'iclod.com' THEN 'icloud.com'
                        WHEN RIGHT(TRIM(LOWER(email)),LEN(TRIM(email)) - CHARINDEX('@', TRIM(email))) = 'outook.com' THEN 'outlook.com'
                        WHEN RIGHT(TRIM(LOWER(email)),LEN(TRIM(email)) - CHARINDEX('@', TRIM(email))) = 'ahoo.com' THEN 'yahoo.com'
                        ELSE RIGHT(TRIM(LOWER(email)),LEN(TRIM(email)) - CHARINDEX('@', TRIM(email)))
                    END
                )
        END as email
    FROM bronze.customers
) 
SELECT
    * 
FROM clean_email 
WHERE email NOT LIKE '%_@_%.__%'; 



--==================================================================
WITH clean_email AS 
(
SELECT
    CASE
        WHEN email IS NULL OR TRIM(email) = '' THEN 'Unknown'
        WHEN TRIM(LOWER(email)) NOT LIKE '%@%' THEN 'Unknown'
        WHEN PATINDEX('%@%@%', TRIM(LOWER(email))) > 0 THEN
                LEFT(TRIM(LOWER(email)),CHARINDEX('@', TRIM(LOWER(email))) - 1)
                + '@' +
                REPLACE(
                    SUBSTRING(
                        TRIM(LOWER(email)),
                        CHARINDEX('@', TRIM(LOWER(email))) + 1,
                        LEN(email)),'@','')
        ELSE
            CONCAT(
                LEFT(TRIM(LOWER(email)),CHARINDEX('@', TRIM(LOWER(email))) - 1), '@',
                CASE
                    WHEN RIGHT(
                            TRIM(LOWER(email)),
                            LEN(TRIM(email)) - CHARINDEX('@', TRIM(email))) = 'yahoocom' THEN 'yahoo.com'
                    WHEN RIGHT(
                            TRIM(LOWER(email)),
                            LEN(TRIM(email)) - CHARINDEX('@', TRIM(email))) = 'iclod.com' THEN 'icloud.com'
                    WHEN RIGHT(
                            TRIM(LOWER(email)),
                            LEN(TRIM(email)) - CHARINDEX('@', TRIM(email))) = 'outook.com' THEN 'outlook.com'
                    WHEN RIGHT(
                            TRIM(LOWER(email)),
                            LEN(TRIM(email)) - CHARINDEX('@', TRIM(email))) = 'ahoo.com' THEN 'yahoo.com'
                    ELSE RIGHT(
                            TRIM(LOWER(email)),
                            LEN(TRIM(email)) - CHARINDEX('@', TRIM(email)))
                END
            )
    END AS email

FROM bronze.customers
)
SELECT
    email
FROM clean_email
WHERE
    email NOT LIKE '%@%'
    OR email LIKE '@%'
    OR email LIKE '%@%@%'
    OR email LIKE '%.@%'
    OR email LIKE '%..%'
    OR email NOT LIKE '%@%.%'
# Inventory Snapshots Data Cleaning Documentation

Comprehensive data profiling, cleaning, standardization, and transformation pipeline for the `bronze.inventory_snapshots` dataset.

---

# Objective

The primary goal of this pipeline is to transform raw and inconsistent inventory snapshot data into a clean, analytics-ready dataset.

The transformation process includes:

- Data profiling
- Pattern analysis
- Type validation
- Data standardization
- Null handling
- Derived metric calculation
- Business rule enforcement

---

# Source Table

```sql
bronze.inventory_snapshots
```

---

# Column-Level Cleaning Strategy

---

# 1. snapshot_date

## Problems Identified

- Multiple date formats
- Mixed separators (`/` and `-`)
- Month name formats
- Ambiguous DD/MM/YYYY and MM/DD/YYYY patterns

## Supported Formats

| Format | Example |
|---|---|
| YYYY-MM-DD | 2025-01-10 |
| YYYY/MM/DD | 2025/01/10 |
| MM/DD/YYYY | 01/10/2025 |
| DD/MM/YYYY | 10/01/2025 |
| MM-DD-YYYY | 01-10-2025 |
| DD-MM-YYYY | 10-01-2025 |
| Mon DD, YYYY | Jan 10, 2025 |
| Month DD, YYYY | January 10, 2025 |

## Cleaning Logic

- Used `TRY_CONVERT`
- Applied conditional format detection using `LIKE`
- Resolved ambiguous date formats using validation rules

---

# 2. product_id

## Problems Identified

- Null values
- Empty strings
- Non-numeric values

## Cleaning Logic

```sql
TRY_CONVERT(INT, product_id)
```

Invalid values are converted to `NULL`.

---

# 3. product_name

## Problems Identified

- Null values
- Empty strings
- Leading/trailing spaces

## Cleaning Logic

- Applied `TRIM`
- Replaced invalid values with `'Unknown'`

---

# 4. sku

## Problems Identified

- Null values
- Empty strings
- Whitespace inconsistencies

## Cleaning Logic

- Applied `TRIM`
- Replaced invalid values with `'Unknown'`

---

# 5. category

## Problems Identified

- Case inconsistency
- Trailing spaces
- Multiple category naming variations

## Example Issues

| Raw Value | Cleaned Value |
|---|---|
| electronics | Electronics |
| ELECTRONICS | Electronics |
| electronics  | Electronics |

## Cleaning Logic

- Applied `TRIM`
- Applied `LOWER`
- Standardized using `CASE WHEN`

---

# 6. stock_on_hand

## Problems Identified

- Negative values
- Invalid integers
- Null values

## Cleaning Logic

```sql
TRY_CONVERT(INT, stock_on_hand)
```

Negative and invalid values are converted to `NULL`.

---

# 7. stock_reserved

## Problems Identified

- Negative values
- Invalid numeric formats

## Cleaning Logic

- Validated numeric conversion
- Converted invalid values to `NULL`

---

# 8. stock_available

## Problems Identified

- Missing values
- Invalid integers

## Cleaning Logic

If `stock_available` is missing:

```sql
stock_on_hand - stock_reserved
```

Otherwise:

```sql
TRY_CONVERT(INT, stock_available)
```

---

# 9. reorder_level

## Problems Identified

- Negative values
- Invalid numeric formats

## Cleaning Logic

Invalid values are converted to `NULL`.

---

# 10. unit_cost

## Problems Identified

- Currency symbols
- Mixed numeric formats

## Example Issues

| Raw Value | Cleaned Value |
|---|---|
| $12.50 | 12.50 |

## Cleaning Logic

- Removed `$`
- Converted using `TRY_CONVERT(DECIMAL(10,2))`

---

# 11. unit_price

## Problems Identified

- Currency symbols
- Comma-separated values
- Mixed formatting

## Example Issues

| Raw Value | Cleaned Value |
|---|---|
| $1,200.50 | 1200.50 |
| 1,22.00 | 122.00 |

## Cleaning Logic

- Removed `$`
- Removed `,`
- Applied `TRY_CONVERT(DECIMAL(10,2))`

---

# 12. inventory_value

## Business Logic

Derived metric calculated using:

```sql
unit_price * stock_on_hand
```

## Purpose

Represents estimated inventory valuation based on available stock and selling price.

---

# 13. warehouse_location

## Problems Identified

- Case inconsistencies
- Null values
- Mixed naming formats

## Example Issues

| Raw Value | Cleaned Value |
|---|---|
| wh-a1 | WH-A1 |
| WH-a1 | WH-A1 |

## Cleaning Logic

- Applied `UPPER`
- Standardized warehouse naming
- Replaced missing values with `'Unknown'`

---

# 14. store_id

## Problems Identified

- Null values
- Empty strings
- Invalid numeric values

## Business Observation

A significant number of records contain `NULL` store IDs, which may represent:

- Warehouse-only inventory
- Unassigned inventory
- Centralized stock
- Missing store mappings

## Cleaning Logic

```sql
TRY_CONVERT(INT, store_id)
```

---

# Final Clean Dataset

The final transformation produces a fully standardized inventory dataset suitable for:

- Reporting
- Analytics
- Dashboarding
- Inventory monitoring
- Business intelligence workflows

---

# Key Data Engineering Concepts Applied

- Data profiling
- Pattern recognition
- Standardization
- Defensive casting
- Business-rule validation
- Derived metric calculation
- Null handling
- Data quality enforcement

---

# Architectural Notes

The pipeline follows a layered transformation approach:

| Layer | Purpose |
|---|---|
| Bronze | Raw source data |
| Silver | Cleaned and standardized data |
| Gold | Business metrics and analytics |

---

# Final Output Fields

| Column |
|---|
| snapshot_date |
| product_id |
| product_name |
| sku |
| category |
| stock_on_hand |
| stock_reserved |
| stock_available |
| reorder_level |
| unit_cost |
| unit_price |
| inventory_value |
| warehouse_location |
| store_id |

---

# Outcome

The resulting dataset is significantly more reliable, standardized, and analytics-ready compared to the original raw bronze-layer data.
# 🚀 Building a Best-in-Class Data Pipeline with dbt

This guide provides a comprehensive overview of the data pipeline architecture in the **dbt SQL Server Learning Project**. It outlines the strategic approach to transforming raw data into business-ready insights using the **Medallion Architecture** and **Analytics Engineering** best practices.

---

## 🏗️ 1. Architecture Overview: The Medallion Model

The project is structured into three distinct functional layers to ensure data quality, traceability, and performance.

### 🥉 Bronze Layer (Raw / Landing)
*   **Purpose:** Ingests raw data from source systems "as-is" with no transformations.
*   **Characteristics:** Preserve history, retain all columns (mostly \`VARCHAR\`), and allow nulls/duplicates.
*   **Location:** \`TestDB.bronze.*\` (e.g., \`bronze.customers\`).
*   **dbt Role:** Defined as **Sources** in \`schema.yml\`.

### 🥈 Silver Layer (Standardized / Cleaned)
*   **Purpose:** Standardizes data types, fixes formatting issues, handles NULLs, and enforces business rules.
*   **Characteristics:** One-to-one mapping with Bronze tables but with cleaned data (ISO dates, boolean flags, imputed values).
*   **Location:** \`TestDB.silver.*\` (e.g., \`silver.customers\`).
*   **dbt Role:** **Staging & Intermediate Models**. This is where the bulk of transformation logic resides.

### 🥇 Gold Layer (Curated / Business Marts)
*   **Purpose:** Business-focused aggregations, dimension models, and fact tables ready for BI tools.
*   **Characteristics:** Join multiple silver tables, optimized for reporting, high-level abstractions (e.g., \`dim_customers\`, \`fact_sales\`).
*   **Location:** \`TestDB.gold.*\`.
*   **dbt Role:** **Mart Models**.

---

## 🛠️ 2. dbt Best Practices for this Project

### 📁 A. Model Organization

```
Organize your models directory by layer to keep the project maintainable:

models/
├── silver/     # Transformations, cleaning, standardization
│   ├── customers.sql
│   ├── employees.sql
│   └── schema.yml
└── gold/       # Business logic, aggregations, KPIs
    └── dim_customers.sql
```

### 🔍 B. Source Definition & Testing
Define all raw tables in a \`sources.yml\` file. This allows you to:
1.  **Reference sources safely:** Use \`{{ source('bronze', 'customers') }}\` instead of hardcoded table names.
2.  **Implement Freshness Checks:** Ensure your raw data is up-to-date.
3.  **Apply Early Tests:** Check for \`not_null\` and \`unique\` on primary keys (e.g., \`customer_id\`) at the source level.

### ✨ C. Transformation Logic (Silver Layer)
When building models in the silver layer, follow these design patterns:

1.  **Standardization:** Convert all dates to ISO 8601 (\`YYYY-MM-DD\`) and booleans to a consistent format.
    *   *Reference:* See \`explore_database/customers/bronze_account_create_date.md\` for a deep dive into date cleaning logic.
2.  **NULL Handling:** 
    *   **Preservation:** Keep NULLs if they have business meaning (e.g., \`loyalty_points\`).
    *   **Standardization:** Convert blanks/line breaks to \`'Unknown'\`.
    *   **Imputation:** Use statistical methods (like median income) for missing numeric data when requested by business rules.
3.  **CTE-First Approach:** Use Common Table Expressions (CTEs) to break down complex logic into readable steps (e.g., \`cleaning\`, \`standardizing\`, \`final_select\`).

### 📖 D. Documentation as Code
Leverage the \`meta\` tag in \`schema.yml\` to capture business context that SQL cannot express.
*   **Describe transformations:** Document *why* a certain logic was chosen.
*   **Standardize values:** List expected categorical values (e.g., \`gender\`, \`preferred_channel\`).
*   **Data Quality Notes:** Warn downstream users about known data issues (e.g., duplicate IDs in source).

---

## 🧪 3. Quality Assurance & Testing

A pipeline is only as good as its tests. Use dbt's built-in testing suite:

| Test Type | Purpose | Example |
| :--- | :--- | :--- |
| **Generic Tests** | Standard checks for constraints. | \`unique\`, \`not_null\`, \`accepted_values\`, \`relationships\`. |
| **Singular Tests** | Custom SQL queries that should return 0 rows. | Ensuring \`refund_amount\` is never greater than \`order_amount\`. |
| **Source Freshness** | Alerts if data ingestion stops. | Error if \`raw_sales\` is older than 24 hours. |

---

## 🚀 4. Workflow for Adding a New Model

1.  **Define Source:** Add the raw table to \`models/silver/schema.yml\` under the \`bronze\` source.
2.  **Create SQL Model:** 
    *   Create \`models/silver/new_model.sql\`.
    *   Import source using \`{{ source('bronze', '...') }}\`.
    *   Apply cleaning and standardization logic.
3.  **Document & Test:** Add descriptions and tests to \`schema.yml\`.
4.  **Run & Validate:**
    \`\`\`bash
    dbt run --select new_model
    dbt test --select new_model
    \`\`\`
5.  **Review Lineage:** Use \`dbt docs generate && dbt docs serve\` to verify the dependency graph.

---

## 📈 5. Future Roadmap: The Gold Layer

To reach full maturity, the project should move towards **Dimensional Modeling (Star Schema)**:
*   **Dimensions:** Slow-changing entities like \`dim_customers\`, \`dim_products\`.
*   **Facts:** Immutable events like \`fact_transactions\`, \`fact_inventory\`.
*   **Snapshots:** Use dbt snapshots to track historical changes in source records.

---
*Created by Ritik CLI Ai agent for the dbt SQL Server Learning Project.*

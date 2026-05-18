# Bronze Customers Data Cleaning & Standardization 

## Overview

This is focuses on profiling, validating, cleaning, and standardizing the `bronze.customers` dataset.

The dataset contained multiple real-world data quality issues originating from:

- mixed source-system ingestion
- inconsistent formatting standards
- manual user input errors
- malformed records
- regional formatting inconsistencies
- incomplete customer information

The primary objective of this project was to improve downstream analytical reliability while preserving defensive and audit-friendly transformation behavior.

---

# Dataset Scope

The customer dataset included multiple customer-related attributes such as:

| Column Category | Examples |
|---|---|
| Identity Information | full_name, first_name, last_name |
| Contact Information | email, phone |
| Temporal Information | account_created_date, date_of_birth |
| Geographic Information | state, state_abbr, state_full |

---

# Core Engineering Objectives

The project focused on:

- raw data profiling
- structural anomaly detection
- standardization
- validation logic
- defensive parsing
- ambiguity detection
- malformed record isolation
- downstream analytical consistency
- auditability and safe transformation handling

---

# Customer Name Cleaning & Validation

## Key Problems Identified

- truncated first names
- malformed full names
- inconsistent title formatting
- mismatched reconstructed names

## Implemented Solutions

- reconstructed full-name validation
- title-aware parsing
- first-name extraction
- last-name extraction
- mismatch detection
- defensive normalization

## Major SQL Techniques Used

- TRIM()
- LOWER()
- CONCAT()
- PARSENAME()
- REPLACE()
- ISNULL()

---

# Customer Email Cleaning & Standardization

## Key Problems Identified

- uppercase inconsistencies
- duplicate `@` symbols
- malformed domains
- domain spelling errors
- incomplete email structures

## Implemented Solutions

- lowercase normalization
- whitespace cleanup
- duplicate `@` handling
- domain typo correction
- structural validation
- malformed email isolation

## Major SQL Techniques Used

- LOWER()
- TRIM()
- PATINDEX()
- LEFT()
- SUBSTRING()
- REPLACE()
- CHARINDEX()
- CONCAT()

---

# Customer Phone Number Cleaning

## Key Problems Identified

- inconsistent US phone formats
- malformed phone structures
- mixed separators
- incomplete numbers

## Implemented Solutions

- structural pattern profiling
- pattern-aware parsing
- US phone standardization
- malformed pattern isolation

## Final Standardized Format

```text
+1 (AAA) BBB-CCCC
```

## Major SQL Techniques Used

- SUBSTRING()
- CONCAT()
- CASE
- TRIM()

---

# Account Created Date Cleaning

## Key Problems Identified

- mixed regional date formats
- locale ambiguity
- inconsistent temporal representations

## Implemented Solutions

- structural pattern extraction
- deterministic format conversion
- locale-aware classification
- ambiguity detection
- ISO 8601 standardization

## Major SQL Techniques Used

- TRY_CONVERT()
- CONVERT()
- LEFT()
- SUBSTRING()
- TRANSLATE()

---

# Date of Birth (DOB) Cleaning

## Key Problems Identified

- mixed regional DOB formats
- ambiguous slash-formatted dates
- malformed temporal records

## Implemented Solutions

- structural pattern profiling
- statistical format analysis
- conditional locale parsing
- SQL style-code conversion
- defensive fallback logic

## Major SQL Techniques Used

- TRY_CONVERT()
- CONVERT()
- TRANSLATE()
- LEFT()
- SUBSTRING()

---

# State Column Quality Analysis

## Key Problems Identified

- inconsistent state naming
- abbreviation/full-name duplication
- spelling inconsistencies
- redundant storage

## Final Engineering Decision

Retained:
- state_abbr
- state_full

Removed:
- state

Reason:
The `state` column was redundant and provided no additional business value.

---

# Defensive Engineering Principles Applied

Throughout the project, transformations prioritized:

- data integrity
- auditability
- conservative correction logic
- ambiguity transparency
- error-safe conversion
- malformed record isolation

Potentially unsafe assumptions were intentionally avoided.

---

# Final Engineering Outcome

This project successfully achieved:

- customer data profiling
- anomaly detection
- structural standardization
- defensive ETL design
- locale-aware parsing
- validation-driven transformations
- standardized analytical formatting
- improved downstream usability

The final implementation significantly improved customer data consistency, analytical reliability, and transformation transparency while preserving safe handling of unresolved or malformed source-system records.

---

# Technologies Used

- Microsoft SQL Server
- T-SQL
- CASE Logic
- TRY_CONVERT()
- String Functions
- Pattern Profiling
- Data Validation Techniques
- ISO 8601 Standardization

---

# Project Type

End-to-End Data Cleaning & Standardization Pipeline for Customer Master Data
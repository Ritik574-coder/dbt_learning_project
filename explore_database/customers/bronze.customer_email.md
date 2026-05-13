# Customer Email Cleaning and Standardization Documentation

## Overview

The `email` column in the `bronze.customers` dataset contained multiple structural inconsistencies caused by manual user input errors, formatting inconsistencies, and source-system quality issues.

The main objective of the email cleaning process was to:

* inspect raw email structures
* identify common formatting inconsistencies
* standardize valid email records
* correct recoverable domain issues
* isolate malformed or invalid email patterns
* preserve data integrity during transformation

The final target format was:

```text id="7w2h4v"
local_part@domain.com
```

Standardization focused on:

* lowercase normalization
* whitespace removal
* duplicate `@` cleanup
* domain typo correction
* structural validation

---

# Step 1 — Raw Data Inspection

Initial inspection was performed to understand the structure and quality of raw email values.

The following validations were performed:

* raw column inspection
* distinct email inspection
* null and empty value analysis
* domain extraction analysis
* malformed structure profiling

This inspection confirmed that the dataset contained:

* uppercase and lowercase inconsistencies
* leading and trailing spaces
* multiple `@` symbols
* malformed domains
* missing dots in domains
* incomplete email structures
* null and empty values

---

# Step 2 — Standardization Preparation

Initial normalization was performed using:

* `LOWER()`
* `TRIM()`

Example:

| Raw Email        | Standardized     |
| ---------------- | ---------------- |
| `JOHN@GMAIL.COM` | `john@gmail.com` |

This step ensured consistent casing and whitespace removal before structural processing.

---

# Step 3 — Structural Pattern Profiling

Structural inspection identified several categories of malformed email patterns.

Detected issue categories included:

| Issue Type              | Example            |
| ----------------------- | ------------------ |
| uppercase inconsistency | `JOHN@GMAIL.COM`   |
| leading/trailing spaces | `john@gmail.com`   |
| multiple `@` symbols    | `john@@@gmail.com` |
| missing domain dot      | `john@yahoocom`    |
| domain spelling errors  | `john@outook.com`  |
| incomplete local part   | `@@gmail.com`      |
| missing `@` symbol      | `johngmail.com`    |
| null values             | `NULL`             |

This profiling stage was critical because each issue required different transformation logic.

---

# Step 4 — Multiple `@` Symbol Detection

Some records contained more than one `@` symbol.

Examples:

| Invalid Email      |
| ------------------ |
| `john@@gmail.com`  |
| `john@@@gmail.com` |
| `amy@@@yahoo.com`  |

Detection logic was implemented using:

```sql id="z0g5jq"
PATINDEX('%@%@%', email)
```

This pattern identifies records containing multiple `@` symbols.

---

# Step 5 — Multiple `@` Cleanup

Recoverable multiple-`@` patterns were standardized by:

* preserving the first valid `@`
* removing all remaining `@` symbols

Example:

| Raw Email          | Cleaned Email    |
| ------------------ | ---------------- |
| `john@@@gmail.com` | `john@gmail.com` |
| `amy@@yahoo.com`   | `amy@yahoo.com`  |

Transformation logic used:

* `LEFT()`
* `SUBSTRING()`
* `REPLACE()`
* `CHARINDEX()`

This approach safely reconstructed recoverable email structures.

---

# Step 6 — Domain Extraction and Validation

After structural cleanup, the email address was divided into:

| Component   | Description         |
| ----------- | ------------------- |
| local part  | username before `@` |
| domain part | domain after `@`    |

Example:

| Email            | Local Part | Domain      |
| ---------------- | ---------- | ----------- |
| `john@gmail.com` | `john`     | `gmail.com` |

Domain extraction logic used:

* `LEFT()`
* `RIGHT()`
* `CHARINDEX()`
* `LEN()`

---

# Step 7 — Domain Typo Detection

Several malformed domain spellings were identified during profiling.

Examples:

| Invalid Domain | Correct Domain |
| -------------- | -------------- |
| `yahoocom`     | `yahoo.com`    |
| `iclod.com`    | `icloud.com`   |
| `outook.com`   | `outlook.com`  |
| `ahoo.com`     | `yahoo.com`    |

These issues most likely originated from:

* manual typing errors
* missing punctuation
* incomplete domain entry
* source-system inconsistencies

---

# Step 8 — Domain Standardization

Known recoverable domain typos were corrected using `CASE`-based domain mapping logic.

Example transformation:

| Raw Email        | Cleaned Email     |
| ---------------- | ----------------- |
| `john@yahoocom`  | `john@yahoo.com`  |
| `amy@outook.com` | `amy@outlook.com` |

This approach standardized common recoverable domain errors while preserving the original local part.

---

# Step 9 — Structural Email Validation

Basic structural validation was implemented to isolate clearly invalid records.

Validation checks included:

| Validation Rule          | Purpose               |
| ------------------------ | --------------------- |
| `email IS NULL`          | detect missing values |
| `TRIM(email) = ''`       | detect empty strings  |
| `NOT LIKE '%@%'`         | detect missing `@`    |
| `PATINDEX('%@%@%', ...)` | detect multiple `@`   |

Examples of invalid structures:

| Invalid Email     |
| ----------------- |
| `@@gmail.com`     |
| `johngmail.com`   |
| `john@gmail..com` |
| `@hotmail.com`    |

These records could not always be safely reconstructed without introducing incorrect data assumptions.

---

# Step 10 — Safe Handling of Invalid Records

Malformed or unresolved email patterns were safely classified using:

```text id="x3h7kt"
Unknown
```

This defensive handling strategy ensured:

* pipeline stability
* prevention of incorrect auto-generated emails
* preservation of data integrity
* safe downstream analytical behavior

---

# Step 11 — Final Standardization Pipeline

The final email transformation pipeline implemented:

* lowercase normalization
* whitespace trimming
* multiple `@` cleanup
* domain typo correction
* structural validation
* malformed email isolation

Transformation logic used:

* `CASE`
* `LOWER()`
* `TRIM()`
* `PATINDEX()`
* `LEFT()`
* `RIGHT()`
* `SUBSTRING()`
* `REPLACE()`
* `CHARINDEX()`
* `CONCAT()`

---

# Final Outcome

The email cleaning pipeline successfully achieved:

* raw email inspection
* structural issue profiling
* lowercase normalization
* whitespace standardization
* duplicate `@` cleanup
* recoverable domain correction
* malformed structure detection
* invalid email isolation
* safe transformation behavior
* standardized analytical formatting

The final implementation significantly improved email consistency, structural quality, and downstream usability while preserving defensive handling of malformed or unresolved source-system data.

Reference pipeline style and transformation structure were aligned with the existing customer cleaning workflow documentation. 

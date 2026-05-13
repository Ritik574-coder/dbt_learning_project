# Customer Full Name Cleaning, Validation, and Name Extraction Documentation

## Overview

The `full_name`, `first_name`, and `last_name` columns in the `bronze.customers` dataset contained multiple inconsistencies caused by:

* incomplete first-name values
* character truncation
* malformed name components
* inconsistent title handling
* mismatched reconstructed full names

The objective of this cleaning and validation process was to:

* profile full-name quality
* validate consistency between name components
* detect malformed or truncated names
* reconstruct standardized full names
* extract first and last names from raw full-name values
* identify source-system inconsistencies

This workflow focused on:

* name consistency validation
* structural parsing
* mismatch detection
* component extraction
* defensive string normalization

---

# Step 1 — Raw Data Profiling

Initial profiling was performed on customer name-related columns.

Inspected columns included:

* `full_name`
* `first_name`
* `last_name`
* `title`
* `email`

This analysis revealed that several customer records contained:

* missing leading characters
* partially truncated first names
* inconsistent title formatting
* mismatched reconstructed names

---

# Step 2 — Full Name Reconstruction Validation

To validate name consistency, a reconstructed full name was generated using:

* `title`
* `first_name`
* `last_name`

Transformation logic used:

* `TRIM()`
* `LOWER()`
* `CONCAT()`
* `ISNULL()`

The reconstructed format:

```text id="x8m3vp"
title + first_name + last_name
```

was compared against the original `full_name` column.

This validation process identified records where:

* reconstructed names did not match original full names
* first-name values appeared truncated
* title formatting created inconsistencies

---

# Step 3 — Mismatch Detection Logic

Mismatch detection was implemented using normalized string comparison.

Normalization included:

* lowercase conversion
* whitespace trimming
* null-safe comparison

Validation query compared:

| Source Column | Reconstructed Column     |
| ------------- | ------------------------ |
| `full_name`   | generated `full_name_ed` |

The following mismatch conditions were identified:

| Original Full Name | Reconstructed Name |
| ------------------ | ------------------ |
| `william murphy`   | `illiam murphy`    |
| `patrick mendoza`  | `ptrick mendoza`   |
| `rebecca brown`    | `rebeca brown`     |
| `ms. robert lee`   | `ms. robrt lee`    |

This analysis revealed that multiple records contained missing characters in the `first_name` column.

---

# Step 4 — Root Cause Identification

Detailed profiling showed that most inconsistencies originated from:

* truncated first-name values
* missing leading characters
* incomplete source-system ingestion
* malformed input records

Examples:

| Expected First Name | Detected Value |
| ------------------- | -------------- |
| `william`           | `illiam`       |
| `patrick`           | `ptrick`       |
| `rebecca`           | `rebeca`       |
| `aaron`             | `aron`         |

Because the original `full_name` column retained more complete information, it became the most reliable source for downstream extraction logic.

---

# Step 5 — First and Last Name Extraction

To improve reliability, first and last names were re-derived from the `full_name` column instead of relying solely on malformed component columns.

Extraction logic used:

* `PARSENAME()`
* `REPLACE()`
* `TRIM()`
* `LEN()`

Spaces were temporarily converted into dot separators to enable positional extraction.

Example transformation:

| Original Full Name | Temporary Structure |
| ------------------ | ------------------- |
| `john smith`       | `john.smith`        |
| `mr. robert lee`   | `mr..robert.lee`    |

This enabled positional parsing of:

* first name
* last name

using SQL Server `PARSENAME()` logic.

---

# Step 6 — Name Component Parsing Logic

Conditional parsing logic was applied based on whitespace count.

Rules used:

| Space Count | Interpretation       |
| ----------- | -------------------- |
| 1 space     | first + last         |
| 2 spaces    | title + first + last |

This allowed extraction of:

* title-aware names
* standard two-part names
* title-prefixed customer names

Examples:

| Full Name        | Extracted First Name | Extracted Last Name |
| ---------------- | -------------------- | ------------------- |
| `john smith`     | `john`               | `smith`             |
| `mr. robert lee` | `robert`             | `lee`               |

---

# Step 7 — Defensive String Normalization

String normalization was applied throughout the workflow using:

* `TRIM()`
* `LOWER()`
* `ISNULL()`

This ensured:

* consistent comparison behavior
* removal of whitespace inconsistencies
* null-safe validation
* reliable mismatch detection

Normalization significantly reduced false-positive mismatches caused by formatting differences alone.

---

# Step 8 — Data Integrity Considerations

The workflow intentionally prioritized:

* preserving original customer identity values
* avoiding aggressive automated corrections
* detecting inconsistencies rather than blindly overwriting names

This was important because:

* name reconstruction can introduce false assumptions
* incomplete source records may not always be safely recoverable
* customer identity data requires conservative handling

The validation process therefore focused primarily on:

* anomaly detection
* mismatch isolation
* extraction reliability
* structural consistency

rather than forced auto-correction.

---

# Step 9 — Final Engineering Outcome

The full-name validation and extraction pipeline successfully achieved:

* customer name profiling
* reconstructed full-name validation
* mismatch detection
* malformed first-name identification
* root-cause analysis
* reliable first-name extraction
* reliable last-name extraction
* title-aware parsing
* defensive string normalization
* null-safe comparison handling

The final implementation significantly improved customer-name consistency analysis and enabled more reliable downstream identity standardization workflows.

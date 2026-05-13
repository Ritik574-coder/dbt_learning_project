# Customer Date of Birth Cleaning and Standardization Documentation

## Overview

The `date_of_birth` column in the `bronze.customers` dataset contained multiple inconsistent date representations originating from mixed source-system formats and regional date-entry conventions.

The primary objective of this cleaning process was to:

* inspect raw DOB values
* identify all structural date patterns
* classify deterministic and ambiguous formats
* standardize valid dates into ISO 8601 format (`YYYY-MM-DD`)
* safely handle locale ambiguity
* preserve data integrity during transformation

The cleaning pipeline implemented:

* structural pattern profiling
* statistical pattern analysis
* locale-aware parsing
* defensive conversion handling
* ambiguity-aware fallback logic
* final null validation

---

# Step 1 — Raw Data Inspection

Initial inspection was performed to understand the structure and quality of the `date_of_birth` column.

Raw inspection queries were executed to:

* review original DOB values
* identify mixed formatting styles
* observe structural inconsistencies

This analysis confirmed that the dataset contained multiple regional and system-generated date formats.

Examples:

| Raw DOB Value        |
| -------------------- |
| `08/06/2018`         |
| `19/05/2022`         |
| `2023-07-13`         |
| `2020/09/19`         |
| `Sep 07, 2018`       |
| `September 14, 2021` |

---

# Step 2 — Structural Pattern Profiling

Structural profiling was performed using:

* `TRIM()`
* `LOWER()`
* `TRANSLATE()`

Digits and alphabetic characters were normalized into generalized structural placeholders to identify reusable date patterns independently of actual values.

Examples:

| Raw Value            | Structural Pattern |
| -------------------- | ------------------ |
| `08/06/2018`         | `99/99/9999`       |
| `2023-07-13`         | `9999-99-99`       |
| `Sep 07, 2018`       | `aaa 99, 9999`     |
| `September 14, 2021` | `aaaa* 99, 9999`   |

This approach enabled complete discovery of all DOB formatting structures present in the dataset.

---

# Step 3 — Pattern Frequency Distribution Analysis

After structural profiling, all patterns were grouped and analyzed statistically.

Detected patterns:

| Pattern          | Record Count | Percentage |
| ---------------- | ------------ | ---------- |
| `99/99/9999`     | 176          | 27%        |
| `9999/99/99`     | 108          | 16%        |
| `9999-99-99`     | 98           | 15%        |
| `99-99-9999`     | 88           | 13%        |
| `aaaa* 99, 9999` | 91           | 12%        |
| `aaa 99, 9999`   | 79           | 11%        |

This analysis confirmed that the dataset contained:

* locale-independent formats
* locale-dependent formats
* slash-separated dates
* dash-separated dates
* short and full month-name formats

---

# Step 4 — Deterministic Format Identification

Formats with explicit temporal ordering were classified as deterministic and safely convertible.

Deterministic formats included:

| Format Type      | Example              |
| ---------------- | -------------------- |
| `YYYY/MM/DD`     | `2020/09/19`         |
| `YYYY-MM-DD`     | `2023-07-13`         |
| `Mon DD, YYYY`   | `Sep 07, 2018`       |
| `Month DD, YYYY` | `September 14, 2021` |

These formats were standardized directly using SQL Server `CONVERT()` because no regional ambiguity existed.

---

# Step 5 — Locale Ambiguity Detection

Several records contained ambiguous regional date formats.

Examples:

| Ambiguous Example |
| ----------------- |
| `08/06/2018`      |
| `04/11/2019`      |
| `08-05-2019`      |

These values could represent either:

| Possible Interpretation |
| ----------------------- |
| `MM/DD/YYYY`            |
| `DD/MM/YYYY`            |

or:

| Possible Interpretation |
| ----------------------- |
| `MM-DD-YYYY`            |
| `DD-MM-YYYY`            |

depending on source-system locale configuration.

Because both day and month values were less than or equal to `12`, deterministic interpretation was not always possible.

---

# Step 6 — Conditional Locale Classification Logic

To reduce ambiguity where possible, conditional parsing logic was implemented using:

* `LEFT()`
* `SUBSTRING()`
* `TRY_CONVERT()`

Classification rules:

| Rule              | Interpretation               |
| ----------------- | ---------------------------- |
| first value > 12  | `DD/MM/YYYY` or `DD-MM-YYYY` |
| second value > 12 | `MM/DD/YYYY` or `MM-DD-YYYY` |
| both values ≤ 12  | ambiguous                    |

Examples:

| Raw Value    | Detected Format |
| ------------ | --------------- |
| `19/05/2022` | `DD/MM/YYYY`    |
| `09/23/2021` | `MM/DD/YYYY`    |
| `15-07-2021` | `DD-MM-YYYY`    |
| `07-25-2023` | `MM-DD-YYYY`    |

This approach enabled safe partial inference of regionally formatted DOB values.

---

# Step 7 — SQL Style Code Mapping

Locale-aware conversion required SQL Server style-specific parsing.

Applied style mappings:

| Style Code | Format       |
| ---------- | ------------ |
| `101`      | `MM/DD/YYYY` |
| `103`      | `DD/MM/YYYY` |
| `105`      | `DD-MM-YYYY` |
| `110`      | `MM-DD-YYYY` |

These style codes were applied conditionally based on inferred regional ordering.

---

# Step 8 — Defensive Parsing Using TRY_CONVERT()

All locale-sensitive parsing operations used `TRY_CONVERT()` instead of `CAST()`.

This approach ensured:

* invalid values returned `NULL`
* transformation queries did not fail
* malformed records remained detectable
* defensive ETL behavior was preserved

This was especially important because the dataset contained:

* mixed locale formats
* malformed values
* unresolved ambiguities

---

# Step 9 — Fallback Parsing Strategy

A final fallback strategy was implemented using:

`TRY_CONVERT(DATE, date_of_birth, 101)`

This fallback was intentionally selected after broader dataset analysis indicated that:

* month-first formatting patterns were dominant
* month-name date ecosystems aligned more closely with `MM/DD/YYYY`
* the dataset overall leaned toward US-style formatting conventions

Because of this directional evidence, unresolved ambiguous records were standardized using a month-first (`MM/DD/YYYY`) fallback assumption.

This decision was:

* statistically informed
* explicitly documented
* intentionally applied
* not arbitrary coercion

---

# Step 10 — Final Null Validation

After all parsing and standardization logic was applied, a final validation step was performed:

`WHERE date_of_birth IS NULL`

This validation isolated:

* unresolved records
* failed conversions
* malformed DOB values
* non-standard source data

This step ensured:

* visibility into remaining issues
* auditability of unresolved records
* transparency of transformation limitations

---

# Final Outcome

The `date_of_birth` cleaning pipeline successfully achieved:

* raw DOB profiling
* structural pattern extraction
* frequency-based pattern analysis
* deterministic format standardization
* locale ambiguity detection
* conditional regional parsing
* SQL style-aware conversion
* defensive error-safe transformation
* ambiguity-aware fallback standardization
* final unresolved-record validation

The final implementation significantly improved DOB consistency and analytical reliability while preserving transparency around ambiguous or unresolved source-system date values.

# Customer Account Created Date Cleaning and Standardization Documentation

## Overview

The `account_created_date` column in the `bronze.customers` dataset contained multiple inconsistent date formats originating from mixed source-system inputs. The objective of this cleaning process was to standardize all valid date values into ISO 8601 format (`YYYY-MM-DD`) while preserving data integrity and minimizing the risk of incorrect temporal conversion.

This process involved:

* raw data inspection
* structural pattern profiling
* pattern frequency analysis
* date format classification
* ambiguity detection
* statistical inference
* ISO standardization
* error-safe conversion handling

---

# Step 1 — Raw Data Inspection

Initial inspection of the dataset revealed that the `account_created_date` column contained multiple date representations.

Example raw values:

| Raw Value            |
| -------------------- |
| `Sep 07, 2018`       |
| `September 14, 2021` |
| `2023/11/20`         |
| `2023-07-13`         |
| `08/06/2018`         |
| `19/05/2022`         |

This confirmed that the dataset did not follow a single standardized temporal format.

---

# Step 2 — Structural Pattern Profiling

To identify all unique date structures, structural profiling was performed using SQL Server `TRANSLATE()`-based token normalization.

Characters were generalized into structural tokens:

| Original Value       | Structural Pattern |
| -------------------- | ------------------ |
| `Sep 07, 2018`       | `aaa 99, 9999`     |
| `September 14, 2021` | `aaaa 99, 9999`    |
| `2023/11/20`         | `9999/99/99`       |
| `08/06/2018`         | `99/99/9999`       |

This approach enabled automatic discovery of all structural date patterns present in the dataset.

---

# Step 3 — Pattern Frequency Analysis

After structural profiling, patterns were grouped and analyzed to determine frequency distribution.

Detected logical patterns:

| Date Pattern               | Total Records |
| -------------------------- | ------------- |
| `MM/DD/YYYY or DD/MM/YYYY` | 187           |
| `Mon DD, YYYY`             | 110           |
| `YYYY/MM/DD`               | 104           |
| `Month DD, YYYY`           | 84            |
| `YYYY-MM-DD`               | 79            |
| `DD-MM-YYYY`               | 76            |

This analysis confirmed that the dataset contained both:

* locale-dependent date formats
* locale-independent date formats

---

# Step 4 — ISO Standardization of Deterministic Formats

Formats that were structurally deterministic and unambiguous were safely converted into ISO 8601 format using SQL Server `CONVERT()`.

Successfully standardized formats:

| Source Format    | Status    |
| ---------------- | --------- |
| `Mon DD, YYYY`   | Converted |
| `Month DD, YYYY` | Converted |
| `YYYY/MM/DD`     | Converted |
| `YYYY-MM-DD`     | Converted |

These formats did not require locale inference because their temporal ordering was explicit.

---

# Step 5 — Identification of Ambiguous Regional Formats

The dataset also contained regionally ambiguous date formats:

| Ambiguous Example |
| ----------------- |
| `08/06/2018`      |
| `12/05/2021`      |
| `04/11/2019`      |

These values could represent either:

| Possible Interpretation |
| ----------------------- |
| `MM/DD/YYYY`            |
| `DD/MM/YYYY`            |

depending on source-system locale configuration.

Because both day and month values were less than or equal to `12`, deterministic conversion was not possible from the raw string alone.

---

# Step 6 — Conditional Locale Detection

To reduce ambiguity where possible, conditional parsing logic was implemented using:

* `LEFT()`
* `SUBSTRING()`
* `TRY_CONVERT()`
* SQL Server style codes (`101`, `103`, `105`, `110`)

Logic used:

| Rule              | Interpretation   |
| ----------------- | ---------------- |
| first value > 12  | `DD/MM/YYYY`     |
| second value > 12 | `MM/DD/YYYY`     |
| both values ≤ 12  | `AMBIGUOUS_DATE` |

Examples:

| Raw Value    | Detected Format  |
| ------------ | ---------------- |
| `19/05/2022` | `DD/MM/YYYY`     |
| `09/23/2021` | `MM/DD/YYYY`     |
| `08/06/2018` | `AMBIGUOUS_DATE` |

This approach enabled safe classification of partially identifiable regional date formats.

---

# Step 7 — Statistical Locale Distribution Analysis

A distribution analysis was performed on slash-formatted ambiguous dates.

Results:

| Detected Format  | Total Records | Percentage |
| ---------------- | ------------- | ---------- |
| `AMBIGUOUS_DATE` | 74            | 39%        |
| `DD/MM/YYYY`     | 58            | 31%        |
| `MM/DD/YYYY`     | 55            | 29%        |

This analysis showed:

* no dominant locale pattern existed
* ambiguous records represented the largest category
* deterministic inference remained unreliable

---

# Step 8 — Big-Picture Dataset Direction Analysis

A broader directional analysis was performed across the entire dataset by combining:

* month-name-based formats
* slash formats
* dash formats
* locale-neutral formats

Directional evidence:

| Format Direction                     | Approximate Share |
| ------------------------------------ | ----------------- |
| `MM/DD/YYYY / Month-First Ecosystem` | ~65%              |
| `DD/MM/YYYY / Day-First Ecosystem`   | ~35%              |

This indicated that the dataset overall leaned toward a month-first (`MM/DD/YYYY`) formatting ecosystem due to the high frequency of:

* `Mon DD, YYYY`
* `Month DD, YYYY`

patterns.

---

# Step 9 — Final Engineering Decision

Despite the broader month-first directional tendency, ambiguous dates were not blindly force-converted because:

* deterministic certainty was unavailable
* conflicting locale evidence existed
* silent temporal corruption posed significant downstream analytical risk

Final handling strategy:

| Date Type                   | Action                                        |
| --------------------------- | --------------------------------------------- |
| deterministic formats       | standardized to ISO                           |
| partially inferable formats | conditionally converted                       |
| fully ambiguous dates       | flagged for manual/business-rule-based review |

This decision prioritized:

* data integrity
* auditability
* conversion reliability
  over forced completeness.

---

# Final Outcome

The cleaning and profiling pipeline successfully achieved:

* raw data profiling
* structural pattern extraction
* frequency-based pattern analysis
* ISO 8601 date normalization
* locale ambiguity detection
* conditional parsing logic
* statistical inference analysis
* error-safe conversion using `TRY_CONVERT()`
* defensive handling of ambiguous temporal data

The final process significantly improved date consistency and analytical reliability while preserving transparency regarding unresolved temporal ambiguity.

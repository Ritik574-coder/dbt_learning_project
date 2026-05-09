## State Column Data Quality Analysis

The `state` column contains inconsistent and redundant data.
Some records store state abbreviations (e.g., `CA`, `TX`), while others store full state names (e.g., `California`, `Texas`). Additionally, several records contain misspelled state names such as `Colorao`, `Georga`, and `Tenessee`.

After validation:

* All values in `state_abbr` are correctly standardized.
* `state_full` contains mostly correct full state names with a no any spelling inconsistencies.
* The `state` column does not provide any additional business value because its information is already fully represented by `state_abbr` and `state_full`.

### Recommendation

* Retain:

  * `state_abbr`
  * `state_full`  
* Remove:

  * `state`

Reason:
The `state` column is redundant, inconsistent, and increases storage and maintenance overhead without adding meaningful information.

---

## Customer Account Created Date Cleaning and Standardization

### Objective

The purpose of this process was to standardize the `account_created_date` column into ISO 8601 date format (`YYYY-MM-DD`) while preserving data integrity and handling inconsistent date patterns present in the raw bronze-layer dataset.

---

## Step 1 — Raw Data Inspection

Initial profiling revealed that the `account_created_date` column contained multiple date formats originating from inconsistent source-system formatting.

Raw date examples included:

* `Sep 07, 2018`
* `September 14, 2021`
* `2023/11/20`
* `2023-07-13`
* `08/06/2018`
* `19/05/2022`

This indicated that the dataset contained mixed temporal representations rather than a single standardized format.

---

## Step 2 — Structural Pattern Profiling

To identify all existing date structures, the dataset was profiled using `TRANSLATE()`-based structural pattern extraction.

Digits and alphabetic characters were normalized into generalized structural tokens:

| Raw Value            | Structural Pattern |
| -------------------- | ------------------ |
| `Sep 07, 2018`       | `aaa 99, 9999`     |
| `September 14, 2021` | `aaaa 99, 9999`    |
| `2023/11/20`         | `9999/99/99`       |
| `08/06/2018`         | `99/99/9999`       |

This profiling process helped identify all unique date structures present in the dataset.

---

## Step 3 — Pattern Frequency Analysis

After profiling, date patterns were grouped and counted to understand dataset distribution.

Detected logical date patterns:

| Date Pattern               | Total Records |
| -------------------------- | ------------- |
| `MM/DD/YYYY or DD/MM/YYYY` | 187           |
| `Mon DD, YYYY`             | 110           |
| `YYYY/MM/DD`               | 104           |
| `Month DD, YYYY`           | 84            |
| `YYYY-MM-DD`               | 79            |
| `DD-MM-YYYY`               | 76            |

This analysis confirmed that the dataset contained multiple coexisting temporal formats.

---

## Step 4 — ISO Date Standardization

Dates with deterministic and unambiguous formats were successfully converted into ISO standard format (`YYYY-MM-DD`) using `CONVERT()`.

Successfully standardized formats:

* `Mon DD, YYYY`
* `Month DD, YYYY`
* `YYYY/MM/DD`
* `YYYY-MM-DD`

---

## Step 5 — Mixed Locale Date Handling

The dataset also contained ambiguous regional date formats such as:

* `08/06/2018`
* `12/05/2021`
* `04/11/2019`

These values could represent either:

* `MM/DD/YYYY`
  OR
* `DD/MM/YYYY`

depending on regional locale interpretation.

To address this issue, conditional parsing logic was implemented using:

* `LEFT()`
* `SUBSTRING()`
* `TRY_CONVERT()`
* SQL Server style codes (`101`, `103`, `105`, `110`)

Examples:

| Raw Value    | Detected Format |
| ------------ | --------------- |
| `19/05/2022` | `DD/MM/YYYY`    |
| `09/23/2021` | `MM/DD/YYYY`    |

---

## Step 6 — Ambiguous Date Detection

A statistical analysis was performed to determine dominant regional date formatting.

Results:

| Detected Format  | Total Records | Percentage |
| ---------------- | ------------- | ---------- |
| `AMBIGUOUS_DATE` | 74            | 39.57%     |
| `DD/MM/YYYY`     | 58            | 31.02%     |
| `MM/DD/YYYY`     | 55            | 29.41%     |

The analysis showed that:

* No dominant locale format existed.
* Ambiguous records represented the largest category.
* Reliable deterministic inference was not possible.

---

## Step 7 — Data Integrity Decision

Because both `DD/MM/YYYY` and `MM/DD/YYYY` patterns appeared with nearly equal frequency, automatic inference for ambiguous records was considered unreliable.

To preserve temporal accuracy and avoid silent data corruption:

* Clearly identifiable dates were standardized successfully.
* Ambiguous date values were isolated and flagged for manual/business-rule-based resolution.

This decision prioritized data integrity over forced standardization.

---

## Final Outcome

The cleaning pipeline successfully achieved:

* Structural date profiling
* Pattern frequency analysis
* ISO 8601 date normalization
* Mixed locale detection
* Ambiguous date isolation
* Error-safe conversion handling using `TRY_CONVERT()`

The resulting process significantly improved temporal data quality while maintaining traceability and minimizing the risk of incorrect date interpretation.

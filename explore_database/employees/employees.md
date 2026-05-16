# Employee Data Cleaning and Standardization Documentation

## Overview

The `bronze.employees` dataset contained multiple inconsistencies originating from mixed source-system formatting, manual data entry variations, and incomplete records.

The primary objective of this cleaning process was to:

* inspect raw employee data quality
* identify inconsistent formatting patterns
* standardize core business attributes
* isolate malformed or invalid records
* preserve analytical reliability during transformation
* prepare structured employee data for downstream reporting and analytics

The cleaning process focused on the following attributes:

* employee names
* email addresses
* phone numbers
* job information
* store information
* hire dates
* employment duration
* salary metrics
* employee activity status
* performance ratings
* manager hierarchy

The transformation logic was implemented using:

* `CASE`
* `TRIM()`
* `LOWER()`
* `TRY_CONVERT()`
* `SUBSTRING()`
* `CONCAT()`
* `CHARINDEX()`
* `PARSENAME()`
* `PATINDEX()`

---

# Step 1 — Raw Dataset Inspection

Initial inspection was performed to understand the structure and quality of the raw employee dataset.

The following validations were performed:

* null value analysis
* empty string detection
* structural pattern inspection
* datatype inconsistency checks
* duplicate formatting detection
* malformed record profiling

This inspection confirmed that the dataset contained:

* inconsistent name structures
* malformed email addresses
* multiple phone number formats
* inconsistent date formats
* invalid numeric values
* mixed boolean representations
* inconsistent performance rating formats
* null and incomplete records

---

# Step 2 — Employee Name Parsing

The `full_name` column was analyzed structurally before transformation.

Dataset profiling confirmed that valid employee names followed the structure:

`FirstName LastName`

Name extraction logic was implemented using:

* `TRIM()`
* `REPLACE()`
* `LEN()`
* `PARSENAME()`

The transformation validated names containing exactly one space.

Example:

| Raw Full Name | Extracted First Name | Extracted Last Name |
| --- | --- | --- |
| `John Doe` | `John` | `Doe` |
| `Alice Smith` | `Alice` | `Smith` |

Validation logic:

```sql
LEN(TRIM(full_name)) - LEN(REPLACE(TRIM(full_name), ' ', '')) = 1
```

This validation ensured:

* consistent two-part name structures
* safe positional extraction
* prevention of malformed parsing
* controlled transformation behavior

First name extraction:

```sql
PARSENAME(REPLACE(TRIM(full_name), ' ', '.'), 2)
```

Last name extraction:

```sql
PARSENAME(REPLACE(TRIM(full_name), ' ', '.'), 1)
```

---

# Step 3 — Email Address Cleaning and Normalization

The `email` column contained:

* uppercase and lowercase inconsistencies
* leading and trailing spaces
* malformed email structures
* multiple `@` symbols
* empty values
* null values

Email normalization logic was implemented using:

* `TRIM()`
* `LOWER()`
* `CHARINDEX()`
* `PATINDEX()`
* `LEFT()`
* `SUBSTRING()`
* `REPLACE()`

Examples:

| Raw Email | Standardized Email |
| --- | --- |
| ` John@Gmail.com ` | `john@gmail.com` |
| `alice@@yahoo.com` | `alice@yahoo.com` |
| `BOB@OUTLOOK.COM` | `bob@outlook.com` |

Malformed emails without valid structures were handled using:

`Unknown`

This cleaning process ensured:

* consistent lowercase formatting
* removal of accidental duplicate `@` symbols
* normalized domain structures
* improved analytical consistency

---

# Step 4 — Phone Number Standardization

The `phone` column contained multiple US phone formatting styles.

Detected formats included:

| Pattern Type | Example |
| --- | --- |
| international canonical | `+12125557890` |
| raw numeric | `2125557890` |
| dash-formatted | `212-555-7890` |
| dot-formatted | `212.555.7890` |
| parenthesized | `(212) 555-7890` |

Phone standardization logic was implemented using:

* `CASE`
* `SUBSTRING()`
* `CONCAT()`
* `LIKE`

Final target format:

```text
+1 (AAA) BBB-CCCC
```

Examples:

| Raw Phone | Standardized Phone |
| --- | --- |
| `2125557890` | `+1 (212) 555-7890` |
| `212-555-7890` | `+1 (212) 555-7890` |
| `212.555.7890` | `+1 (212) 555-7890` |
| `(212) 555-7890` | `+1 (212) 555-7890` |
| `+12125557890` | `+1 (212) 555-7890` |

This standardization ensured:

* consistent formatting
* improved readability
* analytical compatibility
* unified presentation standards

---

# Step 5 — Job and Department Cleaning

The following business attributes were standardized:

* `job_title`
* `department`

Validation logic handled:

* null values
* empty strings
* leading and trailing spaces

Invalid or missing values were standardized using:

`Unknown`

Transformation logic:

```sql
CASE
    WHEN job_title IS NULL OR job_title = '' THEN 'Unknown'
    ELSE TRIM(job_title)
END
```

This ensured:

* consistent categorical values
* cleaner reporting dimensions
* improved grouping reliability

---

# Step 6 — Store Information Cleaning

The following store attributes were cleaned:

* `store_id`
* `store_name`
* `store_city`

Validation included:

* numeric conversion checks
* negative value detection
* minimum text-length validation
* null handling

Examples:

| Validation Rule | Handling |
| --- | --- |
| negative store IDs | `NULL` |
| non-numeric store IDs | `NULL` |
| short city names | `Unknown` |
| empty store names | `Unknown` |

This cleaning process ensured:

* valid store identifiers
* readable dimensional attributes
* improved location consistency

---

# Step 7 — Hire Date Standardization

The `hire_date` column contained multiple date structures originating from mixed source systems.

Detected formats included:

| Raw Format | Example |
| --- | --- |
| ISO format | `2024-01-15` |
| slash-separated | `2024/01/15` |
| US formatted | `01/15/2024` |
| European formatted | `15/01/2024` |
| textual month | `January 15, 2024` |
| abbreviated month | `Jan 15, 2024` |

Date parsing logic was implemented using:

* `TRY_CONVERT()`
* `LIKE`
* `LEFT()`
* `SUBSTRING()`

Ambiguous date handling was resolved using conditional month/day validation.

Example:

```sql
WHEN hire_date LIKE '__/__/____'
AND TRY_CONVERT(INT, LEFT(hire_date, 2)) > 12
THEN TRY_CONVERT(DATE, hire_date, 103)
```

This logic ensured:

* accurate date conversion
* safe handling of mixed regional formats
* prevention of invalid conversions
* consistent date standardization

---

# Step 8 — Employment Duration Cleaning

The `years_employed` column was validated using numeric conversion logic.

Validation rules included:

* null detection
* invalid numeric detection
* negative value handling

Transformation logic:

```sql
TRY_CONVERT(DECIMAL(4,2), years_employed)
```

Invalid values were safely converted to:

`NULL`

Examples:

| Raw Value | Standardized Value |
| --- | --- |
| `5` | `5.00` |
| `7.5` | `7.50` |
| `-2` | `NULL` |
| `abc` | `NULL` |

This cleaning ensured:

* numeric consistency
* analytical compatibility
* safe aggregation behavior

---

# Step 9 — Salary and Commission Cleaning

The following financial attributes were standardized:

* `annual_salary_usd`
* `commission_rate_pct`

Validation included:

* decimal conversion checks
* negative value detection
* null handling

Transformation logic:

```sql
TRY_CONVERT(DECIMAL(18,2), annual_salary_usd)
```

and

```sql
TRY_CONVERT(DECIMAL(4,2), commission_rate_pct)
```

Invalid values were safely converted to:

`NULL`

This ensured:

* financial precision consistency
* reliable aggregation behavior
* standardized decimal formatting

---

# Step 10 — Employee Activity Status Standardization

The `is_active` column contained multiple boolean representations.

Detected representations included:

| Raw Value | Standardized Value |
| --- | --- |
| `active` | `True` |
| `yes` | `True` |
| `1` | `True` |
| `true` | `True` |
| `terminated` | `False` |
| `no` | `False` |
| `0` | `False` |
| `false` | `False` |

Standardization logic was implemented using:

* `TRIM()`
* `LOWER()`
* `CASE`

This ensured:

* consistent boolean representation
* reliable filtering behavior
* improved reporting consistency

---

# Step 11 — Performance Rating Standardization

The `performance_rating` column contained multiple grading structures.

Detected rating representations included:

| Raw Value | Standardized Rating |
| --- | --- |
| `A` | `Excellent` |
| `5` | `Excellent` |
| `B` | `Good` |
| `4` | `Good` |
| `C` | `Average` |
| `3` | `Average` |
| `D` | `Below Average` |
| `2` | `Below Average` |

Unknown or empty values were standardized using:

`Unknown`

This cleaning process ensured:

* consistent rating categories
* simplified performance analysis
* standardized reporting dimensions

---

# Step 12 — Manager ID Validation

The `manager_id` column was validated using:

* `TRY_CONVERT(INT, manager_id)`

Validation logic handled:

* non-numeric values
* malformed identifiers
* null values

Invalid manager identifiers were converted to:

`NULL`

This ensured:

* valid hierarchical references
* safe relational joins
* consistent identifier structures

---

# Final Outcome

The employee data cleaning pipeline successfully achieved:

* raw employee dataset inspection
* structural pattern profiling
* controlled employee name parsing
* email normalization and repair
* US phone number standardization
* business attribute normalization
* store information validation
* multi-format date standardization
* employment duration validation
* salary and commission standardization
* boolean activity normalization
* performance rating standardization
* manager hierarchy validation
* safe handling of malformed records

The final transformation significantly improved:

* data consistency
* analytical reliability
* reporting usability
* dimensional standardization
* downstream ETL compatibility
* defensive handling of dirty source-system data

The cleaned employee dataset is now suitable for:

* reporting
* dashboarding
* dimensional modeling
* downstream transformations
* business analytics
* workforce analysis
* operational monitoring
* data warehousing workflows

# Customer Phone Number Cleaning and US Standardization Documentation

## Overview

The `phone` column in the `bronze.customers` dataset contained multiple inconsistent phone number formats originating from mixed source-system formatting and manual user input.

The main objective of this cleaning process was to:

* inspect raw phone number structures
* identify valid US phone number formats
* standardize valid phone numbers into a consistent US display format
* isolate malformed or incomplete records
* preserve data integrity during transformation

The final target standardization format was:

`+1 (AAA) BBB-CCCC`

This standardized format was selected because:

* it is readable and presentation-friendly
* widely recognized in US phone formatting conventions
* consistent across all valid US phone patterns
* suitable for downstream analytics and reporting

---

# Step 1 — Raw Data Inspection

Initial inspection was performed to understand the quality and structure of the raw phone data.

The following validations were performed:

* raw column inspection
* distinct value inspection
* null and empty value analysis

This inspection confirmed that the dataset contained:

* multiple US phone formatting styles
* inconsistent separators
* incomplete phone numbers
* malformed patterns
* null and missing values

---

# Step 2 — Structural Pattern Profiling

Structural pattern profiling was performed to identify all unique phone number structures present in the dataset.

Pattern extraction logic was implemented using:

* `TRIM()`
* `TRANSLATE()`
* `LEN()`
* `GROUP BY`

Digits were normalized into generalized structural placeholders (`9`) while preserving formatting characters.

Examples:

| Raw Value        | Detected Pattern |
| ---------------- | ---------------- |
| `2125557890`     | `9999999999`     |
| `212-555-7890`   | `999-999-9999`   |
| `212.555.7890`   | `999.999.9999`   |
| `(212) 555-7890` | `(999) 999-9999` |
| `+12125557890`   | `+99999999999`   |

This profiling approach enabled complete discovery of all phone formatting structures in the dataset.

---

# Step 3 — Pattern Frequency Analysis

After structural profiling, all phone patterns were grouped and analyzed statistically.

Detected patterns:

| Pattern            | Pattern Length | Record Count  | Percentage |
| ------------------ | -------------- | ------------- | ---------- |
| `+99999999999`     | 12             | 129           | 20%        |
| `9999999999`       | 10             | 127           | 19%        |
| `999-999-9999`     | 12             | 124           | 19%        |
| `999.999.9999`     | 12             | 117           | 18%        |
| `(999) 999-9999`   | 14             | 101           | 15%        |
| `NULL`             | NULL           | 28            | 4%         |
| malformed patterns | various        | low frequency | <1%        |

This analysis showed that the majority of records followed recognizable US phone number structures, although represented using different formatting conventions.

---

# Step 4 — Valid US Phone Pattern Identification

Each valid phone number structure was isolated separately using pattern-specific filtering logic with `LIKE`.

Detected valid structures included:

| Pattern Type            | Example          |
| ----------------------- | ---------------- |
| canonical international | `+12125557890`   |
| raw 10-digit            | `2125557890`     |
| dash-formatted          | `212-555-7890`   |
| dot-formatted           | `212.555.7890`   |
| parenthesized           | `(212) 555-7890` |

Each structure required separate parsing logic because formatting symbols shifted positional indexing.

---

# Step 5 — Pattern-Specific Parsing Logic

Phone number standardization was implemented using:

* `SUBSTRING()`
* `CONCAT()`
* `CASE`
* `TRIM()`

Different patterns required different positional offsets.

Examples:

## Raw 10-Digit Format

Input:
`2125557890`

Standardized Output:
`+1 (212) 555-7890`

---

## Dash-Formatted Pattern

Input:
`212-555-7890`

Standardized Output:
`+1 (212) 555-7890`

---

## Dot-Formatted Pattern

Input:
`212.555.7890`

Standardized Output:
`+1 (212) 555-7890`

---

## Parenthesized Pattern

Input:
`(212) 555-7890`

Standardized Output:
`+1 (212) 555-7890`

---

## International Canonical Pattern

Input:
`+12125557890`

Standardized Output:
`+1 (212) 555-7890`

---

# Step 6 — Index Offset Handling

Different formatting symbols changed substring positions.

Examples:

| Pattern          | Offset Cause                           |
| ---------------- | -------------------------------------- |
| `+12125557890`   | `+1` prefix shifts indexing            |
| `212-555-7890`   | dash separators shift positions        |
| `212.555.7890`   | dot separators shift positions         |
| `(212) 555-7890` | parentheses and spaces shift positions |

Because of this, each phone structure required independent `SUBSTRING()` extraction logic.

---

# Step 7 — Malformed and Invalid Pattern Detection

Several malformed patterns were detected during profiling.

Examples:

| Malformed Pattern | Possible Cause                 |
| ----------------- | ------------------------------ |
| `999999999`       | missing digit                  |
| `+9999999999`     | incomplete country-code format |
| `(99) 999-9999`   | incomplete area code           |
| `99.999.9999`     | malformed grouping             |
| `999-99-9999`     | suspicious non-phone structure |
| `99-999-9999`     | invalid dash grouping          |
| `999.99.9999`     | incomplete dot grouping        |

These patterns most likely originated from:

* manual entry mistakes
* source-system inconsistencies
* incomplete ingestion
* truncation errors
* malformed identifiers

---

# Step 8 — 9-Digit Pattern Validation

Special validation was performed on 9-digit phone structures because standard US public telephone numbers require 10 digits.

Examples:

| Invalid Pattern |
| --------------- |
| `999999999`     |
| `999-99-9999`   |

These values were classified as invalid because:

* they do not follow official US public phone numbering conventions
* reliable reconstruction of missing digits was not possible
* forcing normalization could create incorrect phone data

Some suspicious patterns also resembled:

* SSN-style formatting
* truncated identifiers
* non-phone source-system values

To preserve data integrity, these records were intentionally excluded from standardization.

---

# Step 9 — Final Standardization Pipeline

The final transformation pipeline standardized all valid phone structures into:

`+1 (AAA) BBB-CCCC`

Transformation logic was implemented using:

* `CASE`
* `TRIM()`
* `SUBSTRING()`
* `CONCAT()`

Invalid, malformed, empty, or unresolved patterns were safely handled using:

`Unknown`

This ensured:

* consistent output formatting
* readable standardized values
* defensive transformation behavior
* safe handling of malformed records

---

# Final Outcome

The phone number cleaning pipeline successfully achieved:

* raw phone data inspection
* structural pattern profiling
* pattern frequency analysis
* valid US phone classification
* pattern-aware positional parsing
* canonical US phone standardization
* malformed pattern isolation
* invalid phone number detection
* consistent standardized formatting

The final implementation significantly improved phone number consistency, readability, and downstream analytical usability while preserving safe handling of incomplete or malformed source data.

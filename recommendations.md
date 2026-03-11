# Coty Genie - CapEx Metric Training Recommendations

**Prepared for:** Coty Data Cloud Team
**Date:** March 11, 2026
**Subject:** Best approach to train Genie for Capital Expenditure (CapEx) queries

---

## Table of Contents

1. [Problem Statement](#1-problem-statement)
2. [Current State](#2-current-state)
3. [Options Analysis](#3-options-analysis)
4. [Recommended Solution Architecture](#4-recommended-solution-architecture)
5. [Step-by-Step Implementation](#5-step-by-step-implementation)
   - [Step 1: Create the Base SQL View](#step-1-create-the-base-sql-view)
   - [Step 2: Create the Metric View](#step-2-create-the-metric-view)
   - [Step 3: Configure the Genie Space](#step-3-configure-the-genie-space)
6. [Parameterization (Multi-Year Support)](#6-parameterization-multi-year-support)
7. [Testing & Validation](#7-testing--validation)
8. [Summary](#8-summary)

---

## 1. Problem Statement

The business needs Genie to answer questions like:

- "What was our total CapEx spend last year?"
- "What was CapEx for FY23?"
- "Show me CapEx by fiscal year for the last 3 years"

The underlying CapEx calculation is non-trivial:

> **Total CapEx = (Closing Balance Current FY - Closing Balance Prior FY) - Additions**

Where:

- **Closing Balance** = Sum of accounts S270 & S271 (converted to USD via FX rates)
- **Additions** = Sum of accounts S211 through S221 (converted to USD via FX rates)

The challenge is determining the best way to encode this logic so Genie can reliably produce correct answers.

---

## 2. Current State

The existing query is a single monolithic SQL statement that:

- Joins 3 subqueries (F24 closing balance, F25 closing balance, additions)
- Each subquery joins 3 tables:
  - `cotydatacloud_pro.gold_finance.pl_management`
  - `cotydatacloud_pro.gold_mdm.hfm_source_hierarchy`
  - `cotydatacloud_pro.gold_mdm.exchange_rate`
- Applies currency conversion (local currency -> USD using ZMEA FX rates)
- Hardcodes fiscal years 2024 and 2025
- Returns: `f24`, `f25`, `change_yoy`, `adds`, `total_capex`

**Current result (verified):**

| f24 | f25 | change_yoy | adds | total_capex |
|-----|-----|------------|------|-------------|
| 108,300,130.44 | 86,182,822.88 | -22,117,307.56 | 200,836,548.21 | -222,953,855.77 |

---

## 3. Options Analysis

### Option 1: One Big Query as a Genie Instruction

**Description:** Provide the entire existing SQL query as a sample query / instruction in the Genie space.

**Pros:**
- Simple to implement (copy-paste the query)
- No schema changes needed

**Cons:**
- Hardcoded fiscal years — Genie would need to modify the query for different years, which it may do incorrectly
- Genie may struggle to adapt a complex 3-subquery join for variations
- No reusability — the logic lives only in the Genie instruction
- Maintenance burden — if account codes change, must update the instruction
- Genie sometimes rewrites SQL in unexpected ways for complex queries

> **Verdict: ACCEPTABLE AS FALLBACK**, but not ideal for production use.

---

### Option 2: Three Separate Queries with Genie Instructions

**Description:** Provide three individual queries (closing balance FY-1, closing balance FY, additions) and instruct Genie on how to combine the results.

**Pros:**
- Each query is simpler and easier for Genie to understand
- Modular approach

**Cons:**
- Genie must execute multi-step reasoning (run 3 queries, then calculate)
- High risk of Genie making errors in the combination step
- Genie may not reliably chain the subtraction: `(FY - FY-1) - Additions`
- No guarantee Genie executes all 3 queries consistently
- Users would see intermediate results instead of a clean answer

> **Verdict: NOT RECOMMENDED.** Multi-step reasoning is Genie's weakest area.

---

### Option 3: Single Metric View Measure for the Final Number

**Description:** Pre-compute the full CapEx calculation in a base SQL view, then expose it through a metric view with a simple SUM measure.

**Pros:**
- Genie sees a simple, clean measure — no complex reasoning needed
- Parameterization via dimensions (`fiscal_year`) for multi-year queries
- Business logic is encapsulated and governed in the view layer
- Easy to maintain — update the view when business rules change
- Reusable across Genie, dashboards, and other tools

**Cons:**
- Requires creating a SQL view and metric view (one-time setup)
- View needs to be maintained if source tables change

> **Verdict: RECOMMENDED.** This is the strongest approach.

---

### Option 4: Three Metric View Measures with Genie Instructions

**Description:** Create separate measures for `closing_balance_prior`, `closing_balance_current`, and `additions`, then instruct Genie to calculate `CapEx = (current - prior) - additions`.

**Pros:**
- Users can ask about individual components
- More granular than Option 3

**Cons:**
- Same multi-step reasoning risk as Option 2
- Genie must combine 3 measures correctly — not reliable
- Instructions can be ignored or misinterpreted by Genie

> **Verdict: NOT RECOMMENDED** as the primary approach. However, the individual measures CAN be included alongside Option 3 for users who want to drill into components.

---

### Recommendation: Option 3 (Enhanced)

Use a base SQL view + metric view. Include `total_capex` as the primary measure, but also expose the component measures (`change_yoy`, `total_additions`) as supplementary measures for drill-down. This gives Genie a clean answer for the main question while allowing follow-up questions about components.

---

## 4. Recommended Solution Architecture

```
Layer 1: BASE SQL VIEW (encapsulates all complex logic)
  |
  |  Contains: FX conversion, account filtering, year-over-year self-join
  |  Location: cotydatacloud_pro.gold_finance.vw_capex_summary
  |
  v
Layer 2: METRIC VIEW (clean measures + dimensions)
  |
  |  Contains: total_capex measure, fiscal_year dimension
  |  Location: cotydatacloud_pro.gold_finance.mv_capex
  |
  v
Layer 3: GENIE SPACE (natural language interface)
  |
  |  Points to: the metric view
  |  Users ask: "What was our total CapEx spend last year?"
  |
  v
  Answer: -222,953,855.77 (for FY2025)
```

---

## 5. Step-by-Step Implementation

### Step 1: Create the Base SQL View

This view parameterizes the fiscal year so it works for ANY year, not just 2024/2025. It self-joins the closing balances to pair each year with its prior year.

```sql
-- DROP VIEW IF EXISTS cotydatacloud_pro.gold_finance.vw_capex_summary;

CREATE OR REPLACE VIEW cotydatacloud_pro.gold_finance.vw_capex_summary AS

WITH closing_balances AS (
    -- Closing balances for accounts S270 & S271, per fiscal year
    -- Converted to USD using ZMEA exchange rates
    SELECT
        pl_mgt.fiscal_year,
        SUM(
            pl_mgt.hfm_amount * CASE
                WHEN pl_mgt.local_currency = 'USD' THEN 1
                ELSE fx.exchange_rate
            END
        ) AS closing_balance
    FROM cotydatacloud_pro.gold_finance.pl_management pl_mgt
    JOIN cotydatacloud_pro.gold_mdm.hfm_source_hierarchy srce
        ON pl_mgt.hfm_source = srce.hfm_source_key
    JOIN cotydatacloud_pro.gold_mdm.exchange_rate fx
        ON pl_mgt.local_currency = fx.from_currency
        AND (pl_mgt.fiscal_year_period + CASE
                WHEN pl_mgt.fiscal_year = (
                    SELECT MIN(fiscal_year)
                    FROM cotydatacloud_pro.gold_finance.pl_management
                    WHERE hfm_account IN ('S270', 'S271')
                ) THEN 1000
                ELSE 0
            END) = fx.fiscal_effective_period
        AND fx.to_currency = CASE
                WHEN pl_mgt.local_currency = 'USD' THEN 'EUR'
                ELSE 'USD'
            END
    WHERE srce.s_level3_key = 'REPORTED'
        AND fx.exchange_rate_type = 'ZMEA'
        AND pl_mgt.hfm_scenario = 'ACT'
        AND pl_mgt.hfm_account IN ('S270', 'S271')
    GROUP BY pl_mgt.fiscal_year
),

additions AS (
    -- Additions for accounts S211 through S221, per fiscal year
    -- Converted to USD using ZMEA exchange rates
    SELECT
        pl_mgt.fiscal_year,
        SUM(
            pl_mgt.hfm_amount * CASE
                WHEN pl_mgt.local_currency = 'USD' THEN 1
                ELSE fx.exchange_rate
            END
        ) AS total_additions
    FROM cotydatacloud_pro.gold_finance.pl_management pl_mgt
    JOIN cotydatacloud_pro.gold_mdm.hfm_source_hierarchy srce
        ON pl_mgt.hfm_source = srce.hfm_source_key
    JOIN cotydatacloud_pro.gold_mdm.exchange_rate fx
        ON pl_mgt.local_currency = fx.from_currency
        AND pl_mgt.fiscal_year_period = fx.fiscal_effective_period
        AND fx.to_currency = CASE
                WHEN pl_mgt.local_currency = 'USD' THEN 'EUR'
                ELSE 'USD'
            END
    WHERE srce.s_level3_key = 'REPORTED'
        AND fx.exchange_rate_type = 'ZMEA'
        AND pl_mgt.hfm_scenario = 'ACT'
        AND pl_mgt.hfm_account IN (
            'S211', 'S212', 'S213', 'S214', 'S215',
            'S216', 'S217', 'S218', 'S219', 'S221'
        )
    GROUP BY pl_mgt.fiscal_year
)

SELECT
    cb.fiscal_year,
    cb_prior.closing_balance                                    AS prior_year_closing_balance,
    cb.closing_balance                                          AS current_year_closing_balance,
    (cb.closing_balance - cb_prior.closing_balance)             AS change_yoy,
    a.total_additions,
    (cb.closing_balance - cb_prior.closing_balance)
        - a.total_additions                                     AS total_capex
FROM closing_balances cb
JOIN closing_balances cb_prior
    ON CAST(cb_prior.fiscal_year AS INT) = CAST(cb.fiscal_year AS INT) - 1
JOIN additions a
    ON a.fiscal_year = cb.fiscal_year;
```

> **IMPORTANT NOTE ON THE FX JOIN:**
>
> In the original query, the FX join for fiscal year 2024 uses:
> `(pl_mgt.fiscal_year_period + 1000) = fx.fiscal_effective_period`
>
> But for fiscal year 2025, it uses:
> `pl_mgt.fiscal_year_period = fx.fiscal_effective_period`
>
> This `+1000` offset appears to be specific to FY2024 data. You will need to verify the business rule behind this:
>
> **Q: Does the +1000 offset apply to ALL prior-year closing balances, or only to FY2024 specifically?**
>
> If it applies to all prior-year lookups, then the closing_balances CTE should use a flag or separate logic for prior vs. current year FX joins. The safest approach is the **REVISED version** below.

#### Revised Version (if +1000 applies to ALL prior-year FX lookups)

```sql
CREATE OR REPLACE VIEW cotydatacloud_pro.gold_finance.vw_capex_summary AS

WITH closing_balances_prior AS (
    -- Prior year closing balances WITH the +1000 FX period offset
    SELECT
        CAST(pl_mgt.fiscal_year AS INT) + 1 AS applies_to_fiscal_year,
        SUM(
            pl_mgt.hfm_amount * CASE
                WHEN pl_mgt.local_currency = 'USD' THEN 1
                ELSE fx.exchange_rate
            END
        ) AS prior_year_closing_balance
    FROM cotydatacloud_pro.gold_finance.pl_management pl_mgt
    JOIN cotydatacloud_pro.gold_mdm.hfm_source_hierarchy srce
        ON pl_mgt.hfm_source = srce.hfm_source_key
    JOIN cotydatacloud_pro.gold_mdm.exchange_rate fx
        ON pl_mgt.local_currency = fx.from_currency
        AND (pl_mgt.fiscal_year_period + 1000) = fx.fiscal_effective_period
        AND fx.to_currency = CASE
            WHEN pl_mgt.local_currency = 'USD' THEN 'EUR'
            ELSE 'USD'
        END
    WHERE srce.s_level3_key = 'REPORTED'
        AND fx.exchange_rate_type = 'ZMEA'
        AND pl_mgt.hfm_scenario = 'ACT'
        AND pl_mgt.hfm_account IN ('S270', 'S271')
    GROUP BY pl_mgt.fiscal_year
),

closing_balances_current AS (
    -- Current year closing balances WITHOUT the +1000 FX period offset
    SELECT
        pl_mgt.fiscal_year,
        SUM(
            pl_mgt.hfm_amount * CASE
                WHEN pl_mgt.local_currency = 'USD' THEN 1
                ELSE fx.exchange_rate
            END
        ) AS current_year_closing_balance
    FROM cotydatacloud_pro.gold_finance.pl_management pl_mgt
    JOIN cotydatacloud_pro.gold_mdm.hfm_source_hierarchy srce
        ON pl_mgt.hfm_source = srce.hfm_source_key
    JOIN cotydatacloud_pro.gold_mdm.exchange_rate fx
        ON pl_mgt.local_currency = fx.from_currency
        AND pl_mgt.fiscal_year_period = fx.fiscal_effective_period
        AND fx.to_currency = CASE
            WHEN pl_mgt.local_currency = 'USD' THEN 'EUR'
            ELSE 'USD'
        END
    WHERE srce.s_level3_key = 'REPORTED'
        AND fx.exchange_rate_type = 'ZMEA'
        AND pl_mgt.hfm_scenario = 'ACT'
        AND pl_mgt.hfm_account IN ('S270', 'S271')
    GROUP BY pl_mgt.fiscal_year
),

additions AS (
    -- Additions for accounts S211 through S221, per fiscal year
    SELECT
        pl_mgt.fiscal_year,
        SUM(
            pl_mgt.hfm_amount * CASE
                WHEN pl_mgt.local_currency = 'USD' THEN 1
                ELSE fx.exchange_rate
            END
        ) AS total_additions
    FROM cotydatacloud_pro.gold_finance.pl_management pl_mgt
    JOIN cotydatacloud_pro.gold_mdm.hfm_source_hierarchy srce
        ON pl_mgt.hfm_source = srce.hfm_source_key
    JOIN cotydatacloud_pro.gold_mdm.exchange_rate fx
        ON pl_mgt.local_currency = fx.from_currency
        AND pl_mgt.fiscal_year_period = fx.fiscal_effective_period
        AND fx.to_currency = CASE
            WHEN pl_mgt.local_currency = 'USD' THEN 'EUR'
            ELSE 'USD'
        END
    WHERE srce.s_level3_key = 'REPORTED'
        AND fx.exchange_rate_type = 'ZMEA'
        AND pl_mgt.hfm_scenario = 'ACT'
        AND pl_mgt.hfm_account IN (
            'S211', 'S212', 'S213', 'S214', 'S215',
            'S216', 'S217', 'S218', 'S219', 'S221'
        )
    GROUP BY pl_mgt.fiscal_year
)

SELECT
    cc.fiscal_year,
    cp.prior_year_closing_balance,
    cc.current_year_closing_balance,
    (cc.current_year_closing_balance - cp.prior_year_closing_balance) AS change_yoy,
    a.total_additions,
    (cc.current_year_closing_balance - cp.prior_year_closing_balance)
        - a.total_additions                                           AS total_capex
FROM closing_balances_current cc
JOIN closing_balances_prior cp
    ON CAST(cp.applies_to_fiscal_year AS STRING) = cc.fiscal_year
JOIN additions a
    ON a.fiscal_year = cc.fiscal_year;
```

#### Validation Query

```sql
SELECT * FROM cotydatacloud_pro.gold_finance.vw_capex_summary
WHERE fiscal_year = '2025';
```

**Expected result:**

| fiscal_year | prior_year_closing_balance | current_year_closing_balance | change_yoy | total_additions | total_capex |
|-------------|---------------------------|------------------------------|------------|-----------------|-------------|
| 2025 | 108,300,130.44 | 86,182,822.88 | -22,117,307.56 | 200,836,548.21 | -222,953,855.77 |

---

### Step 2: Create the Metric View

The metric view sits on top of the base SQL view and provides clean, governed measures and dimensions for Genie.

```sql
CREATE OR REPLACE METRIC VIEW cotydatacloud_pro.gold_finance.mv_capex
AS SELECT * FROM cotydatacloud_pro.gold_finance.vw_capex_summary
WITH (
    -- DIMENSIONS (filter/group-by columns)
    fiscal_year DIMENSION
        COMMENT 'The fiscal year for the CapEx calculation (e.g., 2025, 2024)',

    -- PRIMARY MEASURE
    total_capex MEASURE SUM DEFAULT
        COMMENT 'Total Capital Expenditure = (Current Year Closing Balance - Prior Year Closing Balance) - Additions. A negative value indicates net capital spending.',

    -- SUPPLEMENTARY MEASURES (for drill-down questions)
    change_yoy MEASURE SUM
        COMMENT 'Year-over-year change in closing balance for fixed asset accounts S270 and S271, converted to USD',

    total_additions MEASURE SUM
        COMMENT 'Total additions to fixed assets (accounts S211-S221) during the fiscal year, converted to USD',

    prior_year_closing_balance MEASURE SUM
        COMMENT 'Closing balance of fixed asset accounts S270 and S271 from the prior fiscal year, converted to USD',

    current_year_closing_balance MEASURE SUM
        COMMENT 'Closing balance of fixed asset accounts S270 and S271 for the current fiscal year, converted to USD'
);
```

#### Validation

```sql
SELECT * FROM cotydatacloud_pro.gold_finance.mv_capex
WHERE fiscal_year = '2025';
```

Expected: same results as the base view validation above.

---

### Step 3: Configure the Genie Space

1. **Create a new Genie Space** (or add to an existing one)

2. **Add the metric view:**
   - Data source: `cotydatacloud_pro.gold_finance.mv_capex`

3. **Add sample questions** (these help Genie understand intent patterns):
   - "What was our total CapEx spend last year?"
   - "What was CapEx for FY2025?"
   - "Show me CapEx by fiscal year"
   - "What were the fixed asset additions in FY2025?"
   - "How did closing balances change year over year?"
   - "Compare CapEx across FY2024 and FY2025"

4. **Add general instructions** (optional, for additional context):

   > CapEx (Capital Expenditures) represents the net investment in fixed assets. It is calculated as the change in closing balances of accounts S270 and S271 minus additions from accounts S211-S221. All values are converted to USD. The `total_capex` measure is the primary metric. A negative `total_capex` indicates net capital spending (which is normal). When a user asks about "last year" CapEx, use the most recent `fiscal_year` available.

---

## 6. Parameterization (Multi-Year Support)

Because the base view computes CapEx for **all available fiscal years** (not just hardcoded 2024/2025), and `fiscal_year` is a dimension in the metric view, users can naturally ask:

| User Question | Genie Translates To |
|---------------|---------------------|
| "What was CapEx for FY2023?" | `WHERE fiscal_year = '2023'` |
| "Show me CapEx for FY2023 and FY2024" | `WHERE fiscal_year IN ('2023', '2024')` |
| "Show me CapEx trend over the last 3 years" | `GROUP BY fiscal_year` (returns all available years) |

No additional configuration is needed. The dimension handles parameterization automatically.

**Prerequisite:** The source table (`pl_management`) must contain data for the relevant fiscal years. The view will automatically compute CapEx for any year where both the current and prior year closing balances exist.

---

## 7. Testing & Validation

After deploying all three layers, validate end-to-end:

### Test 1: Base View Accuracy

```sql
SELECT * FROM cotydatacloud_pro.gold_finance.vw_capex_summary
WHERE fiscal_year = '2025';
```

Verify: `total_capex = -222,953,855.77` (matches original query)

### Test 2: Metric View Accuracy

```sql
SELECT * FROM cotydatacloud_pro.gold_finance.mv_capex
WHERE fiscal_year = '2025';
```

Verify: same results as Test 1

### Test 3: Multi-Year

```sql
SELECT * FROM cotydatacloud_pro.gold_finance.vw_capex_summary
ORDER BY fiscal_year;
```

Verify: all available fiscal years appear with reasonable values

### Test 4: Genie Natural Language

| Question | Expected Answer |
|----------|-----------------|
| "What was our total CapEx spend in FY2025?" | -222,953,855.77 |
| "Show me CapEx by fiscal year" | Table with `fiscal_year` and `total_capex` columns |
| "What were the fixed asset additions in FY2025?" | 200,836,548.21 |

---

## 8. Summary

### Chosen Approach

**Option 3 (Enhanced)** — Base SQL View + Metric View

### Why This Approach

- Genie sees clean, simple measures — no complex SQL reasoning needed
- Parameterized by fiscal year via dimensions — works for any year
- Business logic is encapsulated and governed in the view layer
- Single source of truth — reusable across Genie, dashboards, reports
- Supplementary measures allow drill-down into components
- Easy to maintain — update the view when business rules change

### Why Not the Other Options

| Option | Reason for Rejection |
|--------|---------------------|
| **Option 1** (big query as instruction) | Hardcoded years, Genie may rewrite complex SQL incorrectly |
| **Option 2** (3 separate queries) | Multi-step reasoning is unreliable in Genie, high error risk |
| **Option 4** (3 separate measures) | Same multi-step risk as Option 2, Genie must combine measures correctly |

### Action Items

- [ ] Clarify the `+1000` FX period offset business rule (see note in Step 1)
- [ ] Create the base SQL view (`vw_capex_summary`)
- [ ] Validate the view output matches the original query result
- [ ] Create the metric view (`mv_capex`)
- [ ] Configure the Genie Space with sample questions
- [ ] Test end-to-end with natural language queries

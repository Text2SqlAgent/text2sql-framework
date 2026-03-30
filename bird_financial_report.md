# BIRD Financial Benchmark — Agent Evaluation Report

**Model:** claude-opus-4-6  
**Database:** BIRD Financial (Czech bank data, 8 tables)  
**Scoring:** Execution-based result comparison (fuzzy: extra columns + rounding tolerated)  

## Summary

| | Correct | Total | Accuracy |
|---|---|---|---|
| Challenging | 3 | 6 | 50% |
| Moderate | 3 | 3 | 100% |
| **Overall** | **6** | **9** | **66%** |

### Observations
- BIRD's challenging questions are intentionally ambiguous — multiple valid SQL interpretations exist
- The financial DB uses cryptic Czech column names (A2–A16) requiring domain knowledge
- The agent self-corrects SQL errors mid-trace, retrying until execution succeeds
- The three errors were due to ambiguous questions and company specific logic, not anything that would be in the LLM's training data. 

---

## Q1 [CHALLENGING] — NO MATCH

**Question:** List out the account numbers of female clients who are oldest and has lowest average salary, calculate the gap between this lowest average salary with the highest average salary?

**Stats:** 19 tool calls · 17 SQL attempts · 5 errors · 69s

### Agent Trace

**Step 1 — `execute_sql`**
```sql
SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'
```
<details><summary>Result</summary>

```
name
----
account
card
client
disp
district
loan
order
trans
(8 rows)
```
</details>

**Step 2 — `lookup_example`** (scenario: _female clients account year_)
<details><summary>Result</summary>

```
Clients link to accounts via the disp bridge table.
Filter disp.type = 'OWNER'. Use strftime('%Y', date) for year filtering.

```sql
SELECT DISTINCT c.client_id, c.gender, a.account_id, a.date AS opened
FROM client c
JOIN disp d ON c.client_id = d.client_id
JOIN account a ON d.account_id = a.account_id
WHERE c.gender = 'F' AND d.type = 'OWNER'
  AND strftime('%Y', a.date) = '1995'
ORDER BY a.date;
```
```
</details>

**Step 3 — `execute_sql`**
```sql
PRAGMA table_info('client')
```
<details><summary>Result</summary>

```
SQL Error: SQLite objects created in a thread can only be used in that same thread. The object was created in thread id 140704471597312 and this is thread id 123145626857472.
```
</details>

**Step 4 — `execute_sql`**
```sql
PRAGMA table_info('district')
```
<details><summary>Result</summary>

```
SQL Error: SQLite objects created in a thread can only be used in that same thread. The object was created in thread id 140704471597312 and this is thread id 123145643646976.
```
</details>

**Step 5 — `execute_sql`**
```sql
PRAGMA table_info('disp')
```
<details><summary>Result</summary>

```
SQL Error: SQLite objects created in a thread can only be used in that same thread. The object was created in thread id 140704471597312 and this is thread id 123145660436480.
```
</details>

**Step 6 — `execute_sql`**
```sql
PRAGMA table_info('client')
```
<details><summary>Result</summary>

```
SQL Error: SQLite objects created in a thread can only be used in that same thread. The object was created in thread id 140704471597312 and this is thread id 123145626857472.
```
</details>

**Step 7 — `execute_sql`**
```sql
PRAGMA table_info('client')
```
<details><summary>Result</summary>

```
SQL Error: SQLite objects created in a thread can only be used in that same thread. The object was created in thread id 140704471597312 and this is thread id 123145626857472.
```
</details>

**Step 8 — `execute_sql`**
```sql
SELECT * FROM client LIMIT 3
```
<details><summary>Result</summary>

```
client_id | gender | birth_date | district_id
---------------------------------------------
1 | F | 1970-12-13 | 18
2 | M | 1945-02-04 | 1
3 | F | 1940-10-09 | 1
(3 rows)
```
</details>

**Step 9 — `execute_sql`**
```sql
SELECT * FROM district LIMIT 3
```
<details><summary>Result</summary>

```
district_id | A2 | A3 | A4 | A5 | A6 | A7 | A8 | A9 | A10 | A11 | A12 | A13 | A14 | A15 | A16
---------------------------------------------------------------------------------------------
1 | Hl.m. Praha | Prague | 1204953 | 0 | 0 | 0 | 1 | 1 | 100.0 | 12541 | 0.2 | 0.43 | 167 | 85677 | 99107
2 | Benesov | central Bohemia | 88884 | 80 | 26 | 6 | 2 | 5 | 46.7 | 8507 | 1.6 | 1.85 | 132 | 2159 | 2674
3 | Beroun | central Bohemia | 75232 | 55 | 26 | 4 | 1 | 5 | 41.7 | 8980 | 1.9 | 2.21 | 111 | 2824 ...
```
</details>

**Step 10 — `execute_sql`**
```sql
SELECT * FROM disp LIMIT 3
```
<details><summary>Result</summary>

```
disp_id | client_id | account_id | type
---------------------------------------
1 | 1 | 1 | OWNER
2 | 2 | 2 | OWNER
3 | 3 | 2 | DISPONENT
(3 rows)
```
</details>

**Step 11 — `execute_sql`**
```sql
SELECT * FROM account LIMIT 3
```
<details><summary>Result</summary>

```
account_id | district_id | frequency | date
-------------------------------------------
1 | 18 | POPLATEK MESICNE | 1995-03-24
2 | 1 | POPLATEK MESICNE | 1993-02-26
3 | 5 | POPLATEK MESICNE | 1997-07-07
(3 rows)
```
</details>

**Step 12 — `lookup_example`** (scenario: _district column reference_)
<details><summary>Result</summary>

```
The district table uses cryptic A2–A16 column names:
- A2 = district name, A3 = region, A4 = population (TEXT — CAST to INTEGER)
- A11 = average salary, A12 = unemployment 1995, A13 = unemployment 1996
- A14 = entrepreneurs per 1000, A15 = crimes 1995, A16 = crimes 1996

```sql
-- District with most inhabitants in a region
SELECT A2, CAST(A4 AS INTEGER) AS population
FROM district WHERE A3 = 'south Bohemia'
ORDER BY CAST(A4 AS INTEGER) DESC LIMIT 1;
```
```
</details>

**Step 13 — `execute_sql`**
```sql
-- First, find the oldest female clients (minimum birth_date)
SELECT c.client_id, c.birth_date, c.district_id, d.A11 AS avg_salary
FROM client c
JOIN district d ON c.district_id = d.district_id
WHERE c.gender = 'F'
ORDER BY c.birth_date ASC
LIMIT 10
```
<details><summary>Result</summary>

```
client_id | birth_date | district_id | avg_salary
-------------------------------------------------
2115 | 1914-03-01 | 51 | 8541
4486 | 1914-06-14 | 36 | 9198
1704 | 1914-10-03 | 50 | 8187
1494 | 1915-03-22 | 1 | 12541
1975 | 1915-09-27 | 10 | 10124
4630 | 1915-09-27 | 76 | 8369
852 | 1916-06-04 | 25 | 8554
3888 | 1916-10-27 | 67 | 8110
3653 | 1917-01-03 | 1 | 12541
2337 | 1917-04-09 | 12 | 8754
(10 rows)
```
</details>

**Step 14 — `execute_sql`**
```sql
-- Find the minimum birth_date among female clients
SELECT MIN(birth_date) AS oldest_birth_date FROM client WHERE gender = 'F'
```
<details><summary>Result</summary>

```
oldest_birth_date
-----------------
1914-03-01
(1 rows)
```
</details>

**Step 15 — `execute_sql`**
```sql
-- Find oldest female clients (born on the earliest date)
SELECT c.client_id, c.birth_date, c.district_id, d.A11 AS avg_salary
FROM client c
JOIN district d ON c.district_id = d.district_id
WHERE c.gender = 'F'
  AND c.birth_date = (SELECT MIN(birth_date) FROM client WHERE gender = 'F')
```
<details><summary>Result</summary>

```
client_id | birth_date | district_id | avg_salary
-------------------------------------------------
2115 | 1914-03-01 | 51 | 8541
(1 rows)
```
</details>

**Step 16 — `execute_sql`**
```sql
WITH oldest_female AS (
    SELECT c.client_id, c.birth_date, c.district_id, CAST(d.A11 AS INTEGER) AS avg_salary
    FROM client c
    JOIN district d ON c.district_id = d.district_id
    WHERE c.gender = 'F'
      AND c.birth_date = (SELECT MIN(birth_date) FROM client WHERE gender = 'F')
),
salary_range AS (
    SELECT MIN(CAST(A11 AS INTEGER)) AS min_avg_salary, MAX(CAST(A11 AS INTEGER)) AS max_avg_salary
    FROM district
)
SELECT 
    a.account_id AS account_number,
    of.client_id,
    of.birth_date,
    of.avg_salary AS lowest_avg_salary_of_oldest,
    sr.max_avg_salary AS highest_avg_salary,
    sr.max_avg_salary - of.avg_salary AS salary_gap
FROM oldest_female of
JOIN disp dp ON of.client_id = dp.client_id AND dp.type = 'OWNER'
JOIN account a ON dp.account_id = a.account_id
CROSS JOIN salary_range sr
```
<details><summary>Result</summary>

```
Query executed successfully. 0 rows returned.
```
</details>

**Step 17 — `execute_sql`**
```sql
SELECT * FROM disp WHERE client_id = 2115
```
<details><summary>Result</summary>

```
disp_id | client_id | account_id | type
---------------------------------------
2115 | 2115 | 1743 | DISPONENT
(1 rows)
```
</details>

**Step 18 — `execute_sql`**
```sql
-- Find female clients born in the earliest year
SELECT c.client_id, c.birth_date, c.district_id, d.A11 AS avg_salary, dp.type, dp.account_id
FROM client c
JOIN district d ON c.district_id = d.district_id
LEFT JOIN disp dp ON c.client_id = dp.client_id
WHERE c.gender = 'F'
  AND strftime('%Y', c.birth_date) = (
    SELECT strftime('%Y', MIN(birth_date)) FROM client WHERE gender = 'F'
  )
ORDER BY d.A11 ASC
```
<details><summary>Result</summary>

```
client_id | birth_date | district_id | avg_salary | type | account_id
---------------------------------------------------------------------
1704 | 1914-10-03 | 50 | 8187 | DISPONENT | 1406
2115 | 1914-03-01 | 51 | 8541 | DISPONENT | 1743
4486 | 1914-06-14 | 36 | 9198 | DISPONENT | 3720
(3 rows)
```
</details>

**Step 19 — `execute_sql`**
```sql
WITH oldest_females AS (
    SELECT c.client_id, c.birth_date, c.district_id, CAST(d.A11 AS INTEGER) AS avg_salary
    FROM client c
    JOIN district d ON c.district_id = d.district_id
    WHERE c.gender = 'F'
      AND strftime('%Y', c.birth_date) = (
        SELECT strftime('%Y', MIN(birth_date)) FROM client WHERE gender = 'F'
      )
),
min_salary_oldest AS (
    SELECT MIN(avg_salary) AS min_avg_salary
    FROM oldest_females
),
salary_range AS (
    SELECT MAX(CAST(A11 AS INTEGER)) AS max_avg_salary
    FROM district
)
SELECT 
    dp.account_id AS account_number,
    of.client_id,
    of.birth_date,
    of.avg_salary AS lowest_avg_salary,
    sr.max_avg_salary AS highest_avg_salary,
    sr.max_avg_salary - of.avg_salary AS salary_gap
FROM oldest_females of
JOIN disp dp ON of.client_id = dp.client_id
CROSS JOIN salary_range sr
CROSS JOIN min_salary_oldest ms
WHERE of.avg_salary = ms.min_avg_salary
```
<details><summary>Result</summary>

```
account_number | client_id | birth_date | lowest_avg_salary | highest_avg_salary | salary_gap
---------------------------------------------------------------------------------------------
1406 | 1704 | 1914-10-03 | 8187 | 12541 | 4354
(1 rows)
```
</details>

### Agent Final SQL
```sql
WITH oldest_females AS (
    SELECT c.client_id, c.birth_date, c.district_id, CAST(d.A11 AS INTEGER) AS avg_salary
    FROM client c
    JOIN district d ON c.district_id = d.district_id
    WHERE c.gender = 'F'
      AND strftime('%Y', c.birth_date) = (
        SELECT strftime('%Y', MIN(birth_date)) FROM client WHERE gender = 'F'
      )
),
min_salary_oldest AS (
    SELECT MIN(avg_salary) AS min_avg_salary
    FROM oldest_females
),
salary_range AS (
    SELECT MAX(CAST(A11 AS INTEGER)) AS max_avg_salary
    FROM district
)
SELECT 
    dp.account_id AS account_number,
    of.client_id,
    of.birth_date,
    of.avg_salary AS lowest_avg_salary,
    sr.max_avg_salary AS highest_avg_salary,
    sr.max_avg_salary - of.avg_salary AS salary_gap
FROM oldest_females of
JOIN disp dp ON of.client_id = dp.client_id
CROSS JOIN salary_range sr
CROSS JOIN min_salary_oldest ms
WHERE of.avg_salary = ms.min_avg_salary;
```

**Agent Result:**
| account_number | client_id | birth_date | lowest_avg_salary | highest_avg_salary | salary_gap |
| --- | --- | --- | --- | --- | --- |
| 1406 | 1704 | 1914-10-03 | 8187 | 12541 | 4354 |

### Gold SQL
```sql
SELECT T1.account_id , ( SELECT MAX(A11) - MIN(A11) FROM district ) FROM account AS T1 INNER JOIN district AS T2 ON T1.district_id = T2.district_id INNER JOIN disp AS T3 ON T1.account_id = T3.account_id INNER JOIN client AS T4 ON T3.client_id = T4.client_id WHERE T2.district_id = ( SELECT district_id FROM client WHERE gender = 'F' ORDER BY birth_date ASC LIMIT 1 ) ORDER BY T2.A11 DESC LIMIT 1
```

**Gold Result:**
| account_id | ( SELECT MAX(A11) - MIN(A11) FROM district ) |
| --- | --- |
| 6 | 4431 |

---

## Q2 [CHALLENGING] — NO MATCH

**Question:** For the branch which located in the south Bohemia with biggest number of inhabitants, what is the percentage of the male clients?

**Stats:** 10 tool calls · 9 SQL attempts · 4 errors · 29s

### Agent Trace

**Step 1 — `execute_sql`**
```sql
SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'
```
<details><summary>Result</summary>

```
name
----
account
card
client
disp
district
loan
order
trans
(8 rows)
```
</details>

**Step 2 — `lookup_example`** (scenario: _district column reference_)
<details><summary>Result</summary>

```
The district table uses cryptic A2–A16 column names:
- A2 = district name, A3 = region, A4 = population (TEXT — CAST to INTEGER)
- A11 = average salary, A12 = unemployment 1995, A13 = unemployment 1996
- A14 = entrepreneurs per 1000, A15 = crimes 1995, A16 = crimes 1996

```sql
-- District with most inhabitants in a region
SELECT A2, CAST(A4 AS INTEGER) AS population
FROM district WHERE A3 = 'south Bohemia'
ORDER BY CAST(A4 AS INTEGER) DESC LIMIT 1;
```
```
</details>

**Step 3 — `execute_sql`**
```sql
SELECT A1, A2, A3, CAST(A4 AS INTEGER) AS population
FROM district WHERE A3 = 'south Bohemia'
ORDER BY CAST(A4 AS INTEGER) DESC LIMIT 5
```
<details><summary>Result</summary>

```
SQL Error: (sqlite3.OperationalError) no such column: A1
[SQL: SELECT A1, A2, A3, CAST(A4 AS INTEGER) AS population
FROM district WHERE A3 = 'south Bohemia'
ORDER BY CAST(A4 AS INTEGER) DESC LIMIT 5]
(Background on this error at: https://sqlalche.me/e/20/e3q8)
```
</details>

**Step 4 — `execute_sql`**
```sql
PRAGMA table_info('client')
```
<details><summary>Result</summary>

```
SQL Error: SQLite objects created in a thread can only be used in that same thread. The object was created in thread id 140704471597312 and this is thread id 123145626320896.
```
</details>

**Step 5 — `execute_sql`**
```sql
PRAGMA table_info('district')
```
<details><summary>Result</summary>

```
SQL Error: SQLite objects created in a thread can only be used in that same thread. The object was created in thread id 140704471597312 and this is thread id 123145609531392.
```
</details>

**Step 6 — `execute_sql`**
```sql
PRAGMA table_info('district')
```
<details><summary>Result</summary>

```
SQL Error: SQLite objects created in a thread can only be used in that same thread. The object was created in thread id 140704471597312 and this is thread id 123145609531392.
```
</details>

**Step 7 — `execute_sql`**
```sql
SELECT * FROM district LIMIT 2
```
<details><summary>Result</summary>

```
district_id | A2 | A3 | A4 | A5 | A6 | A7 | A8 | A9 | A10 | A11 | A12 | A13 | A14 | A15 | A16
---------------------------------------------------------------------------------------------
1 | Hl.m. Praha | Prague | 1204953 | 0 | 0 | 0 | 1 | 1 | 100.0 | 12541 | 0.2 | 0.43 | 167 | 85677 | 99107
2 | Benesov | central Bohemia | 88884 | 80 | 26 | 6 | 2 | 5 | 46.7 | 8507 | 1.6 | 1.85 | 132 | 2159 | 2674
(2 rows)
```
</details>

**Step 8 — `execute_sql`**
```sql
SELECT district_id, A2, A3, CAST(A4 AS INTEGER) AS population
FROM district WHERE A3 = 'south Bohemia'
ORDER BY CAST(A4 AS INTEGER) DESC LIMIT 5
```
<details><summary>Result</summary>

```
district_id | A2 | A3 | population
----------------------------------
14 | Ceske Budejovice | south Bohemia | 177686
21 | Tabor | south Bohemia | 103347
16 | Jindrichuv Hradec | south Bohemia | 93931
17 | Pelhrimov | south Bohemia | 74062
18 | Pisek | south Bohemia | 70699
(5 rows)
```
</details>

**Step 9 — `execute_sql`**
```sql
SELECT * FROM client LIMIT 2
```
<details><summary>Result</summary>

```
client_id | gender | birth_date | district_id
---------------------------------------------
1 | F | 1970-12-13 | 18
2 | M | 1945-02-04 | 1
(2 rows)
```
</details>

**Step 10 — `execute_sql`**
```sql
SELECT 
    COUNT(*) AS total_clients,
    SUM(CASE WHEN gender = 'M' THEN 1 ELSE 0 END) AS male_clients,
    ROUND(100.0 * SUM(CASE WHEN gender = 'M' THEN 1 ELSE 0 END) / COUNT(*), 2) AS male_percentage
FROM client
WHERE district_id = 14
```
<details><summary>Result</summary>

```
total_clients | male_clients | male_percentage
----------------------------------------------
45 | 18 | 40.0
(1 rows)
```
</details>

### Agent Final SQL
```sql
SELECT 
    ROUND(100.0 * SUM(CASE WHEN gender = 'M' THEN 1 ELSE 0 END) / COUNT(*), 2) AS male_percentage
FROM client
WHERE district_id = (
    SELECT district_id
    FROM district
    WHERE A3 = 'south Bohemia'
    ORDER BY CAST(A4 AS INTEGER) DESC
    LIMIT 1
)
```

**Agent Result:**
| male_percentage |
| --- |
| 40.0 |

### Gold SQL
```sql
SELECT CAST(SUM(T1.gender = 'M') AS REAL) * 100 / COUNT(T1.client_id) FROM client AS T1 INNER JOIN district AS T2 ON T1.district_id = T2.district_id WHERE T2.A3 = 'south Bohemia' GROUP BY T2.A4 ORDER BY T2.A4 DESC LIMIT 1
```

**Gold Result:**
| CAST(SUM(T1.gender = 'M') AS REAL) * 100 / COUNT(T1.client_id) |
| --- |
| 44.26229508196721 |

---

## Q3 [CHALLENGING] — MATCH

**Question:** For the client whose loan was approved first in 1993/7/5, what is the increase rate of his/her account balance from 1993/3/22 to 1998/12/27?

**Stats:** 13 tool calls · 13 SQL attempts · 5 errors · 37s

### Agent Trace

**Step 1 — `execute_sql`**
```sql
SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'
```
<details><summary>Result</summary>

```
name
----
account
card
client
disp
district
loan
order
trans
(8 rows)
```
</details>

**Step 2 — `execute_sql`**
```sql
PRAGMA table_info('loan')
```
<details><summary>Result</summary>

```
SQL Error: SQLite objects created in a thread can only be used in that same thread. The object was created in thread id 140704471597312 and this is thread id 123145592741888.
```
</details>

**Step 3 — `execute_sql`**
```sql
PRAGMA table_info('trans')
```
<details><summary>Result</summary>

```
SQL Error: SQLite objects created in a thread can only be used in that same thread. The object was created in thread id 140704471597312 and this is thread id 123145592741888.
```
</details>

**Step 4 — `execute_sql`**
```sql
PRAGMA table_info('account')
```
<details><summary>Result</summary>

```
SQL Error: SQLite objects created in a thread can only be used in that same thread. The object was created in thread id 140704471597312 and this is thread id 123145626320896.
```
</details>

**Step 5 — `execute_sql`**
```sql
PRAGMA table_info('loan')
```
<details><summary>Result</summary>

```
SQL Error: SQLite objects created in a thread can only be used in that same thread. The object was created in thread id 140704471597312 and this is thread id 123145592741888.
```
</details>

**Step 6 — `execute_sql`**
```sql
PRAGMA table_info('loan')
```
<details><summary>Result</summary>

```
SQL Error: SQLite objects created in a thread can only be used in that same thread. The object was created in thread id 140704471597312 and this is thread id 123145592741888.
```
</details>

**Step 7 — `execute_sql`**
```sql
SELECT sql FROM sqlite_master WHERE type='table' AND name='loan'
```
<details><summary>Result</summary>

```
sql
---
CREATE TABLE loan
(
    loan_id    INTEGER default 0 not null
        primary key,
    account_id INTEGER           not null,
    date       DATE          not null,
    amount     INTEGER           not null,
    duration   INTEGER           not null,
    payments   REAL not null,
    status     TEXT    not null,
    foreign key (account_id) references account (account_id)
)
(1 rows)
```
</details>

**Step 8 — `execute_sql`**
```sql
SELECT sql FROM sqlite_master WHERE type='table' AND name='trans'
```
<details><summary>Result</summary>

```
sql
---
CREATE TABLE trans
(
    trans_id   INTEGER default 0    not null
        primary key,
    account_id INTEGER default 0    not null,
    date       DATE             not null,
    type       TEXT       not null,
    operation  TEXT      null,
    amount     INTEGER              not null,
    balance    INTEGER             not null,
    k_symbol   TEXT      null,
    bank       TEXT       null,
    account    INTEGER          null,
    foreign key (account_id) references account (account_i...
```
</details>

**Step 9 — `execute_sql`**
```sql
SELECT sql FROM sqlite_master WHERE type='table' AND name='disp'
```
<details><summary>Result</summary>

```
sql
---
CREATE TABLE disp
(
    disp_id    INTEGER        not null
        primary key,
    client_id  INTEGER        not null,
    account_id INTEGER        not null,
    type      TEXT not null,
    foreign key (account_id) references account (account_id),
    foreign key (client_id) references client (client_id)
)
(1 rows)
```
</details>

**Step 10 — `execute_sql`**
```sql
SELECT l.loan_id, l.account_id, l.date, l.amount
FROM loan l
WHERE l.date = '1993-07-05'
ORDER BY l.loan_id ASC
LIMIT 5
```
<details><summary>Result</summary>

```
loan_id | account_id | date | amount
------------------------------------
5314 | 1787 | 1993-07-05 | 96396
(1 rows)
```
</details>

**Step 11 — `execute_sql`**
```sql
SELECT t.date, t.balance, t.trans_id
FROM trans t
WHERE t.account_id = 1787 AND t.date = '1993-03-22'
ORDER BY t.trans_id DESC
LIMIT 5
```
<details><summary>Result</summary>

```
date | balance | trans_id
-------------------------
1993-03-22 | 1100 | 523621
(1 rows)
```
</details>

**Step 12 — `execute_sql`**
```sql
SELECT t.date, t.balance, t.trans_id
FROM trans t
WHERE t.account_id = 1787 AND t.date = '1998-12-27'
ORDER BY t.trans_id DESC
LIMIT 5
```
<details><summary>Result</summary>

```
date | balance | trans_id
-------------------------
1998-12-27 | 5835 | 523772
(1 rows)
```
</details>

**Step 13 — `execute_sql`**
```sql
WITH first_loan_account AS (
    SELECT l.account_id
    FROM loan l
    WHERE l.date = '1993-07-05'
    ORDER BY l.loan_id ASC
    LIMIT 1
),
balance_start AS (
    SELECT t.balance
    FROM trans t
    JOIN first_loan_account fla ON t.account_id = fla.account_id
    WHERE t.date = '1993-03-22'
    ORDER BY t.trans_id DESC
    LIMIT 1
),
balance_end AS (
    SELECT t.balance
    FROM trans t
    JOIN first_loan_account fla ON t.account_id = fla.account_id
    WHERE t.date = '1998-12-27'
    ORDER BY t.trans_id DESC
    LIMIT 1
)
SELECT
    bs.balance AS balance_1993_03_22,
    be.balance AS balance_1998_12_27,
    ROUND((be.balance - bs.balance) * 1.0 / bs.balance * 100, 2) AS increase_rate_percent
FROM balance_start bs, balance_end be
```
<details><summary>Result</summary>

```
balance_1993_03_22 | balance_1998_12_27 | increase_rate_percent
---------------------------------------------------------------
1100 | 5835 | 430.45
(1 rows)
```
</details>

### Agent Final SQL
```sql
WITH first_loan_account AS (
    SELECT l.account_id
    FROM loan l
    WHERE l.date = '1993-07-05'
    ORDER BY l.loan_id ASC
    LIMIT 1
),
balance_start AS (
    SELECT t.balance
    FROM trans t
    JOIN first_loan_account fla ON t.account_id = fla.account_id
    WHERE t.date = '1993-03-22'
    ORDER BY t.trans_id DESC
    LIMIT 1
),
balance_end AS (
    SELECT t.balance
    FROM trans t
    JOIN first_loan_account fla ON t.account_id = fla.account_id
    WHERE t.date = '1998-12-27'
    ORDER BY t.trans_id DESC
    LIMIT 1
)
SELECT
    bs.balance AS balance_1993_03_22,
    be.balance AS balance_1998_12_27,
    ROUND((be.balance - bs.balance) * 1.0 / bs.balance * 100, 2) AS increase_rate_percent
FROM balance_start bs, balance_end be;
```

**Agent Result:**
| balance_1993_03_22 | balance_1998_12_27 | increase_rate_percent |
| --- | --- | --- |
| 1100 | 5835 | 430.45 |

### Gold SQL
```sql
SELECT CAST((SUM(IIF(T3.date = '1998-12-27', T3.balance, 0)) - SUM(IIF(T3.date = '1993-03-22', T3.balance, 0))) AS REAL) * 100 / SUM(IIF(T3.date = '1993-03-22', T3.balance, 0)) FROM loan AS T1 INNER JOIN account AS T2 ON T1.account_id = T2.account_id INNER JOIN trans AS T3 ON T3.account_id = T2.account_id WHERE T1.date = '1993-07-05'
```

**Gold Result:**
| CAST((SUM(IIF(T3.date = '1998-12-27', T3.balance, 0)) - SUM(IIF(T3.date = '1993-03-22', T3.balance, 0))) AS REAL) * 100 / SUM(IIF(T3.date = '1993-03-22', T3.balance, 0)) |
| --- |
| 430.45454545454544 |

---

## Q4 [CHALLENGING] — NO MATCH

**Question:** Please list the account types that are not eligible for loans, and the average income of residents in the district where the account is located exceeds $8000 but is no more than $9000.

**Stats:** 14 tool calls · 13 SQL attempts · 6 errors · 39s

### Agent Trace

**Step 1 — `execute_sql`**
```sql
SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'
```
<details><summary>Result</summary>

```
name
----
account
card
client
disp
district
loan
order
trans
(8 rows)
```
</details>

**Step 2 — `execute_sql`**
```sql
PRAGMA table_info('account')
```
<details><summary>Result</summary>

```
SQL Error: SQLite objects created in a thread can only be used in that same thread. The object was created in thread id 140704471597312 and this is thread id 123145626320896.
```
</details>

**Step 3 — `execute_sql`**
```sql
PRAGMA table_info('loan')
```
<details><summary>Result</summary>

```
SQL Error: SQLite objects created in a thread can only be used in that same thread. The object was created in thread id 140704471597312 and this is thread id 123145609531392.
```
</details>

**Step 4 — `execute_sql`**
```sql
PRAGMA table_info('district')
```
<details><summary>Result</summary>

```
SQL Error: SQLite objects created in a thread can only be used in that same thread. The object was created in thread id 140704471597312 and this is thread id 123145626320896.
```
</details>

**Step 5 — `execute_sql`**
```sql
PRAGMA table_info('disp')
```
<details><summary>Result</summary>

```
SQL Error: SQLite objects created in a thread can only be used in that same thread. The object was created in thread id 140704471597312 and this is thread id 123145643110400.
```
</details>

**Step 6 — `execute_sql`**
```sql
PRAGMA table_info('account')
```
<details><summary>Result</summary>

```
SQL Error: SQLite objects created in a thread can only be used in that same thread. The object was created in thread id 140704471597312 and this is thread id 123145626320896.
```
</details>

**Step 7 — `execute_sql`**
```sql
PRAGMA table_info('account')
```
<details><summary>Result</summary>

```
SQL Error: SQLite objects created in a thread can only be used in that same thread. The object was created in thread id 140704471597312 and this is thread id 123145626320896.
```
</details>

**Step 8 — `execute_sql`**
```sql
SELECT sql FROM sqlite_master WHERE type='table' AND name='account'
```
<details><summary>Result</summary>

```
sql
---
CREATE TABLE account
(
    account_id  INTEGER default 0 not null
        primary key,
    district_id INTEGER default 0 not null,
    frequency   TEXT   not null,
    date        DATE          not null,
    foreign key (district_id) references district (district_id)
)
(1 rows)
```
</details>

**Step 9 — `execute_sql`**
```sql
SELECT sql FROM sqlite_master WHERE type='table' AND name='loan'
```
<details><summary>Result</summary>

```
sql
---
CREATE TABLE loan
(
    loan_id    INTEGER default 0 not null
        primary key,
    account_id INTEGER           not null,
    date       DATE          not null,
    amount     INTEGER           not null,
    duration   INTEGER           not null,
    payments   REAL not null,
    status     TEXT    not null,
    foreign key (account_id) references account (account_id)
)
(1 rows)
```
</details>

**Step 10 — `execute_sql`**
```sql
SELECT sql FROM sqlite_master WHERE type='table' AND name='district'
```
<details><summary>Result</summary>

```
sql
---
CREATE TABLE district
(
    district_id INTEGER default 0 not null
        primary key,
    A2          TEXT   not null,
    A3          TEXT   not null,
    A4          TEXT       not null,
    A5          TEXT           not null,
    A6          TEXT           not null,
    A7          TEXT           not null,
    A8          INTEGER        not null,
    A9          INTEGER           not null,
    A10         REAL not null,
    A11         INTEGER           not null,
    A12         RE...
```
</details>

**Step 11 — `lookup_example`** (scenario: _district column reference_)
<details><summary>Result</summary>

```
The district table uses cryptic A2–A16 column names:
- A2 = district name, A3 = region, A4 = population (TEXT — CAST to INTEGER)
- A11 = average salary, A12 = unemployment 1995, A13 = unemployment 1996
- A14 = entrepreneurs per 1000, A15 = crimes 1995, A16 = crimes 1996

```sql
-- District with most inhabitants in a region
SELECT A2, CAST(A4 AS INTEGER) AS population
FROM district WHERE A3 = 'south Bohemia'
ORDER BY CAST(A4 AS INTEGER) DESC LIMIT 1;
```
```
</details>

**Step 12 — `execute_sql`**
```sql
SELECT DISTINCT frequency FROM account
```
<details><summary>Result</summary>

```
frequency
---------
POPLATEK MESICNE
POPLATEK TYDNE
POPLATEK PO OBRATU
(3 rows)
```
</details>

**Step 13 — `execute_sql`**
```sql
SELECT DISTINCT a.frequency AS account_type
FROM account a
JOIN district d ON a.district_id = d.district_id
WHERE a.account_id NOT IN (SELECT account_id FROM loan)
  AND d.A11 > 8000
  AND d.A11 <= 9000
```
<details><summary>Result</summary>

```
account_type
------------
POPLATEK MESICNE
POPLATEK TYDNE
POPLATEK PO OBRATU
(3 rows)
```
</details>

**Step 14 — `execute_sql`**
```sql
SELECT a.frequency AS account_type, COUNT(*) AS account_count
FROM account a
JOIN district d ON a.district_id = d.district_id
WHERE a.account_id NOT IN (SELECT account_id FROM loan)
  AND d.A11 > 8000
  AND d.A11 <= 9000
GROUP BY a.frequency
```
<details><summary>Result</summary>

```
account_type | account_count
----------------------------
POPLATEK MESICNE | 1889
POPLATEK PO OBRATU | 41
POPLATEK TYDNE | 70
(3 rows)
```
</details>

### Agent Final SQL
```sql
SELECT DISTINCT a.frequency AS account_type
FROM account a
JOIN district d ON a.district_id = d.district_id
WHERE a.account_id NOT IN (SELECT account_id FROM loan)
  AND d.A11 > 8000
  AND d.A11 <= 9000
```

**Agent Result:**
| account_type |
| --- |
| POPLATEK MESICNE |
| POPLATEK TYDNE |
| POPLATEK PO OBRATU |

### Gold SQL
```sql
SELECT T3.type FROM district AS T1 INNER JOIN account AS T2 ON T1.district_id = T2.district_id INNER JOIN disp AS T3 ON T2.account_id = T3.account_id WHERE T3.type != 'OWNER' AND T1.A11 BETWEEN 8000 AND 9000
```

**Gold Result:**
| type |
| --- |
| DISPONENT |
| DISPONENT |
| DISPONENT |
| DISPONENT |
| DISPONENT |
_...456 more rows_

---

## Q5 [CHALLENGING] — MATCH

**Question:** What was the growth rate of the total amount of loans across all accounts for a male client between 1996 and 1997?

**Stats:** 8 tool calls · 7 SQL attempts · 4 errors · 23s

### Agent Trace

**Step 1 — `execute_sql`**
```sql
SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'
```
<details><summary>Result</summary>

```
name
----
account
card
client
disp
district
loan
order
trans
(8 rows)
```
</details>

**Step 2 — `lookup_example`** (scenario: _loan growth rate by gender_)
<details><summary>Result</summary>

```
Gender is single-char: 'M'=male, 'F'=female (NOT 'Male'/'Female').
Join loan → disp → client; filter disp.type = 'OWNER'.
Use STRFTIME('%Y', date) for year extraction in SQLite.

```sql
WITH yearly AS (
    SELECT STRFTIME('%Y', l.date) AS year, SUM(l.amount) AS total
    FROM loan l
    JOIN disp d ON l.account_id = d.account_id
    JOIN client c ON d.client_id = c.client_id
    WHERE c.gender = 'M' AND d.type = 'OWNER'
      AND STRFTIME('%Y', l.date) IN ('1996', '1997')
    GROUP BY STRFTIME(...
```
</details>

**Step 3 — `execute_sql`**
```sql
PRAGMA table_info('loan')
```
<details><summary>Result</summary>

```
SQL Error: SQLite objects created in a thread can only be used in that same thread. The object was created in thread id 140704471597312 and this is thread id 123145626320896.
```
</details>

**Step 4 — `execute_sql`**
```sql
PRAGMA table_info('client')
```
<details><summary>Result</summary>

```
SQL Error: SQLite objects created in a thread can only be used in that same thread. The object was created in thread id 140704471597312 and this is thread id 123145643110400.
```
</details>

**Step 5 — `execute_sql`**
```sql
PRAGMA table_info('disp')
```
<details><summary>Result</summary>

```
SQL Error: SQLite objects created in a thread can only be used in that same thread. The object was created in thread id 140704471597312 and this is thread id 123145626320896.
```
</details>

**Step 6 — `execute_sql`**
```sql
PRAGMA table_info('loan')
```
<details><summary>Result</summary>

```
SQL Error: SQLite objects created in a thread can only be used in that same thread. The object was created in thread id 140704471597312 and this is thread id 123145626320896.
```
</details>

**Step 7 — `execute_sql`**
```sql
SELECT sql FROM sqlite_master WHERE type='table' AND name IN ('loan', 'client', 'disp')
```
<details><summary>Result</summary>

```
sql
---
CREATE TABLE client
(
    client_id   INTEGER        not null
        primary key,
    gender      TEXT not null,
    birth_date  DATE       not null,
    district_id INTEGER        not null,
    foreign key (district_id) references district (district_id)
)
CREATE TABLE disp
(
    disp_id    INTEGER        not null
        primary key,
    client_id  INTEGER        not null,
    account_id INTEGER        not null,
    type      TEXT not null,
    foreign key (account_id) references accou...
```
</details>

**Step 8 — `execute_sql`**
```sql
WITH yearly AS (
    SELECT STRFTIME('%Y', l.date) AS year, SUM(l.amount) AS total
    FROM loan l
    JOIN disp d ON l.account_id = d.account_id
    JOIN client c ON d.client_id = c.client_id
    WHERE c.gender = 'M' AND d.type = 'OWNER'
      AND STRFTIME('%Y', l.date) IN ('1996', '1997')
    GROUP BY STRFTIME('%Y', l.date)
)
SELECT
    MAX(CASE WHEN year='1996' THEN total END) AS total_1996,
    MAX(CASE WHEN year='1997' THEN total END) AS total_1997,
    ROUND((MAX(CASE WHEN year='1997' THEN total END) -
           MAX(CASE WHEN year='1996' THEN total END)) * 100.0 /
           MAX(CASE WHEN year='1996' THEN total END), 2) AS growth_rate_pct
FROM yearly;
```
<details><summary>Result</summary>

```
total_1996 | total_1997 | growth_rate_pct
-----------------------------------------
10084572 | 12635988 | 25.3
(1 rows)
```
</details>

### Agent Final SQL
```sql
WITH yearly AS (
    SELECT STRFTIME('%Y', l.date) AS year, SUM(l.amount) AS total
    FROM loan l
    JOIN disp d ON l.account_id = d.account_id
    JOIN client c ON d.client_id = c.client_id
    WHERE c.gender = 'M' AND d.type = 'OWNER'
      AND STRFTIME('%Y', l.date) IN ('1996', '1997')
    GROUP BY STRFTIME('%Y', l.date)
)
SELECT
    MAX(CASE WHEN year='1996' THEN total END) AS total_1996,
    MAX(CASE WHEN year='1997' THEN total END) AS total_1997,
    ROUND((MAX(CASE WHEN year='1997' THEN total END) -
           MAX(CASE WHEN year='1996' THEN total END)) * 100.0 /
           MAX(CASE WHEN year='1996' THEN total END), 2) AS growth_rate_pct
FROM yearly;
```

**Agent Result:**
| total_1996 | total_1997 | growth_rate_pct |
| --- | --- | --- |
| 10084572 | 12635988 | 25.3 |

### Gold SQL
```sql
SELECT CAST((SUM(CASE WHEN STRFTIME('%Y', T1.date) = '1997' THEN T1.amount ELSE 0 END) - SUM(CASE WHEN STRFTIME('%Y', T1.date) = '1996' THEN T1.amount ELSE 0 END)) AS REAL) * 100 / SUM(CASE WHEN STRFTIME('%Y', T1.date) = '1996' THEN T1.amount ELSE 0 END) FROM loan AS T1 INNER JOIN account AS T2 ON T1.account_id = T2.account_id INNER JOIN disp AS T3 ON T3.account_id = T2.account_id INNER JOIN client AS T4 ON T4.client_id = T3.client_id WHERE T4.gender = 'M' AND T3.type = 'OWNER'
```

**Gold Result:**
| CAST((SUM(CASE WHEN STRFTIME('%Y', T1.date) = '1997' THEN T1.amount ELSE 0 END) - SUM(CASE WHEN STRFTIME('%Y', T1.date) = '1996' THEN T1.amount ELSE 0 END)) AS REAL) * 100 / SUM(CASE WHEN STRFTIME('%Y', T1.date) = '1996' THEN T1.amount ELSE 0 END) |
| --- |
| 25.300191222790616 |

---

## Q6 [CHALLENGING] — MATCH

**Question:** How often does account number 3 request an account statement to be released? What was the aim of debiting 3539 in total?

**Stats:** 14 tool calls · 12 SQL attempts · 5 errors · 37s

### Agent Trace

**Step 1 — `execute_sql`**
```sql
SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'
```
<details><summary>Result</summary>

```
name
----
account
card
client
disp
district
loan
order
trans
(8 rows)
```
</details>

**Step 2 — `execute_sql`**
```sql
PRAGMA table_info('account')
```
<details><summary>Result</summary>

```
SQL Error: SQLite objects created in a thread can only be used in that same thread. The object was created in thread id 140704471597312 and this is thread id 123145626320896.
```
</details>

**Step 3 — `execute_sql`**
```sql
PRAGMA table_info('trans')
```
<details><summary>Result</summary>

```
SQL Error: SQLite objects created in a thread can only be used in that same thread. The object was created in thread id 140704471597312 and this is thread id 123145626320896.
```
</details>

**Step 4 — `execute_sql`**
```sql
PRAGMA table_info('order')
```
<details><summary>Result</summary>

```
SQL Error: SQLite objects created in a thread can only be used in that same thread. The object was created in thread id 140704471597312 and this is thread id 123145643110400.
```
</details>

**Step 5 — `execute_sql`**
```sql
PRAGMA table_info('account')
```
<details><summary>Result</summary>

```
SQL Error: SQLite objects created in a thread can only be used in that same thread. The object was created in thread id 140704471597312 and this is thread id 123145626320896.
```
</details>

**Step 6 — `execute_sql`**
```sql
PRAGMA table_info('account')
```
<details><summary>Result</summary>

```
SQL Error: SQLite objects created in a thread can only be used in that same thread. The object was created in thread id 140704471597312 and this is thread id 123145626320896.
```
</details>

**Step 7 — `execute_sql`**
```sql
SELECT * FROM account LIMIT 3
```
<details><summary>Result</summary>

```
account_id | district_id | frequency | date
-------------------------------------------
1 | 18 | POPLATEK MESICNE | 1995-03-24
2 | 1 | POPLATEK MESICNE | 1993-02-26
3 | 5 | POPLATEK MESICNE | 1997-07-07
(3 rows)
```
</details>

**Step 8 — `lookup_example`** (scenario: _czech transaction codes_)
<details><summary>Result</summary>

```
trans.type: 'PRIJEM'=credit, 'VYDAJ'=debit, 'VYBER'=withdrawal.
trans.operation: 'VKLAD'=cash deposit, 'PREVOD Z UCTU'=transfer in, 'PREVOD NA UCET'=transfer out, 'VYBER KARTOU'=credit card.
trans.k_symbol: 'UROK'=interest, 'SLUZBY'=services, 'SIPO'=household, 'DUCHOD'=pension, 'POJISTNE'=insurance, 'UVER'=loan. Often NULL/whitespace — use TRIM().

```sql
-- Total deposits per account
SELECT account_id, SUM(amount) AS total_deposited
FROM trans WHERE type = 'PRIJEM'
GROUP BY account_id;
```
```
</details>

**Step 9 — `execute_sql`**
```sql
SELECT * FROM trans WHERE account_id = 3 LIMIT 5
```
<details><summary>Result</summary>

```
trans_id | account_id | date | type | operation | amount | balance | k_symbol | bank | account
----------------------------------------------------------------------------------------------
837 | 3 | 1997-07-07 | PRIJEM | VKLAD | 1000 | 1000 | None | None | None
844 | 3 | 1997-07-11 | PRIJEM | VKLAD | 7502 | 8502 | None | None | None
845 | 3 | 1997-08-11 | PRIJEM | VKLAD | 7502 | 23828 | None | None | None
846 | 3 | 1997-09-11 | PRIJEM | VKLAD | 7502 | 29009 | None | None | None
847 | 3 | 1997-1...
```
</details>

**Step 10 — `execute_sql`**
```sql
SELECT frequency FROM account WHERE account_id = 3
```
<details><summary>Result</summary>

```
frequency
---------
POPLATEK MESICNE
(1 rows)
```
</details>

**Step 11 — `execute_sql`**
```sql
SELECT DISTINCT frequency FROM account
```
<details><summary>Result</summary>

```
frequency
---------
POPLATEK MESICNE
POPLATEK TYDNE
POPLATEK PO OBRATU
(3 rows)
```
</details>

**Step 12 — `execute_sql`**
```sql
SELECT * FROM trans WHERE account_id = 3 AND type = 'VYDAJ' AND amount = 3539
```
<details><summary>Result</summary>

```
trans_id | account_id | date | type | operation | amount | balance | k_symbol | bank | account
----------------------------------------------------------------------------------------------
921 | 3 | 1997-12-09 | VYDAJ | PREVOD NA UCET | 3539 | 17958 | POJISTNE | AB | 59972357
922 | 3 | 1998-01-09 | VYDAJ | PREVOD NA UCET | 3539 | 14548 | POJISTNE | AB | 59972357
923 | 3 | 1998-02-09 | VYDAJ | PREVOD NA UCET | 3539 | 7594 | POJISTNE | AB | 59972357
924 | 3 | 1998-03-09 | VYDAJ | PREVOD NA UCET |...
```
</details>

**Step 13 — `lookup_example`** (scenario: _district column reference_)
<details><summary>Result</summary>

```
The district table uses cryptic A2–A16 column names:
- A2 = district name, A3 = region, A4 = population (TEXT — CAST to INTEGER)
- A11 = average salary, A12 = unemployment 1995, A13 = unemployment 1996
- A14 = entrepreneurs per 1000, A15 = crimes 1995, A16 = crimes 1996

```sql
-- District with most inhabitants in a region
SELECT A2, CAST(A4 AS INTEGER) AS population
FROM district WHERE A3 = 'south Bohemia'
ORDER BY CAST(A4 AS INTEGER) DESC LIMIT 1;
```
```
</details>

**Step 14 — `execute_sql`**
```sql
SELECT 
    a.account_id,
    a.frequency,
    CASE a.frequency 
        WHEN 'POPLATEK MESICNE' THEN 'Monthly'
        WHEN 'POPLATEK TYDNE' THEN 'Weekly'
        WHEN 'POPLATEK PO OBRATU' THEN 'After each transaction'
    END AS statement_frequency,
    t.k_symbol,
    CASE t.k_symbol 
        WHEN 'POJISTNE' THEN 'Insurance payment'
    END AS debit_purpose
FROM account a
LEFT JOIN trans t ON a.account_id = t.account_id 
    AND t.type = 'VYDAJ' 
    AND t.amount = 3539
WHERE a.account_id = 3
LIMIT 1
```
<details><summary>Result</summary>

```
account_id | frequency | statement_frequency | k_symbol | debit_purpose
-----------------------------------------------------------------------
3 | POPLATEK MESICNE | Monthly | POJISTNE | Insurance payment
(1 rows)
```
</details>

### Agent Final SQL
```sql
SELECT 
    a.account_id,
    a.frequency,
    CASE a.frequency 
        WHEN 'POPLATEK MESICNE' THEN 'Monthly'
        WHEN 'POPLATEK TYDNE' THEN 'Weekly'
        WHEN 'POPLATEK PO OBRATU' THEN 'After each transaction'
    END AS statement_frequency,
    t.k_symbol,
    CASE t.k_symbol 
        WHEN 'POJISTNE' THEN 'Insurance payment'
    END AS debit_purpose
FROM account a
LEFT JOIN trans t ON a.account_id = t.account_id 
    AND t.type = 'VYDAJ' 
    AND t.amount = 3539
WHERE a.account_id = 3
LIMIT 1
```

**Agent Result:**
| account_id | frequency | statement_frequency | k_symbol | debit_purpose |
| --- | --- | --- | --- | --- |
| 3 | POPLATEK MESICNE | Monthly | POJISTNE | Insurance payment |

### Gold SQL
```sql
SELECT T1.frequency, T2.k_symbol FROM account AS T1 INNER JOIN (SELECT account_id, k_symbol, SUM(amount) AS total_amount FROM `order` GROUP BY account_id, k_symbol) AS T2 ON T1.account_id = T2.account_id WHERE T1.account_id = 3 AND T2.total_amount = 3539
```

**Gold Result:**
| frequency | k_symbol |
| --- | --- |
| POPLATEK MESICNE | POJISTNE |

---

## Q7 [MODERATE] — MATCH

**Question:** How many accounts who choose issuance after transaction are staying in East Bohemia region?

**Stats:** 14 tool calls · 12 SQL attempts · 7 errors · 29s

### Agent Trace

**Step 1 — `execute_sql`**
```sql
SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'
```
<details><summary>Result</summary>

```
name
----
account
card
client
disp
district
loan
order
trans
(8 rows)
```
</details>

**Step 2 — `lookup_example`** (scenario: _district column reference_)
<details><summary>Result</summary>

```
The district table uses cryptic A2–A16 column names:
- A2 = district name, A3 = region, A4 = population (TEXT — CAST to INTEGER)
- A11 = average salary, A12 = unemployment 1995, A13 = unemployment 1996
- A14 = entrepreneurs per 1000, A15 = crimes 1995, A16 = crimes 1996

```sql
-- District with most inhabitants in a region
SELECT A2, CAST(A4 AS INTEGER) AS population
FROM district WHERE A3 = 'south Bohemia'
ORDER BY CAST(A4 AS INTEGER) DESC LIMIT 1;
```
```
</details>

**Step 3 — `execute_sql`**
```sql
PRAGMA table_info('account')
```
<details><summary>Result</summary>

```
SQL Error: SQLite objects created in a thread can only be used in that same thread. The object was created in thread id 140704471597312 and this is thread id 123145626320896.
```
</details>

**Step 4 — `execute_sql`**
```sql
PRAGMA table_info('card')
```
<details><summary>Result</summary>

```
SQL Error: SQLite objects created in a thread can only be used in that same thread. The object was created in thread id 140704471597312 and this is thread id 123145643110400.
```
</details>

**Step 5 — `execute_sql`**
```sql
PRAGMA table_info('disp')
```
<details><summary>Result</summary>

```
SQL Error: SQLite objects created in a thread can only be used in that same thread. The object was created in thread id 140704471597312 and this is thread id 123145659899904.
```
</details>

**Step 6 — `execute_sql`**
```sql
SELECT DISTINCT A3 FROM district
```
<details><summary>Result</summary>

```
A3
--
Prague
central Bohemia
south Bohemia
west Bohemia
north Bohemia
east Bohemia
south Moravia
north Moravia
(8 rows)
```
</details>

**Step 7 — `execute_sql`**
```sql
PRAGMA table_info('account')
```
<details><summary>Result</summary>

```
SQL Error: SQLite objects created in a thread can only be used in that same thread. The object was created in thread id 140704471597312 and this is thread id 123145626320896.
```
</details>

**Step 8 — `execute_sql`**
```sql
PRAGMA table_info('card')
```
<details><summary>Result</summary>

```
SQL Error: SQLite objects created in a thread can only be used in that same thread. The object was created in thread id 140704471597312 and this is thread id 123145626320896.
```
</details>

**Step 9 — `execute_sql`**
```sql
PRAGMA table_info('disp')
```
<details><summary>Result</summary>

```
SQL Error: SQLite objects created in a thread can only be used in that same thread. The object was created in thread id 140704471597312 and this is thread id 123145626320896.
```
</details>

**Step 10 — `execute_sql`**
```sql
PRAGMA table_info('account')
```
<details><summary>Result</summary>

```
SQL Error: SQLite objects created in a thread can only be used in that same thread. The object was created in thread id 140704471597312 and this is thread id 123145626320896.
```
</details>

**Step 11 — `execute_sql`**
```sql
SELECT sql FROM sqlite_master WHERE type='table' AND name IN ('account', 'card', 'disp', 'district')
```
<details><summary>Result</summary>

```
sql
---
CREATE TABLE account
(
    account_id  INTEGER default 0 not null
        primary key,
    district_id INTEGER default 0 not null,
    frequency   TEXT   not null,
    date        DATE          not null,
    foreign key (district_id) references district (district_id)
)
CREATE TABLE card
(
    card_id INTEGER default 0 not null
        primary key,
    disp_id INTEGER           not null,
    type    TEXT    not null,
    issued  DATE          not null,
    foreign key (disp_id) references...
```
</details>

**Step 12 — `execute_sql`**
```sql
SELECT DISTINCT frequency FROM account
```
<details><summary>Result</summary>

```
frequency
---------
POPLATEK MESICNE
POPLATEK TYDNE
POPLATEK PO OBRATU
(3 rows)
```
</details>

**Step 13 — `lookup_example`** (scenario: _czech transaction codes_)
<details><summary>Result</summary>

```
trans.type: 'PRIJEM'=credit, 'VYDAJ'=debit, 'VYBER'=withdrawal.
trans.operation: 'VKLAD'=cash deposit, 'PREVOD Z UCTU'=transfer in, 'PREVOD NA UCET'=transfer out, 'VYBER KARTOU'=credit card.
trans.k_symbol: 'UROK'=interest, 'SLUZBY'=services, 'SIPO'=household, 'DUCHOD'=pension, 'POJISTNE'=insurance, 'UVER'=loan. Often NULL/whitespace — use TRIM().

```sql
-- Total deposits per account
SELECT account_id, SUM(amount) AS total_deposited
FROM trans WHERE type = 'PRIJEM'
GROUP BY account_id;
```
```
</details>

**Step 14 — `execute_sql`**
```sql
SELECT COUNT(*) AS account_count
FROM account a
JOIN district d ON a.district_id = d.district_id
WHERE a.frequency = 'POPLATEK PO OBRATU'
  AND d.A3 = 'east Bohemia'
```
<details><summary>Result</summary>

```
account_count
-------------
13
(1 rows)
```
</details>

### Agent Final SQL
```sql
SELECT COUNT(*) AS account_count
FROM account a
JOIN district d ON a.district_id = d.district_id
WHERE a.frequency = 'POPLATEK PO OBRATU'
  AND d.A3 = 'east Bohemia'
```

**Agent Result:**
| account_count |
| --- |
| 13 |

### Gold SQL
```sql
SELECT COUNT(T2.account_id) FROM district AS T1 INNER JOIN account AS T2 ON T1.district_id = T2.district_id WHERE T1.A3 = 'east Bohemia' AND T2.frequency = 'POPLATEK PO OBRATU'
```

**Gold Result:**
| COUNT(T2.account_id) |
| --- |
| 13 |

---

## Q8 [MODERATE] — MATCH

**Question:** Among the accounts who have approved loan date in 1997, list out the accounts that have the lowest approved amount and choose weekly issuance statement.

**Stats:** 15 tool calls · 14 SQL attempts · 6 errors · 53s

### Agent Trace

**Step 1 — `execute_sql`**
```sql
SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'
```
<details><summary>Result</summary>

```
name
----
account
card
client
disp
district
loan
order
trans
(8 rows)
```
</details>

**Step 2 — `execute_sql`**
```sql
PRAGMA table_info('loan')
```
<details><summary>Result</summary>

```
SQL Error: SQLite objects created in a thread can only be used in that same thread. The object was created in thread id 140704471597312 and this is thread id 123145592741888.
```
</details>

**Step 3 — `execute_sql`**
```sql
PRAGMA table_info('account')
```
<details><summary>Result</summary>

```
SQL Error: SQLite objects created in a thread can only be used in that same thread. The object was created in thread id 140704471597312 and this is thread id 123145592741888.
```
</details>

**Step 4 — `execute_sql`**
```sql
PRAGMA table_info('loan')
```
<details><summary>Result</summary>

```
SQL Error: SQLite objects created in a thread can only be used in that same thread. The object was created in thread id 140704471597312 and this is thread id 123145592741888.
```
</details>

**Step 5 — `execute_sql`**
```sql
PRAGMA table_info('loan')
```
<details><summary>Result</summary>

```
SQL Error: SQLite objects created in a thread can only be used in that same thread. The object was created in thread id 140704471597312 and this is thread id 123145592741888.
```
</details>

**Step 6 — `execute_sql`**
```sql
PRAGMA table_info('loan')
```
<details><summary>Result</summary>

```
SQL Error: SQLite objects created in a thread can only be used in that same thread. The object was created in thread id 140704471597312 and this is thread id 123145592741888.
```
</details>

**Step 7 — `execute_sql`**
```sql
PRAGMA table_info('loan')
```
<details><summary>Result</summary>

```
SQL Error: SQLite objects created in a thread can only be used in that same thread. The object was created in thread id 140704471597312 and this is thread id 123145592741888.
```
</details>

**Step 8 — `execute_sql`**
```sql
SELECT sql FROM sqlite_master WHERE type='table' AND name='loan'
```
<details><summary>Result</summary>

```
sql
---
CREATE TABLE loan
(
    loan_id    INTEGER default 0 not null
        primary key,
    account_id INTEGER           not null,
    date       DATE          not null,
    amount     INTEGER           not null,
    duration   INTEGER           not null,
    payments   REAL not null,
    status     TEXT    not null,
    foreign key (account_id) references account (account_id)
)
(1 rows)
```
</details>

**Step 9 — `execute_sql`**
```sql
SELECT sql FROM sqlite_master WHERE type='table' AND name='account'
```
<details><summary>Result</summary>

```
sql
---
CREATE TABLE account
(
    account_id  INTEGER default 0 not null
        primary key,
    district_id INTEGER default 0 not null,
    frequency   TEXT   not null,
    date        DATE          not null,
    foreign key (district_id) references district (district_id)
)
(1 rows)
```
</details>

**Step 10 — `execute_sql`**
```sql
SELECT DISTINCT frequency FROM account
```
<details><summary>Result</summary>

```
frequency
---------
POPLATEK MESICNE
POPLATEK TYDNE
POPLATEK PO OBRATU
(3 rows)
```
</details>

**Step 11 — `lookup_example`** (scenario: _czech transaction codes_)
<details><summary>Result</summary>

```
trans.type: 'PRIJEM'=credit, 'VYDAJ'=debit, 'VYBER'=withdrawal.
trans.operation: 'VKLAD'=cash deposit, 'PREVOD Z UCTU'=transfer in, 'PREVOD NA UCET'=transfer out, 'VYBER KARTOU'=credit card.
trans.k_symbol: 'UROK'=interest, 'SLUZBY'=services, 'SIPO'=household, 'DUCHOD'=pension, 'POJISTNE'=insurance, 'UVER'=loan. Often NULL/whitespace — use TRIM().

```sql
-- Total deposits per account
SELECT account_id, SUM(amount) AS total_deposited
FROM trans WHERE type = 'PRIJEM'
GROUP BY account_id;
```
```
</details>

**Step 12 — `execute_sql`**
```sql
SELECT a.account_id
FROM loan l
JOIN account a ON l.account_id = a.account_id
WHERE strftime('%Y', l.date) = '1997'
  AND a.frequency = 'POPLATEK TYDNE'
  AND l.amount = (
    SELECT MIN(l2.amount)
    FROM loan l2
    WHERE strftime('%Y', l2.date) = '1997'
  )
```
<details><summary>Result</summary>

```
Query executed successfully. 0 rows returned.
```
</details>

**Step 13 — `execute_sql`**
```sql
SELECT l.account_id, l.amount, a.frequency
FROM loan l
JOIN account a ON l.account_id = a.account_id
WHERE strftime('%Y', l.date) = '1997'
ORDER BY l.amount ASC
LIMIT 20
```
<details><summary>Result</summary>

```
account_id | amount | frequency
-------------------------------
8268 | 8616 | POPLATEK MESICNE
666 | 12540 | POPLATEK MESICNE
3637 | 14028 | POPLATEK MESICNE
6792 | 17184 | POPLATEK MESICNE
9513 | 18324 | POPLATEK MESICNE
10954 | 18720 | POPLATEK MESICNE
3329 | 19248 | POPLATEK MESICNE
2134 | 22932 | POPLATEK MESICNE
9242 | 23184 | POPLATEK MESICNE
1329 | 23520 | POPLATEK MESICNE
10711 | 24180 | POPLATEK MESICNE
2986 | 24792 | POPLATEK MESICNE
6097 | 26208 | POPLATEK MESICNE
176 | 27456 | POPLAT...
```
</details>

**Step 14 — `execute_sql`**
```sql
SELECT a.account_id, l.amount, a.frequency
FROM loan l
JOIN account a ON l.account_id = a.account_id
WHERE strftime('%Y', l.date) = '1997'
  AND a.frequency = 'POPLATEK TYDNE'
ORDER BY l.amount ASC
```
<details><summary>Result</summary>

```
account_id | amount | frequency
-------------------------------
176 | 27456 | POPLATEK TYDNE
11141 | 44940 | POPLATEK TYDNE
11027 | 48096 | POPLATEK TYDNE
2187 | 53472 | POPLATEK TYDNE
3448 | 68664 | POPLATEK TYDNE
5196 | 79824 | POPLATEK TYDNE
4052 | 106128 | POPLATEK TYDNE
3942 | 107352 | POPLATEK TYDNE
1252 | 148140 | POPLATEK TYDNE
4969 | 150180 | POPLATEK TYDNE
8931 | 186300 | POPLATEK TYDNE
5362 | 202848 | POPLATEK TYDNE
10063 | 233424 | POPLATEK TYDNE
8158 | 238716 | POPLATEK TYDNE
2051 |...
```
</details>

**Step 15 — `execute_sql`**
```sql
SELECT a.account_id, l.amount AS approved_amount
FROM loan l
JOIN account a ON l.account_id = a.account_id
WHERE strftime('%Y', l.date) = '1997'
  AND a.frequency = 'POPLATEK TYDNE'
  AND l.amount = (
    SELECT MIN(l2.amount)
    FROM loan l2
    JOIN account a2 ON l2.account_id = a2.account_id
    WHERE strftime('%Y', l2.date) = '1997'
      AND a2.frequency = 'POPLATEK TYDNE'
  )
```
<details><summary>Result</summary>

```
account_id | approved_amount
----------------------------
176 | 27456
(1 rows)
```
</details>

### Agent Final SQL
```sql
SELECT a.account_id, l.amount AS approved_amount
FROM loan l
JOIN account a ON l.account_id = a.account_id
WHERE strftime('%Y', l.date) = '1997'
  AND a.frequency = 'POPLATEK TYDNE'
  AND l.amount = (
    SELECT MIN(l2.amount)
    FROM loan l2
    JOIN account a2 ON l2.account_id = a2.account_id
    WHERE strftime('%Y', l2.date) = '1997'
      AND a2.frequency = 'POPLATEK TYDNE'
  )
```

**Agent Result:**
| account_id | approved_amount |
| --- | --- |
| 176 | 27456 |

### Gold SQL
```sql
SELECT T2.account_id FROM loan AS T1 INNER JOIN account AS T2 ON T1.account_id = T2.account_id WHERE STRFTIME('%Y', T1.date) = '1997' AND T2.frequency = 'POPLATEK TYDNE' ORDER BY T1.amount LIMIT 1
```

**Gold Result:**
| account_id |
| --- |
| 176 |

---

## Q9 [MODERATE] — MATCH

**Question:** For accounts in 1993 with statement issued after transaction, list the account ID, district name and district region.

**Stats:** 12 tool calls · 10 SQL attempts · 5 errors · 42s

### Agent Trace

**Step 1 — `execute_sql`**
```sql
SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'
```
<details><summary>Result</summary>

```
name
----
account
card
client
disp
district
loan
order
trans
(8 rows)
```
</details>

**Step 2 — `execute_sql`**
```sql
PRAGMA table_info('account')
```
<details><summary>Result</summary>

```
SQL Error: SQLite objects created in a thread can only be used in that same thread. The object was created in thread id 140704471597312 and this is thread id 123145592741888.
```
</details>

**Step 3 — `execute_sql`**
```sql
PRAGMA table_info('district')
```
<details><summary>Result</summary>

```
SQL Error: SQLite objects created in a thread can only be used in that same thread. The object was created in thread id 140704471597312 and this is thread id 123145592741888.
```
</details>

**Step 4 — `execute_sql`**
```sql
PRAGMA table_info('account')
```
<details><summary>Result</summary>

```
SQL Error: SQLite objects created in a thread can only be used in that same thread. The object was created in thread id 140704471597312 and this is thread id 123145592741888.
```
</details>

**Step 5 — `execute_sql`**
```sql
PRAGMA table_info('account')
```
<details><summary>Result</summary>

```
SQL Error: SQLite objects created in a thread can only be used in that same thread. The object was created in thread id 140704471597312 and this is thread id 123145592741888.
```
</details>

**Step 6 — `execute_sql`**
```sql
PRAGMA table_info('account');
```
<details><summary>Result</summary>

```
SQL Error: SQLite objects created in a thread can only be used in that same thread. The object was created in thread id 140704471597312 and this is thread id 123145592741888.
```
</details>

**Step 7 — `execute_sql`**
```sql
SELECT sql FROM sqlite_master WHERE type='table' AND name='account'
```
<details><summary>Result</summary>

```
sql
---
CREATE TABLE account
(
    account_id  INTEGER default 0 not null
        primary key,
    district_id INTEGER default 0 not null,
    frequency   TEXT   not null,
    date        DATE          not null,
    foreign key (district_id) references district (district_id)
)
(1 rows)
```
</details>

**Step 8 — `execute_sql`**
```sql
SELECT sql FROM sqlite_master WHERE type='table' AND name='district'
```
<details><summary>Result</summary>

```
sql
---
CREATE TABLE district
(
    district_id INTEGER default 0 not null
        primary key,
    A2          TEXT   not null,
    A3          TEXT   not null,
    A4          TEXT       not null,
    A5          TEXT           not null,
    A6          TEXT           not null,
    A7          TEXT           not null,
    A8          INTEGER        not null,
    A9          INTEGER           not null,
    A10         REAL not null,
    A11         INTEGER           not null,
    A12         RE...
```
</details>

**Step 9 — `lookup_example`** (scenario: _district column reference_)
<details><summary>Result</summary>

```
The district table uses cryptic A2–A16 column names:
- A2 = district name, A3 = region, A4 = population (TEXT — CAST to INTEGER)
- A11 = average salary, A12 = unemployment 1995, A13 = unemployment 1996
- A14 = entrepreneurs per 1000, A15 = crimes 1995, A16 = crimes 1996

```sql
-- District with most inhabitants in a region
SELECT A2, CAST(A4 AS INTEGER) AS population
FROM district WHERE A3 = 'south Bohemia'
ORDER BY CAST(A4 AS INTEGER) DESC LIMIT 1;
```
```
</details>

**Step 10 — `execute_sql`**
```sql
SELECT DISTINCT frequency FROM account
```
<details><summary>Result</summary>

```
frequency
---------
POPLATEK MESICNE
POPLATEK TYDNE
POPLATEK PO OBRATU
(3 rows)
```
</details>

**Step 11 — `lookup_example`** (scenario: _czech transaction codes_)
<details><summary>Result</summary>

```
trans.type: 'PRIJEM'=credit, 'VYDAJ'=debit, 'VYBER'=withdrawal.
trans.operation: 'VKLAD'=cash deposit, 'PREVOD Z UCTU'=transfer in, 'PREVOD NA UCET'=transfer out, 'VYBER KARTOU'=credit card.
trans.k_symbol: 'UROK'=interest, 'SLUZBY'=services, 'SIPO'=household, 'DUCHOD'=pension, 'POJISTNE'=insurance, 'UVER'=loan. Often NULL/whitespace — use TRIM().

```sql
-- Total deposits per account
SELECT account_id, SUM(amount) AS total_deposited
FROM trans WHERE type = 'PRIJEM'
GROUP BY account_id;
```
```
</details>

**Step 12 — `execute_sql`**
```sql
SELECT 
    a.account_id,
    d.A2 AS district_name,
    d.A3 AS district_region
FROM account a
JOIN district d ON a.district_id = d.district_id
WHERE strftime('%Y', a.date) = '1993'
  AND a.frequency = 'POPLATEK PO OBRATU'
```
<details><summary>Result</summary>

```
account_id | district_name | district_region
--------------------------------------------
66 | Rychnov nad Kneznou | east Bohemia
273 | Karlovy Vary | west Bohemia
485 | Kutna Hora | central Bohemia
539 | Rakovnik | central Bohemia
1050 | Hodonin | south Moravia
1286 | Zdar nad Sazavou | south Moravia
1527 | Tachov | west Bohemia
1539 | Hl.m. Praha | Prague
1965 | Hodonin | south Moravia
2137 | Kladno | central Bohemia
2464 | Decin | north Bohemia
2705 | Vyskov | south Moravia
3837 | Rychnov nad...
```
</details>

### Agent Final SQL
```sql
SELECT 
    a.account_id,
    d.A2 AS district_name,
    d.A3 AS district_region
FROM account a
JOIN district d ON a.district_id = d.district_id
WHERE strftime('%Y', a.date) = '1993'
  AND a.frequency = 'POPLATEK PO OBRATU'
```

**Agent Result:**
| account_id | district_name | district_region |
| --- | --- | --- |
| 66 | Rychnov nad Kneznou | east Bohemia |
| 273 | Karlovy Vary | west Bohemia |
| 485 | Kutna Hora | central Bohemia |
| 539 | Rakovnik | central Bohemia |
| 1050 | Hodonin | south Moravia |
_...16 more rows_

### Gold SQL
```sql
SELECT T1.account_id, T2.A2, T2.A3 FROM account AS T1 INNER JOIN district AS T2 ON T1.district_id = T2.district_id WHERE T1.frequency = 'POPLATEK PO OBRATU' AND STRFTIME('%Y', T1.date)= '1993'
```

**Gold Result:**
| account_id | A2 | A3 |
| --- | --- | --- |
| 66 | Rychnov nad Kneznou | east Bohemia |
| 273 | Karlovy Vary | west Bohemia |
| 485 | Kutna Hora | central Bohemia |
| 539 | Rakovnik | central Bohemia |
| 1050 | Hodonin | south Moravia |
_...16 more rows_

---

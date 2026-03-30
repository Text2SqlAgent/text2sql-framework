# Text to SQL Agents for 2026

The test results below outline the ease of use and effectiveness of simply giving an agent execute SQL to do text to sql. Using Opus 4.6, the results show how it will intelligently uses the execute sql tool to explore the data schema, then uses it again to test and validate queries. 

## Test Setup

- **Database:** BIRD financial — Czech banking data, 8 tables (clients, accounts, loans, transactions, districts)
- **Questions:** 8 total — 6 challenging, 2 moderate
- **Model:** Claude Opus 4.6
- **Set Up:** We used the text2sql SDK and ONLY exposed the execute_sql tool to the LLM. This was a very minimal set up - all we did was install the SDK, add in DB connection string + LLM API creds. (Took about 2 min)


## About BIRD

[BIRD](https://bird-bench.github.io/) is the industry-standard text-to-SQL benchmark — The "challenging" tier requires deep schema understanding; even human annotators sometimes disagree on the correct SQL. I picked several challenging/moderate questions from the financial dataset to do my testing.

---

---

## First Run: 6/8

| # | Difficulty | Question | Result |
|---|---|---|---|
| 1 | Challenging | List account numbers of oldest female clients with lowest avg salary; calculate salary gap | ✗ |
| 2 | Challenging | For the client whose loan was approved first on 1993/7/5, what is the balance increase rate from 1993/3/22 to 1998/12/27? | ✓ |
| 3 | Challenging | Account types not eligible for loans where district avg income is $8k–$9k | ✗ |
| 4 | Challenging | Growth rate of total loans for male clients between 1996 and 1997 | ✓ |
| 5 | Challenging | How often does account 3 request a statement? What was the aim of debiting 3539 in total? | ✓ |
| 6 | Challenging | For loans still running where client is in debt, list district and unemployment rate change 1995→1996 | ✓ |
| 7 | Moderate | How many accounts with issuance after transaction are in East Bohemia? | ✓ |
| 8 | Moderate | For accounts in 1993 with statement issued after transaction, list account ID, district name and region | ✓ |

**6/8 correct on the first run.** The two failures were both interpretation problems — questions with multiple valid answers — not schema confusion or execution errors. 

Id encourage people to take a look at the traces to understand how the LLM uses the info schema to explore, then execute_sql to test and validate. 

→ [View traces](traces_bird_financial_opus.jsonl)

---

## Improving with Circular MCP

The Circular MCP (coming soon!) is an especially designed MCP + agent observability UI which is optimized for improving text to sql agents built on top of the text2sql SDK. 

I passed the tool call traces + results to the analyze_traces tool. Under the hood of the MCP is an LLM reading the traces. This LLM then returned suggested context additions for improved results on the 8 test questions. Specifically, it suggested we introduce the SDK's 'lookup_example' tool (this is essentially creates a skills.md file for query solutions which may not be obvious from the data schema alone). I clicked accept on the suggested edits, re ran the test set, and got 8/8 results. 

(Checkout the traces_bird_financial_opus_v2.jsonl file to see how it used thus new tool)

---

## Detailed Trace Report

For a full breakdown of each question including the agent's step-by-step reasoning, every SQL attempt, and side-by-side comparison with the gold answer: [bird_financial_report.md](bird_financial_report.md)

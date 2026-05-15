# Canonical queries — example/starter template

Each `## heading` is a canonical query the SDK will route to directly when a
user's question matches its name + aliases. Copy this file, swap the SQL for
your real schema, and pass it as `canonical_queries=` to `TextSQL(...)`.

The matcher is keyword-overlap based — list the realistic phrasings finance
or operations staff actually use. Keep aliases short and specific.

## accounts_receivable_total
aliases: how much are we owed, total receivables, ar balance, money owed to us, outstanding invoices total
description: Sum of unpaid invoice amounts across all customers.

```sql
SELECT SUM(amount_due) AS total_owed
FROM invoices
WHERE status = 'unpaid'
```

## accounts_payable_total
aliases: how much do we owe, total payables, ap balance, money we owe, outstanding bills total
description: Sum of unpaid vendor bills.

```sql
SELECT SUM(amount_due) AS total_we_owe
FROM bills
WHERE status = 'unpaid'
```

## top_customers_by_revenue_ytd
aliases: top customers this year, biggest customers ytd, best customers year to date
description: Top 10 customers by revenue, year to date.

```sql
SELECT c.customer_name, SUM(i.amount) AS revenue_ytd
FROM customers c
JOIN invoices i ON i.customer_id = c.customer_id
WHERE i.invoice_date >= DATE_TRUNC('year', CURRENT_DATE)
  AND i.status IN ('paid', 'partial')
GROUP BY c.customer_name
ORDER BY revenue_ytd DESC
LIMIT 10
```

## top_product_last_quarter
aliases: best selling product last quarter, top seller last quarter, top product q
description: Single best-selling product (by units) in the last 3 months.

```sql
SELECT p.product_name, SUM(oi.quantity) AS units_sold
FROM order_items oi
JOIN products p ON p.product_id = oi.product_id
JOIN orders o ON o.order_id = oi.order_id
WHERE o.order_date >= CURRENT_DATE - INTERVAL '3 months'
GROUP BY p.product_name
ORDER BY units_sold DESC
LIMIT 1
```

## monthly_expenses
aliases: expense report, expenses by month, monthly spend, expense breakdown by month
description: Total expenses grouped by month, most recent first.

```sql
SELECT TO_CHAR(expense_date, 'YYYY-MM') AS month,
       SUM(amount) AS total_expense
FROM expenses
GROUP BY TO_CHAR(expense_date, 'YYYY-MM')
ORDER BY month DESC
```

## overdue_invoices
aliases: overdue invoices, late invoices, past due, invoices past due date
description: Unpaid invoices whose due date is in the past.

```sql
SELECT i.invoice_id, c.customer_name, i.amount_due, i.due_date,
       CURRENT_DATE - i.due_date AS days_overdue
FROM invoices i
JOIN customers c ON c.customer_id = i.customer_id
WHERE i.status = 'unpaid' AND i.due_date < CURRENT_DATE
ORDER BY days_overdue DESC
```

## inventory_low_stock
aliases: low stock items, products running low, items to reorder, inventory alert
description: Products at or below the reorder threshold.

```sql
SELECT product_name, stock_quantity, reorder_threshold
FROM products
WHERE stock_quantity <= reorder_threshold
ORDER BY stock_quantity ASC
```

## customer spending
Total customer spending is the sum of `orders.total_amount` grouped by customer.
Join `customers` to `orders` on `customer_id`.

## revenue by category
To get revenue by product category, you need to go through order_items:
- Join `categories` -> `products` on `category_id`
- Join `products` -> `order_items` on `product_id`
- Revenue = SUM(order_items.quantity * order_items.unit_price)
Do NOT use `orders.total_amount` for category-level revenue — that's the whole order total.

## orders by country
Customer country is NOT on the customers table directly.
- Join `customers` -> `regions` on `region_id`
- Country is `regions.country`

## customer location
Customer location is stored in the `regions` table, not on customers directly.
- `regions.region_name` = the region (e.g. "Northeast", "West Coast")
- `regions.country` = the country (e.g. "USA", "Canada")
- Join `customers` to `regions` on `region_id`

## product inventory
Current stock levels are in `products.stock_quantity`.
To find products low on stock: `WHERE stock_quantity < 50`

## order status
Orders have a `status` column with values: 'completed', 'shipped', 'pending'.
- Completed = fully delivered
- Shipped = in transit
- Pending = not yet shipped

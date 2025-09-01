# Database Schema Documentation

This directory contains the SQL scripts for setting up the PostgreSQL database schema for the Sales Reporting System.

## Tables Overview

### 1. `clients` Table
Stores customer/client information with the following structure:
- `client_id` (TEXT, PRIMARY KEY): Unique identifier for each client
- `client_name` (TEXT, NOT NULL): Full name of the client
- `email` (TEXT): Client's email address
- `phone` (TEXT): Client's phone number
- `address` (TEXT): Client's address
- `created_at` (TIMESTAMP): When the client record was created
- `updated_at` (TIMESTAMP): When the client record was last updated

### 2. `items` Table
Stores product catalog information:
- `sku` (TEXT, PRIMARY KEY): Stock Keeping Unit - unique product identifier
- `product_name` (TEXT, NOT NULL): Name of the product
- `category` (TEXT): Product category
- `brand` (TEXT): Product brand
- `description` (TEXT): Product description
- `unit_price` (NUMERIC): Standard selling price
- `cost_price` (NUMERIC): Product cost price
- `is_active` (BOOLEAN): Whether the product is currently active
- `created_at` (TIMESTAMP): When the product record was created
- `updated_at` (TIMESTAMP): When the product record was last updated

### 3. `sales` Table
Stores sales transaction records:
- `order_id` (TEXT, PRIMARY KEY): Unique order identifier
- `client_id` (TEXT, NOT NULL): Reference to the client (foreign key)
- `date` (DATE, NOT NULL): Date of the sale
- `total_sum` (NUMERIC(10,2), NOT NULL): Total amount of the sale
- `price_type` (TEXT, NOT NULL): Type of pricing (retail, wholesale, etc.)
- `status` (TEXT): Order status (default: 'confirmed')
- `created_at` (TIMESTAMP): When the sale record was created
- `updated_at` (TIMESTAMP): When the sale record was last updated

### 4. `sales_items` Table
Stores line items for each sale:
- `order_id` (TEXT, NOT NULL): Reference to the sale (foreign key)
- `line_no` (INTEGER, NOT NULL): Line number within the order
- `sku` (TEXT, NOT NULL): Reference to the product (foreign key)
- `product_name` (TEXT): Product name (denormalized for performance)
- `qty` (NUMERIC(10,3), NOT NULL): Quantity of the item sold
- `price` (NUMERIC(10,2), NOT NULL): Unit price of the item
- `total` (NUMERIC(10,2), NOT NULL): Total amount for this line item
- `created_at` (TIMESTAMP): When the line item was created

## Relationships

- **clients** ← **sales**: One-to-many relationship (one client can have many sales)
- **sales** ← **sales_items**: One-to-many relationship (one sale can have many line items)
- **items** ← **sales_items**: One-to-many relationship (one product can appear in many sales)

## Constraints

- Foreign key constraints ensure referential integrity
- `ON DELETE CASCADE` for sales_items when a sale is deleted
- `ON DELETE RESTRICT` for clients and items to prevent accidental deletion
- Primary keys ensure uniqueness
- NOT NULL constraints on required fields

## Performance Features

- Indexes on frequently queried columns (client_id, date, price_type, sku, etc.)
- Automatic timestamp updates via triggers
- Proper data types for efficient storage and queries

## Usage

1. **Create Tables**: Run `create_tables.sql` to set up the complete schema
2. **Data Loading**: Use the existing data loader service to populate the tables
3. **Reporting**: Query the tables for various sales analytics and reports

## Example Queries

```sql
-- Get total sales by client
SELECT c.client_name, SUM(s.total_sum) as total_sales
FROM clients c
JOIN sales s ON c.client_id = s.client_id
GROUP BY c.client_id, c.client_name
ORDER BY total_sales DESC;

-- Get top selling products
SELECT i.product_name, SUM(si.qty) as total_quantity
FROM items i
JOIN sales_items si ON i.sku = si.sku
GROUP BY i.sku, i.product_name
ORDER BY total_quantity DESC;

-- Get sales by date range
SELECT date, COUNT(*) as order_count, SUM(total_sum) as daily_total
FROM sales
WHERE date BETWEEN '2024-01-01' AND '2024-12-31'
GROUP BY date
ORDER BY date;
```

## Notes

- The schema is designed to work with the existing Python data loader service
- All timestamps include timezone information
- The `uuid-ossp` extension is enabled for potential future use
- The schema supports the existing sales reporting functionality

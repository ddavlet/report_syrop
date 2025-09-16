## Report Syrop — Comprehensive Reports Documentation

This document provides detailed descriptions of all available reports in the Report Syrop system.
The system currently supports 5 main report types, each designed for specific business analysis needs.

### 1. ABC Analysis — Clients (`abc_clients`)

- **Purpose**: Analyzes clients by their revenue contribution with categorization into three segments (A, B, C). Helps identify the most valuable customers and their contribution to total revenue.
- **Business use cases**:
  - Identify top-revenue clients for special treatment
  - Understand customer revenue distribution
  - Focus marketing efforts on high-value segments
  - Client retention strategies
- **Required parameters**:
  - Either `period_days` (integer, 1–5000) OR both `date_from` and `date_to`
- **Optional parameters**:
  - `period_days`: Number of days for analysis (default: 30)
  - `date_from`: Start date (`YYYY-MM-DD` format)
  - `date_to`: End date (`YYYY-MM-DD` format)
- **Output file columns**:
  - `client`: Client name or identifier
  - `total_revenue`: Total revenue from client (rounded to 2 decimals)
  - `orders_count`: Number of orders placed by client
  - `abc_category`: ABC category (A/B/C)
    - A: Clients generating up to 80% of revenue (top clients)
    - B: Clients generating 80–95% of revenue (middle segment)
    - C: Clients generating >95% of revenue (small clients)
  - `revenue_share`: Client's share of total revenue (%, rounded to 2 decimals)
  - `cumulative_share`: Cumulative revenue share (%, rounded to 2 decimals)
  - `avg_order_value`: Average order value for client (revenue/orders, rounded to 2 decimals)
- **How it works**:
  1. Load sales data for specified period
  2. Group by client and calculate total revenue and order count
  3. Sort clients by revenue (descending)
  4. Calculate revenue shares and cumulative percentages
  5. Assign ABC categories based on cumulative share thresholds

### 2. ABC Analysis — Goods (`abc_goods`)

- **Purpose**: Analyzes products/goods by their revenue contribution with ABC categorization. Identifies which products generate the most revenue for the business.
- **Business use cases**:
  - Identify best-selling products
  - Optimize inventory management
  - Focus marketing on high-revenue products
  - Product portfolio analysis
- **Required parameters**:
  - Either `period_days` (integer, 1–5000) OR both `date_from` and `date_to`
- **Optional parameters**:
  - `period_days`: Number of days for analysis (default: 30)
  - `date_from`: Start date (`YYYY-MM-DD` format)
  - `date_to`: End date (`YYYY-MM-DD` format)
- **Output file columns**:
  - `item`: Product/goods name or identifier
  - `total_revenue`: Total revenue from product (rounded to 2 decimals)
  - `orders_count`: Number of orders containing this product
  - `abc_category`: ABC category (A/B/C)
    - A: Products generating up to 80% of revenue
    - B: Products generating 80–95% of revenue
    - C: Products generating >95% of revenue
  - `revenue_share`: Product's share of total revenue (%, rounded to 2 decimals)
  - `cumulative_share`: Cumulative revenue share (%, rounded to 2 decimals)
- **How it works**:
  1. Load sales items data for specified period
  2. Group by product and calculate total revenue and order count
  3. Sort products by revenue (descending)
  4. Calculate revenue shares and cumulative percentages
  5. Assign ABC categories based on cumulative share thresholds

### 3. Average Check Analysis (`average_check`)

- **Purpose**: Analyzes average order value (AOV) across different dimensions. Provides insights into customer spending patterns and trends.
- **Business use cases**:
  - Monitor average order value trends
  - Compare client spending patterns
  - Analyze monthly revenue patterns
  - Identify high-value vs low-value customers
- **Required parameters**:
  - `dim`: Analysis dimension (string, required)
  - `date_from`: Start date (`YYYY-MM-DD` format, required)
  - `date_to`: End date (`YYYY-MM-DD` format, required)
- **Parameter options**:
  - `dim` values:
    - `overall`: Single aggregate across all orders
    - `client`: Breakdown by client
    - `month`: Monthly trend analysis
    - `client_month`: Client performance by month
- **Output file columns**:
  - `dimension`: The analysis dimension used
  - `client`: Client name (when dimension includes client)
  - `month`: Month (when dimension includes month)
  - `avg_check`: Average order value (mean of `total_sum`, rounded to 2 decimals)
  - `orders`: Number of orders
  - `revenue`: Total revenue (sum of `total_sum`, rounded to 2 decimals)
  - `note`: Additional notes (e.g., for invalid parameters)
- **How it works**:
  1. Load sales data for specified date range
  2. Add month column for time-based analysis
  3. Group data according to selected dimension
  4. Calculate average check, order count, and total revenue
  5. Sort appropriately based on dimension
- **Usage recommendations**:
  - Use `overall` for quick general overview
  - Use `client` to identify high/low AOV customers
  - Use `month` for trend analysis
  - Consider data volatility for small order volumes

### 4. New Customers Report (`new_customers`)

- **Purpose**: Identifies customers who made their first purchase within a specified period. Tracks customer acquisition effectiveness.
- **Business use cases**:
  - Monitor new customer acquisition
  - Evaluate marketing campaign effectiveness
  - Track business growth through new customers
  - Analyze first-purchase behavior
- **Required parameters**:
  - Either `period_days` (integer, 1–5000) OR both `date_from` and `date_to`
- **Optional parameters**:
  - `period_days`: Days back from today to search for first purchases (default: 7)
  - `date_from`: Start date for analysis (`YYYY-MM-DD` format)
  - `date_to`: End date for analysis (`YYYY-MM-DD` format)
- **Output file columns**:
  - `client`: Client name
  - `first_purchase`: Date and time of first purchase
  - `first_purchase_date`: Date of first purchase (date only)
  - `first_order_sum`: Amount of first order (rounded to 2 decimals)
  - `period_start`: Start of analysis period
  - `period_end`: End of analysis period
- **How it works**:
  1. Load all sales data
  2. Identify first purchase date for each client
  3. Filter clients whose first purchase falls within specified period
  4. Return list of new customers with their first purchase details
- **Usage notes**:
  - Suitable for operational monitoring (7–30 days)
  - Use specific dates for historical analysis
  - Period is inclusive of start date, exclusive of end date

### 5. Inactive Clients Report (`inactive_clients`)

- **Purpose**: Identifies clients who haven't made purchases for a specified period. Helps with customer retention and re-engagement efforts.
- **Business use cases**:
  - Identify clients at risk of churn
  - Target re-engagement campaigns
  - Monitor customer retention
  - Analyze customer lifecycle patterns
- **Required parameters**:
  - `cutoff_days`: Inactivity threshold in days (integer, 1–5000, required)
  - `start_date`: Beginning of sales tracking period in days from today (integer, 0–5000, required)
- **Parameter constraints**:
  - `start_date` MUST be greater than `cutoff_days`
- **Output file columns**:
  - `client`: Client name
  - `last_purchase`: Date of last purchase
  - `last_sum`: Amount of last order (rounded to 2 decimals)
  - `orders_count`: Total number of orders from client
  - `total_spent`: Total amount spent by client (rounded to 2 decimals)
  - `days_inactive`: Number of days since last purchase
- **How it works**:
  1. Load sales data starting from `start_date` days ago
  2. Find last purchase date for each client
  3. Identify clients whose last purchase was more than `cutoff_days` ago
  4. Calculate inactivity duration and spending statistics
  5. Sort by last purchase date
- **Usage notes**:
  - Example: `cutoff_days=60`, `start_date=180` finds clients inactive for 60+ days within last 180 days
  - Clients with no purchases in `start_date` period won't appear in report
  - Useful for targeted retention campaigns

### General Information

- **Data sources**:
  - Sales data from PostgreSQL database
  - Sales items data for product-level analysis
  - Data updated via HTTP API endpoints
- **Output format**:
  - All reports generate Excel (`.xlsx`) files
  - Files include proper headers and formatting
  - Monetary values rounded to 2 decimal places
  - Percentage values rounded to 2 decimal places
- **Parameter types**:
  - Integer parameters: Whole numbers within specified ranges
  - Date parameters: `YYYY-MM-DD` format
  - String parameters: Predefined allowed values for dimensions
- **Common parameter patterns**:
  - `period_days`: Recent period analysis (operational reports)
  - `date_from`/`date_to`: Historical period analysis (strategic reports)
  - Flexible date filtering allows both operational and historical analysis
- **System integration**:
  - Telegram bot interface for user interaction
  - Automated report generation and delivery
  - Parameter validation and error handling
  - Session management for multi-step parameter selection

This documentation covers all available reports in the Report Syrop system.
Each report serves specific business intelligence needs and can be customized through various parameters to provide targeted insights.

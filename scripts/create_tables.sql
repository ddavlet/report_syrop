-- PostgreSQL Tables for Sales Reporting System
-- This file creates the necessary tables for the sales reporting application

-- Enable UUID extension for generating unique IDs
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Clients table - stores customer information
CREATE TABLE IF NOT EXISTS clients (
    client_id TEXT PRIMARY KEY,
    client_name TEXT NOT NULL,
    email TEXT,
    phone TEXT,
    address TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Items table - stores product catalog
CREATE TABLE IF NOT EXISTS items (
    sku TEXT PRIMARY KEY,
    product_name TEXT NOT NULL,
    category TEXT,
    brand TEXT,
    description TEXT,
    unit_price NUMERIC(10,2),
    cost_price NUMERIC(10,2),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Sales table - stores sales transactions
CREATE TABLE IF NOT EXISTS sales (
    order_id TEXT PRIMARY KEY,
    client_id TEXT NOT NULL,
    date DATE NOT NULL,
    total_sum NUMERIC(10,2) NOT NULL,
    price_type TEXT NOT NULL,
    status TEXT DEFAULT 'confirmed',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (client_id) REFERENCES clients(client_id) ON DELETE RESTRICT
);

-- Sales items table - stores line items for each sale
CREATE TABLE IF NOT EXISTS sales_items (
    order_id TEXT NOT NULL,
    line_no INTEGER NOT NULL,
    sku TEXT NOT NULL,
    product_name TEXT,
    qty NUMERIC(10,3) NOT NULL,
    price NUMERIC(10,2) NOT NULL,
    total NUMERIC(10,2) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (order_id, line_no),
    FOREIGN KEY (order_id) REFERENCES sales(order_id) ON DELETE CASCADE,
    FOREIGN KEY (sku) REFERENCES items(sku) ON DELETE RESTRICT
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_sales_client_id ON sales(client_id);
CREATE INDEX IF NOT EXISTS idx_sales_date ON sales(date);
CREATE INDEX IF NOT EXISTS idx_sales_price_type ON sales(price_type);
CREATE INDEX IF NOT EXISTS idx_sales_items_sku ON sales_items(sku);
CREATE INDEX IF NOT EXISTS idx_clients_name ON clients(client_name);
CREATE INDEX IF NOT EXISTS idx_items_category ON items(category);
CREATE INDEX IF NOT EXISTS idx_items_brand ON items(brand);

-- Create updated_at trigger function
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create triggers to automatically update updated_at columns
CREATE TRIGGER update_clients_updated_at
    BEFORE UPDATE ON clients
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_items_updated_at
    BEFORE UPDATE ON items
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_sales_updated_at
    BEFORE UPDATE ON sales
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Add comments to tables and columns for documentation
COMMENT ON TABLE clients IS 'Stores customer/client information';
COMMENT ON TABLE items IS 'Stores product catalog information';
COMMENT ON TABLE sales IS 'Stores sales transaction records';
COMMENT ON TABLE sales_items IS 'Stores line items for each sale';

COMMENT ON COLUMN clients.client_id IS 'Unique identifier for the client';
COMMENT ON COLUMN clients.client_name IS 'Full name of the client';
COMMENT ON COLUMN items.sku IS 'Stock Keeping Unit - unique product identifier';
COMMENT ON COLUMN items.product_name IS 'Name of the product';
COMMENT ON COLUMN sales.order_id IS 'Unique order identifier';
COMMENT ON COLUMN sales.client_id IS 'Reference to the client who made the purchase';
COMMENT ON COLUMN sales.date IS 'Date of the sale';
COMMENT ON COLUMN sales.total_sum IS 'Total amount of the sale';
COMMENT ON COLUMN sales.price_type IS 'Type of pricing (retail, wholesale, etc.)';
COMMENT ON COLUMN sales_items.line_no IS 'Line number within the order';
COMMENT ON COLUMN sales_items.qty IS 'Quantity of the item sold';
COMMENT ON COLUMN sales_items.price IS 'Unit price of the item';
COMMENT ON COLUMN sales_items.total IS 'Total amount for this line item (qty * price)';

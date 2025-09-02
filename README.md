# Report Syrop - Telegram Bot for Sales Reports

A Telegram bot that generates various sales reports from your data. Reports are generated as Excel files and sent directly to users through the bot interface.

## Architecture

The system consists of two main services:

1. **Telegram Bot** (`src/bot.py`) - Handles user interactions and generates reports
2. **Data Loader Service** (`src/data_loader_service.py`) - HTTP API for receiving and loading sales data into PostgreSQL

## Features

- **Telegram Bot Interface**: All reports are accessed through a user-friendly Telegram bot
- **HTTP API**: POST endpoints for updating sales data and loading from JSON files
- **PostgreSQL Integration**: Automatic upsert functionality for data updates
- **Multiple Report Types**:
  - Average Check Analysis
  - Inactive Clients
  - New Customers
  - Purchase Frequency
  - ABC Analysis (Clients & Goods)
  - Declined Flavors
- **Interactive Parameter Selection**: Choose report parameters through inline keyboard buttons
- **Excel Export**: All reports are generated as Excel files with proper formatting

## Quick Start

### 1. Environment Setup

Create a `.env` file with your configuration:

```bash
# Telegram Bot Configuration
TELEGRAM_BOT_TOKEN=your_bot_token_here
TELEGRAM_CHAT_ID=your_chat_id_here
BOT_ALLOWED_USER_IDS=123456,789012

# Data Configuration
SALES_JSON_PATH=data/sales.json

# PostgreSQL
PG_DSN=postgresql+psycopg2://user:pass@localhost:5432/dbname
PG_TABLE=sales
```

### 2. Install Dependencies

```bash
make install
```

### 3. Run Services

#### Option A: Run both services locally
```bash
# Terminal 1: Start PostgreSQL
make up

# Terminal 2: Run data loader service
make data-loader

# Terminal 3: Run Telegram bot
make bot
```

#### Option B: Run with Docker
```bash
# Start all services
docker compose up

# Or start specific services
docker compose up db data_loader  # Start database and data loader
docker compose up app             # Start bot in another terminal
```

## HTTP API

The data loader service runs on port 8000 and provides the following endpoints:

### Update Sales Data

```bash
POST http://localhost:8000/update
Content-Type: application/json

[
  {
    "client": "Client Name",
    "date": "2024-01-01",
    "total_sum": 123.45,
    "price_type": "retail",
    "id": "ORD-001"
  }
]
```

### Load Data from JSON File

```bash
POST http://localhost:8000/load-json
Content-Type: application/json

{
  "json_path": "/path/to/sales.json"
}
```

### Health Check

```bash
GET http://localhost:8000/health (Forwarded to port 5500)
```

## Data Flow

1. **Data Ingestion**: External systems send sales data via HTTP POST to `/update`
2. **Data Validation**: Service validates data structure and required fields
3. **PostgreSQL Upsert**: Data is loaded into PostgreSQL using upsert (INSERT ... ON CONFLICT DO UPDATE)
4. **Backup**: Data is automatically backed up to timestamped JSON files
5. **Reports**: Users can generate reports through the Telegram bot interface

## Docker Deployment

### Start with PostgreSQL

```bash
make up
```

### Run all services

```bash
docker compose up
```

### Run specific services

```bash
# Data loader service (port 8000)
docker compose up data_loader

# Telegram bot
docker compose up app

# Database only
docker compose up db
```

### Stop all services

```bash
make down
```

## Report Parameters

Each report has configurable parameters accessible through the bot interface:

- **Average Check**: Dimension (overall/client/month/client_month), period days
- **Inactive Clients**: Cutoff days, start date (beginning of year, 90/180/365 days ago)
- **New Customers**: Period days
- **Purchase Frequency**: Minimum orders, period days
- **ABC Analysis**: Period days

## Data Format

The system expects sales data with the following structure:

```json
[
  {
    "client": "Client Name",
    "date": "2024-01-01",
    "total_sum": 123.45,
    "price_type": "retail",
    "id": "ORD-001"
  }
]
```

## Project Structure

```
src/
├── bot.py                    # Telegram bot service
├── data_loader_service.py    # HTTP API for data ingestion
├── core/                     # Core report framework
├── reports/                  # Individual report implementations
└── settings.py              # Configuration management
```

## Commands

- `/start` - Initialize the bot and show main menu
- Interactive buttons for report selection and parameter configuration

## Development

The bot uses an in-memory session system to track user parameter selections. All reports inherit from `BaseReport` and implement the `compute()` method.

The data loader service provides a RESTful API for data ingestion and automatically handles:
- Data validation
- PostgreSQL upserts
- Backup creation
- Error handling and logging

## License

This project is designed for internal business use.

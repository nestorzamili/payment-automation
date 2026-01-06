# Payment Reconciliation Automation

Automated payment reconciliation system that scrapes transaction data from multiple payment platforms, processes and reconciles the data, and syncs results to Google Sheets.

##  Features

- **Multi-Platform Scraping** - AXAI, Kira, M1 payment portals
- **Automated Parsing** - Transaction data extraction and normalization
- **Reconciliation** - Kira vs PG variance calculation
- **Settlement Tracking** - FPX and E-Wallet settlement dates with holiday awareness
- **Agent & Merchant Ledgers** - Balance tracking with carry-forward
- **Google Sheets Sync** - Real-time data sync via Apps Script triggers

## Documentation

Detailed documentation available in [`docs/`](docs/):

| Document | Description |
|----------|-------------|
| [Architecture](docs/architecture.md) | System architecture, components, database schema |
| [Data Flow](docs/data-flow.md) | Complete data flow from download to sync |
| [Scraper Flow](docs/scraper-flow.md) | Scraping process per platform |
| [Reconciliation Flow](docs/reconciliation-flow.md) | Kira vs PG variance calculation |
| [Settlement Flow](docs/settlement-flow.md) | Settlement date calculation with holidays |
| [Ledger Flow](docs/ledger-flow.md) | Merchant & Agent balance calculation |

## Project Structure

```
payment-reconciliation-automation/
├── server.py              # Flask app entry point
├── config/                # Configuration files
├── data/                  # Raw data and database
├── logs/                  # Log files
├── sessions/              # Scraper sessions
├── docs/                  # Documentation
├── src/
│   ├── core/              # Database, models, jobs, logger
│   ├── scrapers/          # Web scrapers (AXAI, Kira, M1)
│   ├── parser/            # Data parsers
│   ├── services/          # Business logic
│   ├── routes/            # API endpoints
│   └── utils/             # Utilities
└── app-script/            # Google Apps Script
```

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Configure settings
cp config/settings.example.json config/settings.json

# Run server
python server.py
# Server runs on http://127.0.0.1:5000
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/health` | GET | Health check |
| `/api/sync/download` | POST | Trigger download jobs |
| `/api/sync/parse` | POST | Trigger parse jobs |
| `/api/kira-pg/sync` | POST | Sync Kira PG sheet |
| `/api/deposit/sync` | POST | Sync Deposit sheet |
| `/api/merchant-ledger/sync` | POST | Sync Merchant Ledger |
| `/api/agent-ledger/sync` | POST | Sync Agent Ledger |
| `/api/summary/sync` | POST | Sync Summary sheet |
| `/api/parameter/sync` | POST | Sync Parameters |

## Google Sheets

| Sheet | Purpose |
|-------|---------|
| **Kira PG** | Kira vs PG transaction reconciliation |
| **Deposit** | Daily deposit tracking with settlement dates |
| **Merchants Ledger** | Merchant balance & settlement ledger |
| **Agents Ledger** | Agent commission & balance ledger |
| **Summary** | Yearly summary by merchant/agent |
| **Parameter** | Add-on holidays and configuration |
| **Jobs** | Job execution status tracking |

## License

Private - All rights reserved
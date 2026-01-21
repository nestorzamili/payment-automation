# System Architecture

System architecture for Payment Reconciliation Automation.

## 🏗️ High-Level Architecture

```mermaid
flowchart TB
    subgraph Client["👤 Client Layer"]
        GS["Google Sheets"]
        AS["Apps Script"]
    end

    subgraph Server["⚙️ Application Layer"]
        direction TB
        FLASK["Flask API Server"]
        
        subgraph Core["Core"]
            DB[("SQLite")]
            JOBS["Job Manager"]
            LOG["Logger"]
        end
    end

    subgraph Workers["🔧 Worker Layer"]
        direction LR
        SCRAPER["Scrapers"]
        PARSER["Parsers"]
        SERVICE["Services"]
    end

    subgraph External["🌐 External"]
        AXAI["AXAI Portal"]
        KIRA["Kira Portal"]
        M1["M1 Portal"]
    end

    AS <-->|"HTTP API"| FLASK
    FLASK --> JOBS
    FLASK --> SCRAPER
    FLASK --> PARSER
    FLASK --> SERVICE
    
    SCRAPER --> External
    PARSER --> DB
    SERVICE --> DB
    SERVICE --> GS
    
    style Client fill:#e3f2fd
    style Server fill:#fff3e0
    style Workers fill:#e8f5e9
    style External fill:#fce4ec
```

## 📦 Component Overview

### Core Layer
| Component | File | Responsibility |
|-----------|------|----------------|
| Database | `core/database.py` | SQLAlchemy session management |
| Models | `core/models.py` | Data models (Transaction, Deposit, Ledger, etc.) |
| Jobs | `core/jobs.py` | Job tracking and status management |
| Logger | `core/logger.py` | Centralized logging |
| Loader | `core/loader.py` | Configuration loading |

### Scraper Layer
| Component | File | Platform |
|-----------|------|----------|
| BaseScraper | `scrapers/base.py` | Abstract base class |
| AxaiScraper | `scrapers/axai.py` | AXAI payment portal |
| KiraScraper | `scrapers/kira.py` | Kira merchant portal |
| M1Scraper | `scrapers/m1.py` | M1 bank portal |
| Browser | `scrapers/browser.py` | Playwright browser management |

### Parser Layer
| Component | File | Data Type |
|-----------|------|-----------|
| AxaiParser | `parser/axai.py` | AXAI transaction files |
| KiraParser | `parser/kira.py` | Kira transaction files |
| M1Parser | `parser/m1.py` | M1 bank statement files |

### Service Layer
| Component | File | Responsibility |
|-----------|------|----------------|
| SheetsClient | `services/client.py` | Google Sheets API wrapper |
| KiraPGService | `services/kira_pg.py` | Kira vs PG reconciliation |
| DepositService | `services/deposit.py` | Deposit tracking |
| MerchantLedger | `services/merchant_ledger.py` | Merchant balance ledger |
| AgentLedger | `services/agent_ledger.py` | Agent commission ledger |
| SummaryService | `services/ledger_summary.py` | Yearly summary |
| ParameterService | `services/parameters.py` | Parameter management |

### Route Layer
| Endpoint | File | Description |
|----------|------|-------------|
| `/api/sync/*` | `routes/sync.py` | Download & parse triggers |
| `/api/kira-pg/*` | `routes/kira_pg.py` | Kira PG sync |
| `/api/deposit/*` | `routes/deposit.py` | Deposit sync |
| `/api/merchant-ledger/*` | `routes/merchant_ledger.py` | Merchant ledger sync |
| `/api/agent-ledger/*` | `routes/agent_ledger.py` | Agent ledger sync |
| `/api/summary/*` | `routes/ledger_summary.py` | Summary sync |

## 🗄️ Database Schema

```mermaid
erDiagram
    KiraTransaction ||--o{ KiraPG : "aggregated"
    PGTransaction ||--o{ KiraPG : "aggregated"
    Deposit ||--o{ MerchantLedger : "references"
    Deposit ||--o{ AgentLedger : "references"
    
    KiraTransaction {
        int id PK
        string merchant
        string transaction_date
        string payment_method
        float amount
        float settlement_amount
    }
    
    PGTransaction {
        int id PK
        string pg_account_label
        string transaction_date
        string channel
        float amount
    }
    
    KiraPG {
        int id PK
        string pg_account_label
        string transaction_date
        string channel
        float kira_amount
        float pg_amount
        float daily_variance
    }
    
    Deposit {
        int id PK
        string merchant
        string transaction_date
        float fpx_amount
        float ewallet_amount
        string fpx_settlement_date
        string ewallet_settlement_date
    }
    
    MerchantLedger {
        int id PK
        string merchant
        string transaction_date
        float settlement_fund
        float settlement_charges
        float withdrawal_amount
        float withdrawal_rate
        float withdrawal_charges
        float topup_payout_pool
        float available_fpx
        float available_ewallet
        float available_total
        float payout_pool_balance
        float available_balance
        float total_balance
    }
    
    AgentLedger {
        int id PK
        string merchant
        string transaction_date
        float commission_rate_fpx
        float commission_rate_ewallet
        float available_fpx
        float available_ewallet
        float available_total
        float volume
        float commission_rate
        float commission_amount
        float debit
        float balance
        float accumulative_balance
    }
```

## 🔐 Security

- Google Sheets access via Service Account
- Session-based scraper authentication
- SSH tunnel for remote database (optional)
- Credentials stored in `config/` (git-ignored)

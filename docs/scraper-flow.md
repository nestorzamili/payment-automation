# Scraper Flow

Detailed scraping process flow for each platform.

## 🌐 Scraper Architecture

```mermaid
classDiagram
    class BaseScraper {
        +label: str
        +platform: str
        +session_path: Path
        +download_data(from_date, to_date, job_id)
        +perform_login(page)
        +download_files(page, download_dir, from_date, to_date)
        #_try_with_existing_session()
        #_login_with_visible_browser()
    }

    class AxaiScraper {
        +fill_login_credentials(page)
        +navigate_to_payment_details(page)
        +select_transaction_status(page)
        +fill_date_range(page, from_date, to_date)
        +export_data(page, download_dir)
    }

    class KiraScraper {
        +perform_login(page)
        +download_files(page, download_dir, from_date, to_date)
    }

    class M1Scraper {
        +perform_login(page)
        +download_files(page, download_dir, from_date, to_date)
    }

    BaseScraper <|-- AxaiScraper
    BaseScraper <|-- KiraScraper
    BaseScraper <|-- M1Scraper
```

## 🔐 Session Management

```mermaid
flowchart TB
    START["Start Download"]
    CHECK{"Session exists?"}
    LOAD["Load session"]
    TEST{"Session valid?"}
    VISIBLE["Open visible browser"]
    MANUAL["Wait for manual login"]
    SAVE["Save session"]
    HEADLESS["Continue headless"]
    DOWNLOAD["Download files"]
    
    START --> CHECK
    CHECK -->|Yes| LOAD
    CHECK -->|No| VISIBLE
    LOAD --> TEST
    TEST -->|Yes| HEADLESS
    TEST -->|No| VISIBLE
    VISIBLE --> MANUAL
    MANUAL --> SAVE
    SAVE --> HEADLESS
    HEADLESS --> DOWNLOAD

    style VISIBLE fill:#fff3e0
    style MANUAL fill:#ffcdd2
```

## 📋 AXAI Scraper Flow

```mermaid
flowchart TB
    subgraph Login["🔐 Login"]
        L1["Navigate to login page"]
        L2["Fill username/password"]
        L3["Wait for CAPTCHA (manual)"]
        L4["Submit login"]
        L1 --> L2 --> L3 --> L4
    end

    subgraph Navigate["📍 Navigate"]
        N1["Open sidebar menu"]
        N2["Click Payment Details"]
        N3["Wait for page load"]
        N1 --> N2 --> N3
    end

    subgraph Filter["🔍 Set Filters"]
        F1["Select Success status"]
        F2["Select Fail status"]
        F3["Open From date picker"]
        F4["Select from date"]
        F5["Open To date picker"]
        F6["Select to date"]
        F1 --> F2 --> F3 --> F4 --> F5 --> F6
    end

    subgraph Export["📥 Export"]
        E1["Click Export button"]
        E2["Wait for download"]
        E3["Rename file"]
        E1 --> E2 --> E3
    end

    Login --> Navigate --> Filter --> Export
```

### AXAI Status Update Flow

```mermaid
stateDiagram-v2
    [*] --> running : Job created
    running --> waiting_manual_login : CAPTCHA detected
    waiting_manual_login --> running : Login successful
    running --> completed : Download finished
    running --> failed : Error occurred
    completed --> [*]
    failed --> [*]
```

## 📋 Kira Scraper Flow

```mermaid
flowchart TB
    subgraph Login["🔐 Login"]
        L1["Navigate to login"]
        L2["Fill credentials"]
        L3["Submit"]
        L1 --> L2 --> L3
    end

    subgraph Export["📥 Export"]
        E1["Navigate to reports"]
        E2["Set date range"]
        E3["Generate report"]
        E4["Download CSV"]
        E1 --> E2 --> E3 --> E4
    end

    Login --> Export
```

## 📋 M1 Scraper Flow

```mermaid
flowchart TB
    subgraph Login["🔐 Login"]
        L1["Navigate to bank portal"]
        L2["Fill credentials"]
        L3["2FA if required"]
        L1 --> L2 --> L3
    end

    subgraph Export["📥 Export"]
        E1["Navigate to statements"]
        E2["Select account"]
        E3["Set date range"]
        E4["Download statement"]
        E1 --> E2 --> E3 --> E4
    end

    Login --> Export
```

## 🗂️ File Naming Convention

Downloaded files are renamed to include date range:

```
{account_label}_{from_date}_{to_date}.csv

Examples:
- merchant_a_2026-01-01_2026-01-31.csv
- kira_2026-01-01_2026-01-31.xlsx
```

## ⚠️ Error Handling

| Error Type | Handling |
|------------|----------|
| Session expired | Open visible browser for re-login |
| CAPTCHA required | Update job status, wait for manual input |
| Download timeout | Retry with increased timeout |
| Network error | Log error, mark job as failed |

## 🔧 Configuration

```json
{
  "accounts": [
    {
      "label": "merchant_a",
      "platform": "axai",
      "credentials": {
        "email": "user@example.com",
        "password": "secret"
      }
    }
  ]
}
```

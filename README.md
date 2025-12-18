# Payment Reconciliation Automation

Automated payment reconciliation system that scrapes transaction data from KIRA, Payment Gateway (PG), and Bank portals, processes the data locally, and uploads results to Google Sheets.

## Features

- **Multi-Account Automation**: Sequential login and download from 12 accounts across KIRA/PG/Bank portals
- **Session Persistence**: Reuses sessions to avoid repeated logins
- **CAPTCHA Handling**: Pauses for manual CAPTCHA solving when needed
- **Timezone Aware**: All operations use Kuala Lumpur timezone (Asia/Kuala_Lumpur)
- **Local Processing**: Pandas-based data processing with local storage
- **Google Sheets Integration**: Uploads final results (Summary, Deposit, Ledgers) to spreadsheet
- **Trigger from Spreadsheet**: Custom button in Google Sheets to trigger the pipeline

## Architecture

```
Google Sheets (Apps Script)
    ↓ Button Click
Flask Server (localhost:5000)
    ↓ Trigger Pipeline
Python Backend
    ├── Download .xlsx files (Playwright)
    ├── Process data (Pandas)
    ├── Calculate results
    └── Upload to Sheets (Google Sheets API)
```

## Project Structure

```
payment-reconciliation-automation/
├── config/
│   ├── accounts.json              # Account credentials (12 accounts)
│   ├── settings.json              # Global settings
│   └── service-account.json       # Google Sheets API credentials
├── core/
│   ├── browser.py                 # Playwright with KL timezone/geolocation
│   ├── session_manager.py         # Session persistence logic
│   ├── logger.py                  # Structured logging
│   └── config_loader.py           # Configuration management
├── scrapers/
│   ├── base_scraper.py            # Base scraper class
│   ├── kira_scraper.py            # KIRA portal scraper
│   ├── pg_ragnarok_scraper.py     # Ragnarok PG scraper
│   ├── pg_m1pay_scraper.py        # M1pay PG scraper
│   ├── pg_rhb_scraper.py          # RHB PG scraper
│   └── bank_rhb_scraper.py        # RHB Bank scraper
├── processors/
│   ├── kira_processor.py          # Process KIRA transaction files
│   ├── pg_processor.py            # Process PG transaction files
│   ├── bank_processor.py          # Process Bank transaction files
│   ├── data_merger.py             # Merge KIRA+PG+Bank data
│   ├── holiday_processor.py       # Malaysia holidays & settlement dates
│   ├── deposit_processor.py       # Deposit calculations
│   ├── merchant_ledger.py         # Merchant ledger generation
│   └── agent_ledger.py            # Agent ledger generation
├── sheets/
│   ├── sheets_client.py           # Google Sheets API wrapper
│   └── parameter_loader.py        # Load parameters from Sheets
├── data/
│   ├── raw/                       # Downloaded .xlsx files
│   │   ├── kira/{account}/{date}/
│   │   ├── pg/{account}/{date}/
│   │   └── bank/{account}/{date}/
│   └── processed/                 # Intermediate processing results
├── sessions/
│   └── {account}/                 # Session cookies per account
│       └── cookies.json
├── logs/                          # Log files
│   └── {YYYY-MM-DD}_KL.log
├── server.py                      # Flask trigger server
├── main.py                        # Main pipeline orchestrator
├── requirements.txt               # Python dependencies
└── README.md
```

## Setup

### 1. Install Dependencies

```powershell
cd D:\Santai\2025\payment-reconciliation-automation
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
playwright install chromium
```

### 2. Configure Accounts

Edit `config/accounts.json` with your 12 account credentials:

```json
{
  "accounts": [
    {
      "id": 1,
      "name": "KIRA_Account_1",
      "platform": "kira",
      "email": "your-email@example.com",
      "password": "your-password",
      "url": "https://portal.example.com/login",
      "need_captcha": false,
      "enabled": true
    }
  ]
}
```

### 3. Setup Google Sheets API

1. Create a Google Cloud Project
2. Enable Google Sheets API
3. Create a Service Account
4. Download the JSON key file
5. Save as `config/service-account.json`
6. Share your spreadsheet with the service account email

### 4. Update Apps Script

Add the trigger button to your Google Sheets by updating the Apps Script Menu.gs file.

### 5. Start Flask Server

```powershell
.\venv\Scripts\Activate.ps1
python server.py
```

The server will run at `http://localhost:5000`

## Usage

### Manual Execution

```powershell
.\venv\Scripts\Activate.ps1
python main.py
```

### Trigger from Google Sheets

1. Open your reconciliation spreadsheet
2. Click **Import Tools** → **Automated Daily Import**
3. The script will:
   - Download today's data from all 12 accounts
   - Process locally
   - Upload results to Summary, Deposit, and Ledger sheets

### CAPTCHA Handling

When an account requires CAPTCHA (`need_captcha: true`):
1. The browser will pause and show the login page
2. Manually solve the CAPTCHA
3. Press Enter in the terminal to continue
4. Session will be saved for future use

## Configuration

### Browser Settings (`config/settings.json`)

- **Timezone**: Asia/Kuala_Lumpur (UTC+8)
- **Geolocation**: Kuala Lumpur (3.1390°N, 101.6869°E)
- **Locale**: English (en)
- **Headless**: false (visual mode for debugging)

### Date Range

- Downloads data for the current day (H-0) when triggered
- Date is based on Kuala Lumpur timezone

## Error Handling

- **Stop on First Error**: If any account fails, the entire pipeline stops
- **Detailed Logging**: All operations logged to `logs/{YYYY-MM-DD}_KL.log`
- **Session Recovery**: Invalid sessions are skipped (manual re-login required)

## Maintenance

### Session Cleanup

If a session is invalid:
1. Delete the session file: `sessions/{account}/cookies.json`
2. Re-run the script to perform fresh login

### Download Cleanup

Old downloads can be cleaned manually from `data/raw/` or set `cleanup_days` in settings.

## Troubleshooting

### Flask Server Not Responding

```powershell
# Check if server is running
netstat -ano | findstr :5000

# Restart server
python server.py
```

### Session Expired

Delete session file and re-login:
```powershell
Remove-Item sessions/KIRA_Account_1/cookies.json
```

### Download Failed

Check logs for specific error:
```powershell
Get-Content logs/(Get-Date -Format yyyy-MM-dd)_KL.log -Tail 50
```

## License

Private project - Internal use only

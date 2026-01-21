# Ledger Flow

Balance calculation for Merchant and Agent Ledger.

## 🏦 Merchant Ledger Overview

```mermaid
flowchart TB
    subgraph Input["📥 Input Sources"]
        DEP["Deposit Data"]
        MANUAL["Manual Inputs<br/>(Settlement, Withdrawal, Top-up)"]
    end

    subgraph Calculate["🧮 Calculate"]
        AVAIL["Available Balance"]
        PAYOUT["Payout Pool Balance"]
        TOTAL["Total Balance"]
    end

    subgraph Output["📊 Output"]
        SHEET["Merchant Ledger Sheet"]
    end

    DEP --> AVAIL
    MANUAL --> PAYOUT
    MANUAL --> AVAIL
    AVAIL --> TOTAL
    PAYOUT --> TOTAL
    TOTAL --> SHEET

    style Input fill:#e3f2fd
    style Calculate fill:#fff3e0
    style Output fill:#c8e6c9
```

## 💰 Available Balance Calculation

```mermaid
flowchart LR
    PREV["Previous<br/>Available Balance"]
    AVAIL["Available Total<br/>(from Deposit)"]
    SETTLE["Settlement Fund<br/>(manual input)"]
    CHARGE["Settlement Charges<br/>(manual input)"]
    RESULT["New<br/>Available Balance"]

    PREV -->|"+"| RESULT
    AVAIL -->|"+"| RESULT
    SETTLE -->|"-"| RESULT
    CHARGE -->|"-"| RESULT
```

**Formula:**
```
available_balance = prev_available_balance 
                  + available_total 
                  - settlement_fund 
                  - settlement_charges
```

## 💳 Payout Pool Balance Calculation

```mermaid
flowchart LR
    PREV["Previous<br/>Payout Pool"]
    WITHDRAW["Withdrawal Amount<br/>(manual input)"]
    WCHARGE["Withdrawal Charges<br/>(calculated)"]
    TOPUP["Top-up Payout Pool<br/>(manual input)"]
    RESULT["New<br/>Payout Pool Balance"]

    PREV -->|"+"| RESULT
    TOPUP -->|"+"| RESULT
    WITHDRAW -->|"-"| RESULT
    WCHARGE -->|"-"| RESULT
```

**Formula:**
```
payout_pool_balance = prev_payout_pool 
                    - withdrawal_amount 
                    - withdrawal_charges 
                    + topup_payout_pool

withdrawal_charges = withdrawal_amount × (withdrawal_rate / 100)
```

## 📊 Carry-Forward Optimization

Balance calculation now uses carry-forward from previous month:

```mermaid
flowchart TB
    subgraph PrevMonth["📅 Previous Month"]
        LAST["Last Day Record"]
        BAL1["Final Balance"]
    end

    subgraph CurrentMonth["📅 Current Month"]
        FIRST["First Day"]
        DAYS["... Days ..."]
        LASTCUR["Last Day"]
    end

    BAL1 -->|"Carry Forward"| FIRST
    FIRST --> DAYS --> LASTCUR
```

---

## 👤 Agent Ledger Overview

```mermaid
flowchart TB
    subgraph Input["📥 Input Sources"]
        KIRA["Kira FPX/Ewallet Amounts"]
        RATES["Commission Rates<br/>(manual input)"]
        VOL["Volume + Rate<br/>(manual input)"]
    end

    subgraph Calculate["🧮 Calculate"]
        GROSS["Gross Commission"]
        AVAIL["Available Commission"]
        COMM["Volume Commission"]
        BAL["Balance"]
    end

    subgraph Output["📊 Output"]
        SHEET["Agent Ledger Sheet"]
    end

    KIRA --> GROSS
    RATES --> GROSS
    RATES --> AVAIL
    VOL --> COMM
    GROSS --> BAL
    AVAIL --> BAL
    COMM --> BAL
    BAL --> SHEET

    style Input fill:#e3f2fd
    style Calculate fill:#fff3e0
    style Output fill:#c8e6c9
```

## 🧮 Commission Calculation

### Gross Commission (Daily)

```mermaid
flowchart LR
    FPX["FPX Amount"]
    RATE_FPX["FPX Rate %"]
    EW["E-Wallet Amount"]
    RATE_EW["E-Wallet Rate %"]
    GROSS["Gross Commission"]

    FPX -->|"× rate / 100"| GROSS
    RATE_FPX --> GROSS
    EW -->|"× rate / 100"| GROSS
    RATE_EW --> GROSS
```

**Formula:**
```
fpx_commission = fpx_amount × (rate_fpx / 100)
ewallet_commission = ewallet_amount × (rate_ewallet / 100)
gross = fpx_commission + ewallet_commission
```

### Available Commission (Settlement-based)

Available Commission is calculated by:
1. Taking commission from each transaction date (using rate at transaction date)
2. Aggregating those commissions by their settlement date

```mermaid
flowchart TB
    subgraph TxDate["📅 Transaction Date"]
        FPX["FPX Amount"]
        EW["E-Wallet Amount"]
        RATE["Commission Rate<br/>(at tx date)"]
        COMM["Commission"]
    end

    subgraph SettleDate["📅 Settlement Date"]
        AGG["Aggregate Commission"]
        AVAIL["Available Total"]
    end

    FPX -->|"× rate / 100"| COMM
    EW -->|"× rate / 100"| COMM
    RATE --> COMM
    COMM -->|"group by settlement date"| AGG
    AGG --> AVAIL
```

**Formula:**
```
For each deposit on transaction_date:
  fpx_commission = fpx_amount × (rate_fpx_at_tx_date / 100)
  ewallet_commission = ewallet_amount × (rate_ewallet_at_tx_date / 100)

Then aggregate by settlement_date:
  available_fpx = Σ fpx_commission (where fpx_settlement_date = current_date)
  available_ewallet = Σ ewallet_commission (where ewallet_settlement_date = current_date)
  available_total = available_fpx + available_ewallet
```

### Volume Commission

```mermaid
flowchart LR
    VOL["Volume<br/>(manual input)"]
    RATE["Commission Rate<br/>(manual input)"]
    COMM["Commission Amount"]

    VOL -->|"×"| COMM
    RATE --> COMM
```

**Formula:**
```
commission_amount = volume × commission_rate
```

## 💰 Balance Calculation

```mermaid
flowchart LR
    PREV["Previous Balance"]
    AVAIL["Available Total"]
    COMM["Commission Amount"]
    RESULT["New Balance"]

    PREV -->|"+"| RESULT
    AVAIL -->|"+"| RESULT
    COMM -->|"+"| RESULT
```

**Formula:**
```
balance = prev_balance + available_total + commission_amount
```

---

## 📋 Sheet Structures

### Merchant Ledger Sheet

| Column | Field | Editable |
|--------|-------|----------|
| A | ID | ❌ |
| B | Date | ❌ |
| C-E | FPX (Amount, Fee, Gross) | ❌ |
| F-H | E-Wallet (Amount, Fee, Gross) | ❌ |
| I-J | Total (Gross, Fee) | ❌ |
| K-M | Available (FPX, E-Wallet, Total) | ❌ |
| N | Settlement Fund | ✅ |
| O | Settlement Charges | ✅ |
| P | Withdrawal Amount | ✅ |
| Q | Withdrawal Rate | ✅ |
| R | Withdrawal Charges | ❌ (calculated) |
| S | Top-up Payout Pool | ✅ |
| T | Payout Pool Balance | ❌ (calculated) |
| U | Available Balance | ❌ (calculated) |
| V | Total Balance | ❌ (calculated) |
| X | Remarks | ✅ |

### Agent Ledger Sheet

| Column | Field | Editable |
|--------|-------|----------|
| A | ID | ❌ |
| B | Date | ❌ |
| C | FPX Rate (%) | ✅ |
| D | FPX Commission | ❌ (calculated) |
| E | E-Wallet Rate (%) | ✅ |
| F | E-Wallet Commission | ❌ (calculated) |
| G | Gross Amount | ❌ (calculated) |
| H | Available FPX | ❌ (calculated from tx date) |
| I | Available E-Wallet | ❌ (calculated from tx date) |
| J | Available Total | ❌ (calculated) |
| K | Volume | ✅ |
| L | Commission Rate | ✅ |
| M | Commission Amount | ❌ (calculated) |
| N | Debit | ✅ |
| O | Balance | ❌ (calculated) |
| P | Accumulative Balance | ❌ (calculated) |
| Q | Updated At | ❌ |

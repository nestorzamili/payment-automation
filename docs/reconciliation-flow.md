# Reconciliation Flow

Detailed Kira vs Payment Gateway transaction reconciliation flow.

## 🔄 Reconciliation Overview

```mermaid
flowchart LR
    KIRA["Kira Transactions"]
    PG["PG Transactions"]
    AGG["Aggregate by<br/>Date + Account + Channel"]
    COMPARE["Compare Amounts"]
    VARIANCE["Calculate Variance"]
    CUMULATIVE["Cumulative Variance"]
    DB[("KiraPG Table")]

    KIRA --> AGG
    PG --> AGG
    AGG --> COMPARE
    COMPARE --> VARIANCE
    VARIANCE --> CUMULATIVE
    CUMULATIVE --> DB

    style KIRA fill:#e3f2fd
    style PG fill:#fff3e0
    style VARIANCE fill:#ffcdd2
```

## 📊 Data Aggregation

### Kira Data Aggregation

```mermaid
flowchart TB
    RAW["Raw Kira Transactions"]
    GROUP["Group by:<br/>• PG Account Label<br/>• Transaction Date<br/>• Payment Method"]
    CAT["Categorize Channel<br/>(FPX or EWALLET)"]
    SUM["Sum:<br/>• Amount<br/>• MDR<br/>• Settlement Amount"]
    MAP["Kira Map"]

    RAW --> GROUP --> CAT --> SUM --> MAP
```

### PG Data Aggregation

```mermaid
flowchart TB
    RAW["Raw PG Transactions"]
    GROUP["Group by:<br/>• PG Account Label<br/>• Transaction Date<br/>• Channel"]
    CAT["Categorize Channel<br/>(FPX or EWALLET)"]
    SUM["Sum:<br/>• Amount<br/>• Volume"]
    MAP["PG Map"]

    RAW --> GROUP --> CAT --> SUM --> MAP
```

## 🧮 Variance Calculation

```mermaid
flowchart LR
    subgraph Input["Input"]
        KIRA_AMT["Kira Amount"]
        PG_AMT["PG Amount"]
    end

    subgraph Calculate["Calculate"]
        DAILY["Daily Variance<br/>= Kira - PG"]
        CUM["Cumulative Variance<br/>= Running Total"]
    end

    subgraph Output["Output"]
        REC["KiraPG Record"]
    end

    KIRA_AMT --> DAILY
    PG_AMT --> DAILY
    DAILY --> CUM
    CUM --> REC
```

### Formula

| Field | Formula |
|-------|---------|
| Daily Variance | `kira_amount - pg_amount` |
| Cumulative Variance | `sum(daily_variance)` ordered by date |
| Settlement Amount | `pg_amount - fees` |
| Fees | Based on fee_type and fee_rate |

## 📋 Fee Calculation

```mermaid
flowchart TB
    TYPE{"Fee Type?"}
    PCT["percentage:<br/>amount × (rate / 100)"]
    VOL["per_volume:<br/>volume × rate"]
    FLAT["flat:<br/>rate"]
    RESULT["Fees Amount"]

    TYPE -->|percentage| PCT
    TYPE -->|per_volume| VOL
    TYPE -->|flat| FLAT
    PCT --> RESULT
    VOL --> RESULT
    FLAT --> RESULT
```

## 🔃 Sync Flow Detail

```mermaid
sequenceDiagram
    participant GS as Google Sheets
    participant SV as KiraPGService
    participant DB as Database

    GS->>SV: sync_sheet()
    SV->>GS: Read period (B1)
    SV->>GS: Read manual inputs (A5:K200)
    
    loop Each Manual Input
        SV->>DB: Apply fee settings
        SV->>DB: Recalculate settlement
    end
    
    SV->>DB: Query KiraPG records
    SV->>SV: Recalculate cumulative variance
    
    SV->>GS: Clear data validation
    SV->>GS: Clear data range
    SV->>GS: Write records
    SV->>GS: Set dropdowns
```

## 📋 Sheet Structure - Kira PG

| Column | Field | Editable |
|--------|-------|----------|
| A | ID | ❌ |
| B | Transaction Date | ❌ |
| C | PG Account | ❌ |
| D | Channel | ❌ |
| E | Kira Amount | ❌ |
| F | MDR | ❌ |
| G | PG Amount | ❌ |
| H | Volume | ❌ |
| I | Settlement Rule | ✅ Dropdown |
| J | Settlement Date | ❌ (calculated) |
| K | Fee Type | ✅ Dropdown |
| L | Fee Rate | ✅ |
| M | Fees | ❌ (calculated) |
| N | Settlement Amount | ❌ (calculated) |
| O | Daily Variance | ❌ |
| P | Cumulative Variance | ❌ |

## ⚠️ Variance Alert

Variance indicates discrepancy between Kira and PG:
- **Positive**: Kira > PG (money missing from PG)
- **Negative**: PG > Kira (extra in PG)
- **Zero**: Perfectly matched ✅

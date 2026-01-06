# Data Flow

Data flow from download to Google Sheets sync.

## 🔄 Complete Data Flow

```mermaid
flowchart TB
    subgraph Phase1["1️⃣ DOWNLOAD PHASE"]
        direction LR
        TRIGGER1["Apps Script Trigger<br/>or Manual API Call"]
        SCRAPER["Scraper"]
        FILES["CSV/Excel Files"]
        
        TRIGGER1 --> SCRAPER
        SCRAPER --> FILES
    end

    subgraph Phase2["2️⃣ PARSE PHASE"]
        direction LR
        PARSER["Parser"]
        VALIDATE["Validate & Normalize"]
        DB1[("Database")]
        
        FILES --> PARSER
        PARSER --> VALIDATE
        VALIDATE --> DB1
    end

    subgraph Phase3["3️⃣ PROCESS PHASE"]
        direction LR
        INIT["Init Records"]
        RECONCILE["Reconciliation"]
        CALCULATE["Calculate Balances"]
        DB2[("Database")]
        
        DB1 --> INIT
        INIT --> RECONCILE
        RECONCILE --> CALCULATE
        CALCULATE --> DB2
    end

    subgraph Phase4["4️⃣ SYNC PHASE"]
        direction LR
        READ["Read Manual Inputs"]
        APPLY["Apply to DB"]
        WRITE["Write to Sheet"]
        GS["Google Sheets"]
        
        DB2 --> READ
        READ --> APPLY
        APPLY --> WRITE
        WRITE --> GS
    end

    Phase1 --> Phase2
    Phase2 --> Phase3
    Phase3 --> Phase4

    style Phase1 fill:#e3f2fd
    style Phase2 fill:#fff3e0
    style Phase3 fill:#e8f5e9
    style Phase4 fill:#fce4ec
```

## 📥 Download Phase Detail

```mermaid
sequenceDiagram
    participant AS as Apps Script
    participant API as Flask API
    participant JM as JobManager
    participant SC as Scraper
    participant FS as File System

    AS->>API: POST /api/sync/download
    API->>JM: Create download jobs
    
    loop Per Account
        JM->>SC: Start download job
        SC->>SC: Login (session/manual)
        SC->>SC: Navigate to reports
        SC->>SC: Set date range
        SC->>SC: Export data
        SC->>FS: Save CSV/Excel
        SC->>JM: Update job status
    end
    
    API->>AS: Return job status
```

## 📄 Parse Phase Detail

```mermaid
sequenceDiagram
    participant AS as Apps Script
    participant API as Flask API
    participant PS as Parser
    participant DB as Database

    AS->>API: POST /api/sync/parse
    API->>PS: Run parsers
    
    loop Per File
        PS->>PS: Read file
        PS->>PS: Extract date range
        PS->>PS: Normalize data
        PS->>DB: Upsert transactions
    end
    
    PS->>DB: Commit
    API->>AS: Return parse count
```

## 🔧 Process Phase Detail

Occurs when sheet sync is called:

```mermaid
flowchart LR
    subgraph KiraPG["Kira PG"]
        K1["Aggregate Kira by date"]
        K2["Aggregate PG by date"]
        K3["Calculate variance"]
        K1 --> K3
        K2 --> K3
    end

    subgraph Deposit["Deposit"]
        D1["Aggregate by merchant/date"]
        D2["Calculate fees"]
        D3["Calculate settlement dates"]
        D1 --> D2 --> D3
    end

    subgraph Ledger["Ledger"]
        L1["Get deposits"]
        L2["Apply manual inputs"]
        L3["Carry-forward balance"]
        L1 --> L2 --> L3
    end
```

## 🔄 Sync Phase Detail

```mermaid
sequenceDiagram
    participant GS as Google Sheets
    participant AS as Apps Script
    participant API as Flask API
    participant SV as Service
    participant DB as Database

    GS->>AS: User clicks "Update Data"
    AS->>API: POST /api/{sheet}/sync
    API->>SV: sync_sheet()
    
    SV->>GS: Read header (B1:B2)
    SV->>GS: Read manual inputs
    SV->>DB: Apply manual inputs
    SV->>DB: Recalculate balances
    SV->>DB: Query data
    SV->>GS: Clear old data
    SV->>GS: Write new data
    SV->>GS: Set dropdowns
    
    API->>AS: Return success
    AS->>GS: Show notification
```

## 📊 Data Transformation

| Stage | Input | Output |
|-------|-------|--------|
| Download | Web portal | CSV/Excel files |
| Parse | CSV/Excel files | KiraTransaction, PGTransaction |
| Process | Transactions | KiraPG, Deposit, Ledgers |
| Sync | Database records | Google Sheets rows |

const CONFIG = {
  BASE_URL: 'cloudflare-tunnel-url',
  API_KEY: 'api-key',
  MERCHANT_LEDGER: 'Merchants Balance & Settlement Ledger',
  AGENT_LEDGER: 'Agents Balance & Settlement Ledger',
  KIRA_PG_SHEET: 'Kira PG',
  DEPOSIT_SHEET: 'Deposit',
  SUMMARY_SHEET: 'Summary',
};

function syncSheet_(sheetConfigKey, endpoint, label) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(CONFIG[sheetConfigKey]);

  if (!sheet) {
    SpreadsheetApp.getUi().alert(`${label} sheet not found`);
    return;
  }

  const options = {
    method: 'POST',
    contentType: 'application/json',
    headers: { 'x-api-key': CONFIG.API_KEY },
    muteHttpExceptions: true,
  };

  try {
    const response = UrlFetchApp.fetch(`${CONFIG.BASE_URL}/${endpoint}`, options);
    const code = response.getResponseCode();

    if (code >= 500) {
      SpreadsheetApp.getUi().alert('Server error: ' + code);
      return;
    }

    const result = JSON.parse(response.getContentText());

    if (result.status === 'success') {
      // no alert on success
    } else {
      SpreadsheetApp.getUi().alert('Error: ' + (result.message || 'Unknown error'));
    }
  } catch (error) {
    SpreadsheetApp.getUi().alert('Error: ' + error.message);
  }
}

function updateKiraPG() {
  syncSheet_('KIRA_PG_SHEET', 'kira-pg', 'Kira PG');
}

function updateDeposit() {
  syncSheet_('DEPOSIT_SHEET', 'deposit', 'Deposit');
}

function updateMerchantLedger() {
  syncSheet_('MERCHANT_LEDGER', 'merchant-ledger', 'Merchant Ledger');
}

function updateAgentLedger() {
  syncSheet_('AGENT_LEDGER', 'agent-ledger', 'Agent Ledger');
}

function updateSummary() {
  syncSheet_('SUMMARY_SHEET', 'summary', 'Summary');
}

function syncData() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName('Jobs');

  if (!sheet) {
    SpreadsheetApp.getUi().alert('Jobs sheet not found');
    return;
  }

  const options = {
    method: 'POST',
    contentType: 'application/json',
    headers: { 'x-api-key': CONFIG.API_KEY },
    muteHttpExceptions: true,
  };

  try {
    const response = UrlFetchApp.fetch(`${CONFIG.BASE_URL}/sync-data`, options);
    const code = response.getResponseCode();

    if (code >= 500) {
      SpreadsheetApp.getUi().alert('Server error: ' + code);
      return;
    }

    if (code === 409) {
      SpreadsheetApp.getUi().alert('Sync already in progress');
      return;
    }

    const result = JSON.parse(response.getContentText());

    if (result.status === 'success') {
      SpreadsheetApp.getUi().alert('Sync started. Jobs sheet will update automatically.');
    } else {
      SpreadsheetApp.getUi().alert('Error: ' + (result.message || 'Unknown error'));
    }
  } catch (error) {
    SpreadsheetApp.getUi().alert('Error: ' + error.message);
  }
}

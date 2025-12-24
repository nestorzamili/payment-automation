function updateAgentLedger() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const ledgerSheet = ss.getSheetByName(CONFIG.AGENT_LEDGER);

  if (!ledgerSheet) {
    SpreadsheetApp.getUi().alert('Agent Ledger sheet not found');
    return;
  }

  const merchant = ledgerSheet.getRange('B1').getValue();
  const period = ledgerSheet.getRange('B2').getValue();

  if (!merchant || !period) {
    SpreadsheetApp.getUi().alert('Please select Merchant and Period');
    return;
  }

  const { year, month } = parsePeriod(period);
  if (!year || !month) {
    SpreadsheetApp.getUi().alert('Invalid period format');
    return;
  }

  const manualData = readAgentManualData(ledgerSheet);

  const payload = {
    merchant: merchant,
    year: year,
    month: month,
    manual_data: manualData,
  };

  const options = {
    method: 'POST',
    contentType: 'application/json',
    headers: { 'x-api-key': CONFIG.API_KEY },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true,
  };

  try {
    const response = UrlFetchApp.fetch(
      `${CONFIG.BASE_URL}/ledger/agent/update`,
      options,
    );

    const code = response.getResponseCode();
    if (code >= 500) {
      SpreadsheetApp.getUi().alert('Server error: ' + code);
      return;
    }

    const result = JSON.parse(response.getContentText());

    if (result.status === 'success' && result.data && result.data.data) {
      ledgerSheet.getRange('A5:M100').clearContent();

      const ledgerData = result.data.data;
      const rows = ledgerData.map((row) => [
        row.agent_ledger_id,
        row.transaction_date,
        row.commission_rate_fpx ?? '',
        row.fpx ?? '',
        row.commission_rate_ewallet ?? '',
        row.ewallet ?? '',
        row.gross_amount ?? '',
        row.available_settlement_fpx ?? '',
        row.available_settlement_ewallet ?? '',
        row.available_settlement_total ?? '',
        row.withdrawal_amount ?? '',
        row.balance ?? '',
        row.updated_at ?? '',
      ]);

      if (rows.length > 0) {
        ledgerSheet.getRange(5, 1, rows.length, 13).setValues(rows);
      }
    }
  } catch (error) {
    SpreadsheetApp.getUi().alert('Error: ' + error.message);
  }
}

function readAgentManualData(sheet) {
  const data = sheet.getRange('A5:M100').getValues();
  const manualData = [];

  for (let i = 0; i < data.length; i++) {
    const row = data[i];
    const id = row[0];
    if (!id) continue;

    manualData.push({
      id: id,
      commission_rate_fpx: row[2] !== '' ? row[2] : 'CLEAR',
      commission_rate_ewallet: row[4] !== '' ? row[4] : 'CLEAR',
      withdrawal_amount: row[10] !== '' ? row[10] : 'CLEAR',
    });
  }

  return manualData;
}

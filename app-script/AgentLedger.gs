function updateAgentLedger() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(CONFIG.AGENT_LEDGER);

  if (!sheet) {
    SpreadsheetApp.getUi().alert('Agent Ledger sheet not found');
    return;
  }

  const merchant = sheet.getRange('B1').getValue();
  const period = sheet.getRange('B2').getValue();

  if (!merchant || !period) {
    SpreadsheetApp.getUi().alert('Please select Merchant and Period');
    return;
  }

  const { year, month } = parsePeriod(period);
  if (!year || !month) {
    SpreadsheetApp.getUi().alert('Invalid period format');
    return;
  }

  const manualData = readAgentManualData(sheet);

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
      `${CONFIG.BASE_URL}/balance/agent/update`,
      options,
    );

    const code = response.getResponseCode();
    if (code >= 500) {
      SpreadsheetApp.getUi().alert('Server error: ' + code);
      return;
    }

    const result = JSON.parse(response.getContentText());

    if (result.status === 'success' && result.data && result.data.data) {
      sheet.getRange('A5:M50').clearContent();

      const data = result.data.data;
      const rows = data.map((row) => [
        row.id,
        row.transaction_date,
        row.commission_rate_fpx ?? '',
        row.fpx_commission ?? '',
        row.commission_rate_ewallet ?? '',
        row.ewallet_commission ?? '',
        row.gross_amount ?? '',
        row.available_fpx ?? '',
        row.available_ewallet ?? '',
        row.available_total ?? '',
        row.withdrawal_amount ?? '',
        row.balance ?? '',
        row.updated_at ?? '',
      ]);

      if (rows.length > 0) {
        sheet.getRange(5, 1, rows.length, 13).setValues(rows);
      }
    }
  } catch (error) {
    SpreadsheetApp.getUi().alert('Error: ' + error.message);
  }
}

function readAgentManualData(sheet) {
  const data = sheet.getRange('A5:M50').getValues();
  const manualData = [];

  for (let i = 0; i < data.length; i++) {
    const row = data[i];
    const id = row[0];
    if (!id) continue;

    const entry = { id: id };
    
    if (row[2] !== '') entry.commission_rate_fpx = row[2];
    if (row[4] !== '') entry.commission_rate_ewallet = row[4];
    if (row[10] !== '') entry.withdrawal_amount = row[10];
    
    manualData.push(entry);
  }

  return manualData;
}

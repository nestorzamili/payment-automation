function updateMerchantLedger() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const ledgerSheet = ss.getSheetByName(CONFIG.MERCHANT_LEDGER);

  if (!ledgerSheet) {
    SpreadsheetApp.getUi().alert('Ledger sheet not found');
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

  const manualData = readMerchantManualData(ledgerSheet);

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
      `${CONFIG.BASE_URL}/ledger/merchant/update`,
      options,
    );

    const code = response.getResponseCode();
    if (code >= 500) {
      SpreadsheetApp.getUi().alert('Server error: ' + code);
      return;
    }

    const result = JSON.parse(response.getContentText());

    if (result.status === 'success' && result.data && result.data.data) {
      ledgerSheet.getRange('A5:Z100').clearContent();

      const ledgerData = result.data.data;
      const rows = ledgerData.map((row) => [
        row.merchant_ledger_id,
        row.transaction_date,
        row.fpx ?? '',
        row.fee_fpx ?? '',
        row.gross_fpx ?? '',
        row.ewallet ?? '',
        row.fee_ewallet ?? '',
        row.gross_ewallet ?? '',
        row.total_gross ?? '',
        row.total_fee ?? '',
        row.available_settlement_amount_fpx ?? '',
        row.available_settlement_amount_ewallet ?? '',
        row.available_settlement_amount_total ?? '',
        row.settlement_fund ?? '',
        row.settlement_charges ?? '',
        row.withdrawal_amount ?? '',
        row.withdrawal_charges ?? '',
        row.topup_payout_pool ?? '',
        row.payout_pool_balance ?? '',
        row.available_balance ?? '',
        row.total_balance ?? '',
        row.updated_at ?? '',
        row.remarks ?? '',
      ]);

      if (rows.length > 0) {
        ledgerSheet.getRange(5, 1, rows.length, 23).setValues(rows);
      }
    }
  } catch (error) {
    SpreadsheetApp.getUi().alert('Error: ' + error.message);
  }
}

function readMerchantManualData(sheet) {
  const data = sheet.getRange('A5:W100').getValues();
  const manualData = [];

  for (let i = 0; i < data.length; i++) {
    const row = data[i];
    const id = row[0];
    if (!id) continue;

    manualData.push({
      id: id,
      settlement_fund: row[13] !== '' ? row[13] : 'CLEAR',
      settlement_charges: row[14] !== '' ? row[14] : 'CLEAR',
      withdrawal_amount: row[15] !== '' ? row[15] : 'CLEAR',
      topup_payout_pool: row[17] !== '' ? row[17] : 'CLEAR',
      remarks: row[22] !== '' ? row[22] : 'CLEAR',
    });
  }

  return manualData;
}

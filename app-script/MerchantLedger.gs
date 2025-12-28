function updateMerchantLedger() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(CONFIG.MERCHANT_LEDGER);

  if (!sheet) {
    SpreadsheetApp.getUi().alert('Merchant Ledger sheet not found');
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

  const manualData = readMerchantManualData(sheet);

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
      `${CONFIG.BASE_URL}/balance/merchant/update`,
      options,
    );

    const code = response.getResponseCode();
    if (code >= 500) {
      SpreadsheetApp.getUi().alert('Server error: ' + code);
      return;
    }

    const result = JSON.parse(response.getContentText());

    if (result.status === 'success' && result.data && result.data.data) {
      sheet.getRange('A5:X50').clearContent();

      const data = result.data.data;
      const rows = data.map((row) => [
        row.id,
        row.transaction_date,
        row.fpx_amount ?? '',
        row.fpx_fee ?? '',
        row.fpx_gross ?? '',
        row.ewallet_amount ?? '',
        row.ewallet_fee ?? '',
        row.ewallet_gross ?? '',
        row.total_gross ?? '',
        row.total_fee ?? '',
        row.available_fpx ?? '',
        row.available_ewallet ?? '',
        row.available_total ?? '',
        row.settlement_fund ?? '',
        row.settlement_charges ?? '',
        row.withdrawal_amount ?? '',
        row.withdrawal_rate ?? '',
        row.withdrawal_charges ?? '',
        row.topup_payout_pool ?? '',
        row.payout_pool_balance ?? '',
        row.available_balance ?? '',
        row.total_balance ?? '',
        row.updated_at ?? '',
        row.remarks ?? '',
      ]);

      if (rows.length > 0) {
        sheet.getRange(5, 1, rows.length, 24).setValues(rows);
      }
    }
  } catch (error) {
    SpreadsheetApp.getUi().alert('Error: ' + error.message);
  }
}

function readMerchantManualData(sheet) {
  const data = sheet.getRange('A5:X50').getValues();
  const manualData = [];

  for (let i = 0; i < data.length; i++) {
    const row = data[i];
    const id = row[0];
    if (!id) continue;

    const entry = { id: id };
    
    if (row[13] !== '') entry.settlement_fund = row[13];
    if (row[14] !== '') entry.settlement_charges = row[14];
    if (row[15] !== '') entry.withdrawal_amount = row[15];
    if (row[16] !== '') entry.withdrawal_rate = row[16];
    if (row[18] !== '') entry.topup_payout_pool = row[18];
    if (row[23] !== '') entry.remarks = row[23];
    
    manualData.push(entry);
  }

  return manualData;
}

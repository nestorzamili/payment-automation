function updateMerchantBalance() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(CONFIG.MERCHANT_LEDGER);

  if (!sheet) {
    SpreadsheetApp.getUi().alert('Merchant Balance sheet not found');
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
      sheet.getRange('A5:V100').clearContent();

      const data = result.data.data;
      const rows = data.map((row) => [
        row.merchant_balance_id,
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
        row.withdrawal_charges ?? '',
        row.topup_payout_pool ?? '',
        row.payout_pool_balance ?? '',
        row.available_balance ?? '',
        row.total_balance ?? '',
        row.remarks ?? '',
      ]);

      if (rows.length > 0) {
        sheet.getRange(5, 1, rows.length, 22).setValues(rows);
      }
    }
  } catch (error) {
    SpreadsheetApp.getUi().alert('Error: ' + error.message);
  }
}

function readMerchantManualData(sheet) {
  const data = sheet.getRange('A5:V100').getValues();
  const manualData = [];

  for (let i = 0; i < data.length; i++) {
    const row = data[i];
    const id = row[0];
    if (!id) continue;

    manualData.push({
      merchant_balance_id: id,
      settlement_fund: row[13] !== '' ? row[13] : 'CLEAR',
      settlement_charges: row[14] !== '' ? row[14] : 'CLEAR',
      withdrawal_amount: row[15] !== '' ? row[15] : 'CLEAR',
      topup_payout_pool: row[17] !== '' ? row[17] : 'CLEAR',
      remarks: row[21] !== '' ? row[21] : 'CLEAR',
    });
  }

  return manualData;
}

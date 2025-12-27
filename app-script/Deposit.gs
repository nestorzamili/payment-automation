function updateDeposit() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(CONFIG.DEPOSIT_SHEET);

  if (!sheet) {
    SpreadsheetApp.getUi().alert('Deposit sheet not found');
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

  const feeData = readDepositFeeData(sheet, merchant);

  const payload = {
    merchant: merchant,
    year: year,
    month: month,
    fee_data: feeData,
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
      `${CONFIG.BASE_URL}/deposit/update`,
      options,
    );

    const code = response.getResponseCode();
    if (code >= 500) {
      SpreadsheetApp.getUi().alert('Server error: ' + code);
      return;
    }

    const result = JSON.parse(response.getContentText());

    if (result.status === 'success' && result.data && result.data.data) {
      sheet.getRange('A7:U100').clearContent();

      const data = result.data.data;
      const rows = data.map((row) => [
        row.transaction_date ?? '',
        row.fpx_amount ?? '',
        row.fpx_volume ?? '',
        row.fpx_fee_type ?? '',
        row.fpx_fee_rate ?? '',
        row.fpx_fee_amount ?? '',
        row.fpx_gross ?? '',
        row.fpx_settlement_date ?? '',
        row.ewallet_amount ?? '',
        row.ewallet_volume ?? '',
        row.ewallet_fee_type ?? '',
        row.ewallet_fee_rate ?? '',
        row.ewallet_fee_amount ?? '',
        row.ewallet_gross ?? '',
        row.ewallet_settlement_date ?? '',
        row.total_amount ?? '',
        row.total_fees ?? '',
        row.available_fpx ?? '',
        row.available_ewallet ?? '',
        row.available_total ?? '',
        row.remarks ?? '',
      ]);

      if (rows.length > 0) {
        sheet.getRange(7, 1, rows.length, 21).setValues(rows);
        
        const feeTypeRule = SpreadsheetApp.newDataValidation()
          .requireValueInList(['percentage', 'per_volume', 'flat'], true)
          .setAllowInvalid(false)
          .build();
        
        sheet.getRange(7, 4, rows.length, 1).setDataValidation(feeTypeRule);
        sheet.getRange(7, 11, rows.length, 1).setDataValidation(feeTypeRule);
      }

      SpreadsheetApp.getUi().alert('Updated ' + rows.length + ' rows');
    }
  } catch (error) {
    SpreadsheetApp.getUi().alert('Error: ' + error.message);
  }
}

function readDepositFeeData(sheet, merchant) {
  const data = sheet.getRange('A7:U100').getValues();
  const feeData = [];

  for (let i = 0; i < data.length; i++) {
    const row = data[i];
    const transactionDate = row[0];

    if (!transactionDate) continue;

    if (row[3] !== '' || row[4] !== '') {
      feeData.push({
        merchant: merchant,
        transaction_date: transactionDate,
        channel: 'FPX',
        fee_type: row[3] !== '' ? row[3] : null,
        fee_rate: row[4] !== '' ? row[4] : null,
        remarks: row[20] !== '' ? row[20] : null,
      });
    }

    if (row[10] !== '' || row[11] !== '') {
      feeData.push({
        merchant: merchant,
        transaction_date: transactionDate,
        channel: 'EWALLET',
        fee_type: row[10] !== '' ? row[10] : null,
        fee_rate: row[11] !== '' ? row[11] : null,
        remarks: row[20] !== '' ? row[20] : null,
      });
    }
  }

  return feeData;
}

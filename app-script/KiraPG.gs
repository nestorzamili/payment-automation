function updateKiraPG() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(CONFIG.KIRA_PG_SHEET);

  if (!sheet) {
    SpreadsheetApp.getUi().alert('Kira PG sheet not found');
    return;
  }

  const period = sheet.getRange('B1').getValue();
  
  if (!period) {
    SpreadsheetApp.getUi().alert('Please select Period');
    return;
  }

  const { year, month } = parsePeriod(period);
  if (!year || !month) {
    SpreadsheetApp.getUi().alert('Invalid period format');
    return;
  }

  const manualData = readKiraPGManualData(sheet);

  const payload = {
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
      `${CONFIG.BASE_URL}/kira-pg`,
      options,
    );

    const code = response.getResponseCode();
    if (code >= 500) {
      SpreadsheetApp.getUi().alert('Server error: ' + code);
      return;
    }

    const result = JSON.parse(response.getContentText());

    if (result.status === 'success' && result.data && result.data.data) {
      sheet.getRange('A4:Q300').clearContent();
      sheet.getRange('A4:Q300').clearDataValidations();

      const data = result.data.data;
      const rows = data.map((row) => [
        row.pg_merchant ?? '',
        row.channel ?? '',
        row.kira_amount ?? '',
        row.mdr ?? '',
        row.kira_settlement_amount ?? '',
        row.pg_date ?? '',
        row.amount_pg ?? '',
        row.transaction_count ?? '',
        row.settlement_rule ?? '',
        row.settlement_date ?? '',
        row.fee_type ?? '',
        row.fee_rate ?? '',
        row.fees ?? '',
        row.settlement_amount ?? '',
        row.daily_variance ?? '',
        row.cumulative_variance ?? '',
        row.remarks ?? '',
      ]);

      if (rows.length > 0) {
        sheet.getRange(4, 1, rows.length, 17).setValues(rows);
        
        const feeTypeRule = SpreadsheetApp.newDataValidation()
          .requireValueInList(['percentage', 'flat'], true)
          .setAllowInvalid(false)
          .build();
        
        sheet.getRange(4, 11, rows.length, 1).setDataValidation(feeTypeRule);
      }
    }
  } catch (error) {
    SpreadsheetApp.getUi().alert('Error: ' + error.message);
  }
}

function readKiraPGManualData(sheet) {
  const data = sheet.getRange('A4:Q300').getValues();
  const manualData = [];

  for (let i = 0; i < data.length; i++) {
    const row = data[i];
    const pgMerchant = row[0];
    const channel = row[1];
    const pgDate = row[5];
    const feeType = row[10];
    const feeRate = row[11];
    const remarks = row[16];

    if (!pgDate || !pgMerchant) continue;
    
    if (feeType === '' && feeRate === '' && remarks === '') continue;

    manualData.push({
      pg_merchant: pgMerchant,
      pg_date: formatDate(pgDate),
      channel: channel,
      fee_type: feeType !== '' ? feeType : null,
      fee_rate: feeRate !== '' ? feeRate : null,
      remarks: remarks !== '' ? remarks : null,
    });
  }

  return manualData;
}

function parsePeriod(period) {
  if (period instanceof Date) {
    return {
      month: period.getMonth() + 1,
      year: period.getFullYear(),
    };
  }

  const match = String(period).match(/(\w+)\s+(\d{4})/);
  if (!match) return {};

  return {
    month: MONTHS[match[1]] || null,
    year: parseInt(match[2]),
  };
}

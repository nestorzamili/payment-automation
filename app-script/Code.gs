function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('Triggers')
    .addItem('Update Data', 'showDatePrompt')
    .addToUi();
}

function parsePeriod(period) {
  if (period instanceof Date) {
    return {
      month: period.getMonth() + 1,
      year: period.getFullYear(),
    };
  }

  const months = {
    Jan: 1,
    Feb: 2,
    Mar: 3,
    Apr: 4,
    May: 5,
    Jun: 6,
    Jul: 7,
    Aug: 8,
    Sep: 9,
    Oct: 10,
    Nov: 11,
    Dec: 12,
  };

  const match = String(period).match(/(\w+)\s+(\d{4})/);
  if (!match) return {};

  return {
    month: months[match[1]] || null,
    year: parseInt(match[2]),
  };
}

function showDatePrompt() {
  const ui = SpreadsheetApp.getUi();

  const fromResponse = ui.prompt('Enter From Date (YYYY-MM-DD):');
  if (fromResponse.getSelectedButton() !== ui.Button.OK) return;

  const toResponse = ui.prompt('Enter To Date (YYYY-MM-DD):');
  if (toResponse.getSelectedButton() !== ui.Button.OK) return;

  const result = updateData(
    fromResponse.getResponseText(),
    toResponse.getResponseText(),
  );
  ui.alert(JSON.stringify(result, null, 2));
}

function updateData(fromDate, toDate) {
  const options = {
    method: 'POST',
    contentType: 'application/json',
    headers: { 'x-api-key': CONFIG.API_KEY },
    payload: JSON.stringify({ from_date: fromDate, to_date: toDate }),
    muteHttpExceptions: true,
  };

  try {
    const response = UrlFetchApp.fetch(
      `${CONFIG.BASE_URL}/sheets/update`,
      options,
    );
    return JSON.parse(response.getContentText());
  } catch (error) {
    return { status: 'error', message: error.message };
  }
}

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

  const manualData = readManualData(ledgerSheet);

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
        row.fpx,
        row.fee_fpx,
        row.gross_fpx,
        row.ewallet,
        row.fee_ewallet,
        row.gross_ewallet,
        row.total_gross,
        row.total_fee,
        row.cum_fpx,
        row.cum_ewallet,
        row.cum_total,
        row.settlement_fund,
        row.settlement_charges,
        row.withdrawal_amount,
        row.withdrawal_charges,
        row.topup_payout_pool,
        row.payout_pool_balance,
        row.available_balance,
        row.total_balance,
        row.updated_at,
        row.remarks,
      ]);

      if (rows.length > 0) {
        ledgerSheet.getRange(5, 1, rows.length, 23).setValues(rows);
      }
    }
  } catch (error) {
    SpreadsheetApp.getUi().alert('Error: ' + error.message);
  }
}

function formatDate(value) {
  if (value instanceof Date) {
    const year = value.getFullYear();
    const month = String(value.getMonth() + 1).padStart(2, '0');
    const day = String(value.getDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
  }
  return String(value);
}

function readManualData(sheet) {
  const data = sheet.getRange('A5:W100').getValues();
  const manualData = [];

  for (let i = 0; i < data.length; i++) {
    const row = data[i];
    const id = row[0];
    if (!id) continue;

    manualData.push({
      id: id,
      settlement_fund: row[13] || null,
      settlement_charges: row[14] || null,
      withdrawal_amount: row[15] || null,
      withdrawal_charges: row[16] || null,
      topup_payout_pool: row[17] || null,
      payout_pool_balance: row[18] || null,
      available_balance: row[19] || null,
      total_balance: row[20] || null,
      remarks: row[22] || null,
    });
  }

  return manualData;
}

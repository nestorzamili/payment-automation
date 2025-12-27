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

function formatDate(value) {
  if (value instanceof Date) {
    const year = value.getFullYear();
    const month = String(value.getMonth() + 1).padStart(2, '0');
    const day = String(value.getDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
  }
  return String(value);
}

function setupMerchantDropdowns() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  
  const options = {
    method: 'GET',
    headers: { 'x-api-key': CONFIG.API_KEY },
    muteHttpExceptions: true,
  };

  try {
    const merchantResponse = UrlFetchApp.fetch(
      `${CONFIG.BASE_URL}/balance/merchant/merchants`,
      options,
    );
    const merchantResult = JSON.parse(merchantResponse.getContentText());

    const periodResponse = UrlFetchApp.fetch(
      `${CONFIG.BASE_URL}/balance/merchant/periods`,
      options,
    );
    const periodResult = JSON.parse(periodResponse.getContentText());

    if (merchantResult.status !== 'success' || !merchantResult.data) {
      SpreadsheetApp.getUi().alert('Failed to get merchants');
      return;
    }

    const merchants = merchantResult.data.merchants || [];
    const periods = periodResult.data?.periods || [];

    const sheetsToUpdate = [
      CONFIG.DEPOSIT_SHEET,
      CONFIG.MERCHANT_LEDGER,
      CONFIG.AGENT_LEDGER,
    ];

    const merchantRule = SpreadsheetApp.newDataValidation()
      .requireValueInList(merchants, true)
      .setAllowInvalid(false)
      .build();

    const periodRule = periods.length > 0 
      ? SpreadsheetApp.newDataValidation()
          .requireValueInList(periods, true)
          .setAllowInvalid(false)
          .build()
      : null;

    for (const sheetName of sheetsToUpdate) {
      const sheet = ss.getSheetByName(sheetName);
      if (sheet) {
        sheet.getRange('B1').setDataValidation(merchantRule);
        if (periodRule) {
          sheet.getRange('B2').setDataValidation(periodRule);
        }
      }
    }

    SpreadsheetApp.getUi().alert(
      'Dropdowns updated:\n' + 
      merchants.length + ' merchants\n' + 
      periods.length + ' periods'
    );
  } catch (error) {
    SpreadsheetApp.getUi().alert('Error: ' + error.message);
  }
}

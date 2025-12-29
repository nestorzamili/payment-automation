function updateParameter() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(CONFIG.PARAMETER_SHEET);

  if (!sheet) {
    SpreadsheetApp.getUi().alert('Parameter sheet not found');
    return;
  }

  const data = readParameterData(sheet);

  const options = {
    method: 'POST',
    contentType: 'application/json',
    headers: { 'x-api-key': CONFIG.API_KEY },
    payload: JSON.stringify(data),
    muteHttpExceptions: true,
  };

  try {
    const response = UrlFetchApp.fetch(
      `${CONFIG.BASE_URL}/parameter`,
      options,
    );

    const code = response.getResponseCode();
    if (code >= 500) {
      SpreadsheetApp.getUi().alert('Server error: ' + code);
      return;
    }

    const result = JSON.parse(response.getContentText());

    if (result.status === 'success') {
      SpreadsheetApp.getUi().alert('Parameters updated successfully');
    } else {
      SpreadsheetApp.getUi().alert('Failed: ' + (result.message || 'Unknown error'));
    }
  } catch (error) {
    SpreadsheetApp.getUi().alert('Error: ' + error.message);
  }
}

function readParameterData(sheet) {
  const data = sheet.getDataRange().getValues();
  
  const settlementRules = {};
  const addOnHolidays = [];
  
  let headerRowIdx = -1;
  for (let i = 0; i < data.length; i++) {
    if (data[i][0] === 'Type' && data[i][1] === 'Key') {
      headerRowIdx = i;
      break;
    }
  }
  
  if (headerRowIdx === -1) {
    return { settlement_rules: {}, add_on_holidays: [] };
  }
  
  for (let i = headerRowIdx + 1; i < data.length; i++) {
    const row = data[i];
    const paramType = String(row[0] || '').trim();
    const paramKey = String(row[1] || '').trim();
    const paramValue = String(row[2] || '').trim();
    const paramDesc = String(row[3] || '').trim();
    
    if (!paramType || !paramKey) continue;
    
    if (paramType === 'SETTLEMENT_RULES') {
      settlementRules[paramKey.toLowerCase()] = paramValue;
    } else if (paramType === 'ADD_ON_HOLIDAYS') {
      addOnHolidays.push({
        date: paramKey,
        description: paramDesc,
      });
    }
  }
  
  return {
    settlement_rules: settlementRules,
    add_on_holidays: addOnHolidays,
  };
}

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
  
  let currentSection = null;
  
  for (let i = 0; i < data.length; i++) {
    const row = data[i];
    const firstCell = String(row[0]).trim();
    
    if (firstCell.startsWith('SECTION:')) {
      currentSection = firstCell.replace('SECTION:', '').trim();
      continue;
    }
    
    if (!firstCell || firstCell === '') {
      currentSection = null;
      continue;
    }
    
    if (currentSection === 'SETTLEMENT_RULES') {
      const channel = firstCell;
      const rule = String(row[1]).trim();
      if (channel && rule) {
        settlementRules[channel.toLowerCase()] = rule;
      }
    } else if (currentSection === 'ADD_ON_HOLIDAYS') {
      if (firstCell === 'Date') continue;
      
      const dateVal = row[0];
      const description = String(row[1] || '').trim();
      
      if (dateVal) {
        addOnHolidays.push({
          date: formatDate(dateVal),
          description: description,
        });
      }
    }
  }
  
  return {
    settlement_rules: settlementRules,
    add_on_holidays: addOnHolidays,
  };
}

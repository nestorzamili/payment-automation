function syncData() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName('Jobs');

  if (!sheet) {
    SpreadsheetApp.getUi().alert('Jobs sheet not found');
    return;
  }

  const options = {
    method: 'POST',
    contentType: 'application/json',
    headers: { 'x-api-key': CONFIG.API_KEY },
    muteHttpExceptions: true,
  };

  try {
    const response = UrlFetchApp.fetch(
      `${CONFIG.BASE_URL}/sync-data`,
      options,
    );

    const code = response.getResponseCode();
    if (code >= 500) {
      SpreadsheetApp.getUi().alert('Server error: ' + code);
      return;
    }

    if (code === 409) {
      SpreadsheetApp.getUi().alert('Sync already in progress');
      return;
    }

    const result = JSON.parse(response.getContentText());

    if (result.status === 'success') {
      SpreadsheetApp.getUi().alert('Sync started. Jobs sheet will update automatically.');
    } else {
      SpreadsheetApp.getUi().alert('Error: ' + (result.message || 'Unknown error'));
    }
  } catch (error) {
    SpreadsheetApp.getUi().alert('Error: ' + error.message);
  }
}

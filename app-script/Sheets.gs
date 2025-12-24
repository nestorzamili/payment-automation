function updateSheets() {
  const options = {
    method: 'POST',
    contentType: 'application/json',
    headers: { 'x-api-key': CONFIG.API_KEY },
    muteHttpExceptions: true,
  };

  try {
    const response = UrlFetchApp.fetch(
      `${CONFIG.BASE_URL}/sheets/update`,
      options,
    );
    
    const result = JSON.parse(response.getContentText());
    
    if (result.status === 'success' && result.data) {
      SpreadsheetApp.getUi().alert(result.data.message);
    } else {
      SpreadsheetApp.getUi().alert('Error: ' + (result.message || 'Unknown error'));
    }
  } catch (error) {
    SpreadsheetApp.getUi().alert('Error: ' + error.message);
  }
}

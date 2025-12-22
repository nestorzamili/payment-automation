function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('Triggers')
    .addItem('Generate Summary', 'showDatePrompt')
    .addToUi();
}

function showDatePrompt() {
  const ui = SpreadsheetApp.getUi();

  const fromResponse = ui.prompt('Enter From Date (YYYY-MM-DD):');
  if (fromResponse.getSelectedButton() !== ui.Button.OK) return;

  const toResponse = ui.prompt('Enter To Date (YYYY-MM-DD):');
  if (toResponse.getSelectedButton() !== ui.Button.OK) return;

  const result = generateSummary(
    fromResponse.getResponseText(),
    toResponse.getResponseText(),
  );
  ui.alert(JSON.stringify(result, null, 2));
}

function generateSummary(fromDate, toDate) {
  const options = {
    method: 'POST',
    contentType: 'application/json',
    headers: { 'x-api-key': CONFIG.API_KEY },
    payload: JSON.stringify({ from_date: fromDate, to_date: toDate }),
    muteHttpExceptions: true,
  };

  try {
    const response = UrlFetchApp.fetch(
      `${CONFIG.BASE_URL}/summary/generate`,
      options,
    );
    return JSON.parse(response.getContentText());
  } catch (error) {
    return { status: 'error', message: error.message };
  }
}

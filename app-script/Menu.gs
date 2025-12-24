function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('Triggers')
    .addItem('Update Sheets', 'updateSheets')
    .addToUi();
}

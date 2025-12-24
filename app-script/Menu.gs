function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('Triggers')
    .addItem('Update Data', 'showDatePrompt')
    .addToUi();
}

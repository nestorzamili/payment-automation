function updateSummary() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const summarySheet = ss.getSheetByName(CONFIG.SUMMARY_SHEET);

  if (!summarySheet) {
    SpreadsheetApp.getUi().alert('Summary sheet not found');
    return;
  }

  const year = summarySheet.getRange('B1').getValue();
  const viewType = summarySheet.getRange('B2').getValue();

  if (!year || !viewType) {
    SpreadsheetApp.getUi().alert('Please select Year and View Type');
    return;
  }

  const payload = {
    year: parseInt(year),
    view_type: viewType,
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
      `${CONFIG.BASE_URL}/summary`,
      options,
    );

    const code = response.getResponseCode();
    if (code >= 500) {
      SpreadsheetApp.getUi().alert('Server error: ' + code);
      return;
    }

    const result = JSON.parse(response.getContentText());

    if (result.status === 'success' && result.data && result.data.data) {
      const dataRange = summarySheet.getRange('A5:N200');
      dataRange.clearContent();
      dataRange.setBackground(null);
      dataRange.setFontWeight('normal');

      const data = result.data.data;
      const merchants = data.merchants;
      const merchantData = data.data;
      const monthlyTotals = data.monthly_totals;

      const rows = [];
      for (const merchant of merchants) {
        const mData = merchantData[merchant];
        rows.push([
          merchant,
          mData['1'] || 0,
          mData['2'] || 0,
          mData['3'] || 0,
          mData['4'] || 0,
          mData['5'] || 0,
          mData['6'] || 0,
          mData['7'] || 0,
          mData['8'] || 0,
          mData['9'] || 0,
          mData['10'] || 0,
          mData['11'] || 0,
          mData['12'] || 0,
          mData['total'] || 0,
        ]);
      }

      const grandTotal = monthlyTotals['grand_total'] || 0;
      
      if (grandTotal !== 0) {
        rows.push([
          'Total Deposit',
          monthlyTotals['1'] || 0,
          monthlyTotals['2'] || 0,
          monthlyTotals['3'] || 0,
          monthlyTotals['4'] || 0,
          monthlyTotals['5'] || 0,
          monthlyTotals['6'] || 0,
          monthlyTotals['7'] || 0,
          monthlyTotals['8'] || 0,
          monthlyTotals['9'] || 0,
          monthlyTotals['10'] || 0,
          monthlyTotals['11'] || 0,
          monthlyTotals['12'] || 0,
          grandTotal,
        ]);
      }

      if (rows.length > 0) {
        summarySheet.getRange(5, 1, rows.length, 14).setValues(rows);
        
        if (grandTotal !== 0) {
          const totalRow = 5 + rows.length - 1;
          const totalRange = summarySheet.getRange(totalRow, 1, 1, 14);
          totalRange.setFontWeight('bold');
          totalRange.setBackground('#d9d9d9');
        }
      }
    }
  } catch (error) {
    SpreadsheetApp.getUi().alert('Error: ' + error.message);
  }
}

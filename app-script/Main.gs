const CONFIG = {
  BASE_URL: 'cloudflare-tunnel-url',
  API_KEY: 'api-key',
  MERCHANT_LEDGER: 'Merchants Balance & Settlement Ledger',
  AGENT_LEDGER: 'Agents Balance & Settlement Ledger',
  KIRA_PG_SHEET: 'Kira PG',
  DEPOSIT_SHEET: 'Deposit',
  SUMMARY_SHEET: 'Summary',
  ACCOUNTS_SHEET: 'Accounts',
};

function syncSheet_(sheetConfigKey, endpoint, label) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(CONFIG[sheetConfigKey]);

  if (!sheet) {
    SpreadsheetApp.getUi().alert(`${label} sheet not found`);
    return;
  }

  const options = {
    method: 'POST',
    contentType: 'application/json',
    headers: { 'x-api-key': CONFIG.API_KEY },
    muteHttpExceptions: true,
  };

  try {
    const response = UrlFetchApp.fetch(`${CONFIG.BASE_URL}/${endpoint}`, options);
    const code = response.getResponseCode();

    if (code >= 500) {
      SpreadsheetApp.getUi().alert('Server error: ' + code);
      return;
    }

    const result = JSON.parse(response.getContentText());

    if (result.status === 'success') {
      // no alert on success
    } else {
      SpreadsheetApp.getUi().alert('Error: ' + (result.message || 'Unknown error'));
    }
  } catch (error) {
    SpreadsheetApp.getUi().alert('Error: ' + error.message);
  }
}

function updateKiraPG() {
  syncSheet_('KIRA_PG_SHEET', 'kira-pg', 'Kira PG');
}

function updateDeposit() {
  syncSheet_('DEPOSIT_SHEET', 'deposit', 'Deposit');
}

function updateMerchantLedger() {
  syncSheet_('MERCHANT_LEDGER', 'merchant-ledger', 'Merchant Ledger');
}

function updateAgentLedger() {
  syncSheet_('AGENT_LEDGER', 'agent-ledger', 'Agent Ledger');
}

function updateSummary() {
  syncSheet_('SUMMARY_SHEET', 'summary', 'Summary');
}

function updateParameter() {
  const options = {
    method: 'POST',
    contentType: 'application/json',
    headers: { 'x-api-key': CONFIG.API_KEY },
    muteHttpExceptions: true,
  };

  try {
    const response = UrlFetchApp.fetch(`${CONFIG.BASE_URL}/parameter`, options);
    const code = response.getResponseCode();

    if (code >= 500) {
      SpreadsheetApp.getUi().alert('Server error: ' + code);
      return;
    }

    const result = JSON.parse(response.getContentText());

    if (result.status === 'success') {
      SpreadsheetApp.getUi().alert('Parameters updated successfully');
    } else {
      SpreadsheetApp.getUi().alert('Error: ' + (result.message || 'Unknown error'));
    }
  } catch (error) {
    SpreadsheetApp.getUi().alert('Error: ' + error.message);
  }
}

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
    const response = UrlFetchApp.fetch(`${CONFIG.BASE_URL}/sync-data`, options);
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

function refreshAccounts() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sheet = ss.getSheetByName(CONFIG.ACCOUNTS_SHEET);
  
  if (!sheet) {
    sheet = ss.insertSheet(CONFIG.ACCOUNTS_SHEET);
    sheet.getRange('A3:D3').setValues([['account_id', 'platform', 'label', 'active']]);
    sheet.getRange('A3:D3').setFontWeight('bold');
  }

  const options = {
    method: 'GET',
    contentType: 'application/json',
    headers: { 'x-api-key': CONFIG.API_KEY },
    muteHttpExceptions: true,
  };

  try {
    const response = UrlFetchApp.fetch(`${CONFIG.BASE_URL}/api/accounts`, options);
    const result = JSON.parse(response.getContentText());

    if (result.success) {
      const lastRow = sheet.getLastRow();
      if (lastRow >= 4) {
        sheet.getRange(4, 1, lastRow - 3, 4).clearContent();
      }

      const rows = result.data.map(acc => [
        acc.account_id,
        acc.platform,
        acc.label,
        acc.is_active ? 'Yes' : 'No'
      ]);

      if (rows.length > 0) {
        sheet.getRange(4, 1, rows.length, 4).setValues(rows);
      }
    } else {
      SpreadsheetApp.getUi().alert('Error: ' + (result.error || 'Unknown error'));
    }
  } catch (error) {
    SpreadsheetApp.getUi().alert('Error: ' + error.message);
  }
}

function showAddAccountDialog() {
  const html = HtmlService.createHtmlOutput(getAddAccountFormHtml_())
    .setWidth(400)
    .setHeight(380);
  SpreadsheetApp.getUi().showModalDialog(html, 'Add Account');
}

function getAddAccountFormHtml_() {
  return `
    <!DOCTYPE html>
    <html>
    <head>
      <style>
        body { font-family: Arial, sans-serif; padding: 15px; }
        .form-group { margin-bottom: 12px; }
        label { display: block; margin-bottom: 4px; font-weight: bold; }
        input, select { width: 100%; padding: 8px; box-sizing: border-box; }
        .checkbox-group { display: flex; align-items: center; gap: 8px; }
        .checkbox-group input { width: auto; }
        .buttons { margin-top: 20px; text-align: right; }
        button { padding: 8px 16px; margin-left: 8px; cursor: pointer; }
        .primary { background: #4285f4; color: white; border: none; border-radius: 4px; }
        .hidden { display: none; }
      </style>
    </head>
    <body>
      <form id="accountForm">
        <div class="form-group">
          <label>Platform *</label>
          <select id="platform" required onchange="updateCredentialFields()">
            <option value="">Select Platform</option>
            <option value="kira">Kira</option>
            <option value="axai">Axai</option>
            <option value="m1">M1</option>
            <option value="fiuu">Fiuu</option>
          </select>
        </div>
        
        <div class="form-group">
          <label>Label *</label>
          <input type="text" id="label" placeholder="e.g. infinetix-axai" required>
        </div>
        
        <div class="form-group hidden" id="username_group">
          <label id="username_label">Username</label>
          <input type="text" id="cred_username">
        </div>
        
        <div class="form-group hidden" id="password_group">
          <label id="password_label">Password</label>
          <input type="password" id="cred_password">
        </div>
        
        <div class="form-group checkbox-group">
          <input type="checkbox" id="need_captcha">
          <label for="need_captcha" style="display:inline; font-weight:normal;">Need Captcha</label>
        </div>
        
        <div class="form-group checkbox-group">
          <input type="checkbox" id="is_active" checked>
          <label for="is_active" style="display:inline; font-weight:normal;">Active</label>
        </div>
        
        <div class="buttons">
          <button type="button" onclick="google.script.host.close()">Cancel</button>
          <button type="submit" class="primary">Create Account</button>
        </div>
      </form>
      
      <script>
        function updateCredentialFields() {
          const platform = document.getElementById('platform').value;
          const usernameLabel = document.getElementById('username_label');
          const passwordLabel = document.getElementById('password_label');
          const usernameGroup = document.getElementById('username_group');
          const passwordGroup = document.getElementById('password_group');
          
          if (platform === 'kira' || platform === 'm1') {
            usernameLabel.textContent = 'Username';
            passwordLabel.textContent = 'Password';
            usernameGroup.classList.remove('hidden');
            passwordGroup.classList.remove('hidden');
          } else if (platform === 'axai') {
            usernameLabel.textContent = 'Email';
            passwordLabel.textContent = 'Password';
            usernameGroup.classList.remove('hidden');
            passwordGroup.classList.remove('hidden');
          } else if (platform === 'fiuu') {
            usernameLabel.textContent = 'Merchant ID';
            passwordLabel.textContent = 'Private Key';
            usernameGroup.classList.remove('hidden');
            passwordGroup.classList.remove('hidden');
          } else {
            usernameGroup.classList.add('hidden');
            passwordGroup.classList.add('hidden');
          }
        }
        
        document.getElementById('accountForm').addEventListener('submit', function(e) {
          e.preventDefault();
          
          const data = {
            platform: document.getElementById('platform').value,
            label: document.getElementById('label').value,
            cred_username: document.getElementById('cred_username').value,
            cred_password: document.getElementById('cred_password').value,
            need_captcha: document.getElementById('need_captcha').checked,
            is_active: document.getElementById('is_active').checked
          };
          
          google.script.run
            .withSuccessHandler(function() {
              google.script.host.close();
            })
            .withFailureHandler(function(error) {
              alert('Error: ' + error.message);
            })
            .saveAccount(data);
        });
      </script>
    </body>
    </html>
  `;
}

function saveAccount(data) {
  const options = {
    method: 'POST',
    contentType: 'application/json',
    headers: { 'x-api-key': CONFIG.API_KEY },
    payload: JSON.stringify(data),
    muteHttpExceptions: true,
  };

  try {
    const response = UrlFetchApp.fetch(`${CONFIG.BASE_URL}/api/accounts`, options);
    const result = JSON.parse(response.getContentText());

    if (result.success) {
      refreshAccounts();
    } else {
      throw new Error(result.error || 'Failed to save account');
    }
  } catch (error) {
    throw error;
  }
}


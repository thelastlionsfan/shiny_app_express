function SFSOQLQuery() {
    let clientId = "3MVG9OffWoFjsEDEZB_SyNIlqmeCbTfnaYyx34btoeQ..yPNtd7fyqA0YaU1L25Z1y.iri6aSmqckLSU9MMSS";
    let clientSecret = "90725F147CD48605D19A8C8CB1DE38D4DEA6C99224DEFDAC5CD5B1C6AA7FD7C5";
    let username = "DataIntegrationuser@maximus.com.pnmtrain";
    let password = "Maximus2025!";
    let securityToken = "yy5BnmYf8aRfHoXGRqKGZ5kXd";
    let SOQL_DB_name = "migration-database-qa";
    const loginUrl = 'jdbc:'+SFSOQLQuery.toLowerCase()+'://'+SFSOQLQuery;//+ '/'+SOQL_DB_name;
    console.log(dbUrl);


    try {
        const sheets = SpreadsheetApp.getActiveSpreadsheet().getSheets();
        //console.log(sheets)
        
        let soqlStr = '';
        const fieldName = 'DS_SOQL';
    
        for (var i = 0; i < sheets.length; i++) {
          var checkSheetName = sheets[i].getName();
          if (checkSheetName.indexOf('Mapping')>=0) {
            console.log(checkSheetName);
            var sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(checkSheetName);
    
            var headerRow = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
    
            var colPos = headerRow.indexOf(fieldName) + 1; // 1-based index for getRange
            
            if (colPos === 0) {
              Logger.log('Column not found: ' + fieldName);
              return;
            }
            
            var data = sheet.getRange(2, colPos, sheet.getLastRow() - 1, 1).getValues();
            
            //Logger.log(data);
    
            for (var j = 0; j < data.length; j++) {
              let row = data[j][0];
              if (row) {
                soqlStr += `${row}; \n`
              }
              //Logger.log('Row ' + (j + 2) + ' Value: ' + data[j][0]);  // Row is i+2 because data starts at row 2
    
            }
    
            console.log(soqlStr);
    
    
          }
        }
        const stmt = conn.createStatement();
        const results = stmt.executeUpdate(soqlStr);
        
        console.log(results);
    
        /*
        const headerRow = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
        
        const fieldName = 'DS_SOQL';
        
        var colPos = headerRow.indexOf(fieldName) + 1; // 1-based index for getRange
        
        if (colPos === 0) {
          Logger.log('Column not found: ' + fieldName);
          return;
        }
        
        const data = sheet.getRange(2, colPos, sheet.getLastRow() - 1, 1).getValues();
        
        //Logger.log(data);
      
        let soqlStr = '';
    
        for (var i = 0; i < data.length; i++) {
          let row = data[i][0];
          if (row) {
            soqlStr += `${row}; \n`
          }
          //Logger.log('Row ' + (i + 2) + ' Value: ' + data[i][0]);  // Row is i+2 because data starts at row 2
    
        }
    
        console.log(soqlStr);
    
        const stmt = conn.createStatement();
        const results = stmt.executeUpdate(soqlStr);
        
        console.log(results);
        */
      }
      catch (err) {
        console.log(err)
      }
      //finally {
        //conn.close()
      //}
    }
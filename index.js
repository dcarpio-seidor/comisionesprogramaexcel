const hana = require('@sap/hana-client');
const XLSX = require('xlsx');

function main() {
    const jsonData = readExcel();
    
    for (const item of jsonData) {
        console.log(item);
    }
}

function readExcel() {
    const wb = XLSX.readFile(__dirname + '/assets/COMISIONES_CRM_BTP.xlsx');
    const sheetName = wb.SheetNames[0];               // first sheet
    const ws = wb.Sheets[sheetName];

    // Convert to JSON (first row = headers by default)
    const rows = XLSX.utils.sheet_to_json(ws, {
        defval: null,       // keep empty cells as null
        raw: false,         // parse dates/numbers as JS values
        dateNF: 'yyyy-mm-dd'
    });

    // console.log(rows.slice(0, 5));
    return rows.slice(0, 5);
}

main();
/**
 * Función para ejecutar procedure
 * @param {*} query 
 * @param {*} params 
 * @returns 
 */
let callBatch = function (conn, query, params) {
    return new Promise(async function (resolve, reject) {
        // let startDate = performance.now();
        try {
            let stmt = conn.prepare(query);
            stmt.execBatch(params, async function (err, rs) {
                if (err) {
                    reject(err);
                } else {
                    // let endDate = performance.now();
                    // console.log("Time in segs", (endDate - startDate) / 1000);
                    startDate = performance.now();
                    // console.log("rs", rs);
                    resolve(rs);
                }
            });
        } catch (error) {
            reject(error);
        }
    });
}

let callSql = function (conn, query, params) {
    return new Promise(async function (resolve, reject) {
        // let startDate = performance.now();
        try {
            conn.exec(query, params, async function (err, rs) {
                if (err) {
                    reject(err);
                } else {
                    // let endDate = performance.now();
                    // console.log("Time in segs", (endDate - startDate) / 1000);
                    // startDate = performance.now();
                    // console.log("rs", rs);
                    resolve(rs);
                }
            });
        } catch (error) {
            reject(error);
        }
    });
}

let deleteAllRecords = async function(conn, dbSchema, dbTable) {
    let query = `DELETE FROM "${dbSchema}"."${dbTable}"`;
    return await callSql(conn, query, []);
}

let getDbTables = async function(conn, dbSchema) {
    let query = `
        SELECT SCHEMA_NAME, OBJECT_NAME
        FROM SYS.OBJECTS
        WHERE "SCHEMA_NAME" = '${dbSchema}'
        AND OBJECT_TYPE = 'TABLE'
        ORDER BY OBJECT_NAME
    `;
    return await callSql(conn, query, []);
}

let getDbColumns = async function(conn, dbSchema, dbTable) {
    let query = `
        SELECT SCHEMA_NAME, TABLE_NAME, COLUMN_NAME, DATA_TYPE_NAME
        FROM SYS.TABLE_COLUMNS
        WHERE "SCHEMA_NAME" = '${dbSchema}'
        AND TABLE_NAME = '${dbTable}'
        ORDER BY POSITION
    `;
    return await callSql(conn, query, []);
}

let getDbPK = async function(conn, dbSchema, dbTable) {
    let query = `
        SELECT TABLE_NAME, COLUMN_NAME
        FROM SYS."CONSTRAINTS"
        WHERE "SCHEMA_NAME" = '${dbSchema}'
        AND TABLE_NAME = '${dbTable}'
        AND IS_PRIMARY_KEY = 'TRUE'
    `;
    return await callSql(conn, query, []);
}

let getDbCount = async function(conn, dbSchema, dbTable) {
    let query = `SELECT COUNT(1) AS CANTIDAD FROM "${dbSchema}"."${dbTable}"`;
    return await callSql(conn, query, []);
}

module.exports = {
    callBatch,
    callSql,
    deleteAllRecords,
    getDbTables,
    getDbColumns,
    getDbPK,
    getDbCount
};
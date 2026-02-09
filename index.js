const hana = require('@sap/hana-client');
const XLSX = require('xlsx');
const utilDb = require('./util/database');
const utilMap = require('./util/map');

const oAuditRequest = {
    sUsuario: 'SYSTEM',
    sTerminal: '127.0.0.1'
}

async function main() {
    const jsonData = readExcel();
    if (jsonData.length) {
        setGlobal();
        let index = 0;
        let conn = getConnection();

        for (const item of jsonData) {
            console.log(`${++index}/${jsonData.length}`);
            //OBTENER EL ID DE CONFIGURACION
            let oFiltro = {};
            let idConf = '';
            oFiltro.ID_PROGRAMA = item.ID_PROGRAMA;
            oFiltro.CODIGO_PLATAFORMA = item.COD_PLATAFORMA;
            oFiltro.COD_PRODUCTO = item.COD_PRODUCTO;
            oFiltro.COD_SUBPRODUCTO = item.COD_SUBPRODUCTO;
            oFiltro.ID_MCCI = item.IDMCC;
            oFiltro.COD_MCCI = item.COD_MCCI;
            oFiltro.APLICA = item.APLICA;
            oFiltro.COM_CRED_OTRA_PLAT = item.COM_CRED_OTRA_PLAT;
            oFiltro.COM_DEB_OTRA_PLAT = item.COM_DEB_OTRA_PLAT;
            const validarProgramaConfiguracion = await consultarProgramaConfiguracion(conn, oFiltro);
            // console.log(validarProgramaConfiguracion);
            if (!Array.isArray(validarProgramaConfiguracion)) {
                throw Error(`Must be an array, instead of ${typeof validarProgramaConfiguracion}`);
            }
            if (validarProgramaConfiguracion.length > 1) {
                throw Error(`More than 1 value was found`);
            } else {
                if (validarProgramaConfiguracion.length) {
                    idConf = validarProgramaConfiguracion[0].ID;
                    await editarProgramaConfiguracion(
                        idConf, {
                        oAuditRequest: oAuditRequest,
                        oData: oFiltro,
                    }, conn);
                } else {
                    const registrarProgramaConfiguracionResponse = await registrarProgramaConfiguracion({
                        oAuditRequest: oAuditRequest,
                        oData: oFiltro,
                    }, conn);
                    idConf = registrarProgramaConfiguracionResponse.ID;
                }
            }
            //MARCAS
            let idMarcas = item.ID_MARCA.split(',');
            let comCreds = item.COM_CRED.split(',');
            let comDebs = item.COM_DEB.split(',');
            let cForCreds = item.CFOR_CRED.split(',');
            let cForDebs = item.CFOR_DEB.split(',');
            for (marca in idMarcas) {
                var idMarca = idMarcas[marca];
                var oParams1 = {};
                var oConditions = {};
                //MODIFICAR COMISIONES
                oParams1.COM_CRED = parseFloat(comCreds[marca]) >= 0 ? comCreds[marca] : null;
                oParams1.COM_DEB = parseFloat(comDebs[marca]) >= 0 ? comDebs[marca] : null;
                oParams1.CFOR_CRED = parseFloat(cForCreds[marca]) >= 0 ? cForCreds[marca] : null;
                oParams1.CFOR_DEB = parseFloat(cForDebs[marca]) >= 0 ? cForDebs[marca] : null;
                oParams1.USUARIO_MODIFICACION = oAuditRequest.sUsuario;
                oParams1.FECHA_MODIFICACION = new Date();
                oParams1.TERMINAL_MODIFICACION = oAuditRequest.sTerminal;
                oConditions.ID_PROGRAMA_CONFIG = idConf;
                oConditions.ID_MARCA = idMarca;
                actualizarProgramaConfiguracionMarcaResponse = await actualizaProgramaConfiguracionMarca(oConditions, oParams1, conn);
            }
            let flatCreds = item.FLAT_CRED.split(',');
            let flatDebs = item.FLAT_DEB.split(',');
            let fForCreds = item.FFOR_CRED.split(',');
            let fForDebs = item.FFOR_DEB.split(',');
            for (marca in idMarcas) {
                var idMarca = idMarcas[marca];
                var oParams1 = {};
                var oConditions = {};
                //MODIFICAR COMISIONES
                oParams1.FLAT_CRED = parseFloat(flatCreds[marca]) >= 0 ? flatCreds[marca] : null;
                oParams1.FLAT_DEB = parseFloat(flatDebs[marca]) >= 0 ? flatDebs[marca] : null;
                oParams1.FFOR_CRED = parseFloat(fForCreds[marca]) >= 0 ? fForCreds[marca] : null;
                oParams1.FFOR_DEB = parseFloat(fForDebs[marca]) >= 0 ? fForDebs[marca] : null;
                oParams1.USUARIO_MODIFICACION = oAuditRequest.sUsuario;
                oParams1.FECHA_MODIFICACION = new Date();
                oParams1.TERMINAL_MODIFICACION = oAuditRequest.sTerminal;
                oConditions.ID_PROGRAMA_CONFIG = idConf;
                oConditions.ID_MARCA = idMarca;
                actualizarProgramaConfiguracionMarcaResponse = await actualizaProgramaConfiguracionMarca(oConditions, oParams1, conn);
            }
        }
    }
}

function setGlobal() {
    globalThis.dbHost = '573e2afa-fd4a-472e-833d-52bb344eb3ca.hna0.prod-us10.hanacloud.ondemand.com'
    globalThis.dbPort = 443
    globalThis.dbTablePrefix = 'NIUBIZ_CORE_'
    // DEV
    globalThis.dbSchema = 'PPROY_CORE_DEV'
    globalThis.dbUsername = 'PPROY_CORE_DEV_7ZIXK9QHWPYJXP213S3ZPERZT_RT'
    globalThis.dbPassword = 'Ap5z7Xmg8LBdlV4CbqDtnwhyAHFlKN8vkOAaMmVWi937McRTz4MoiIthDD-ZfYxVZOy_dHqzB.DxDSD4Zb2jzv5DSV-eNT77caZJTOCn4SjgYHI15gQpvvQrG_cXJ5EN'
    // QAS
    // globalThis.dbSchema = 'PPROY_CORE_QAS'
    // globalThis.dbUsername = 'PPROY_CORE_QAS_B0X7SQX4TSFMZXZPZKQ3VSE7A_RT'
    // globalThis.dbPassword = 'Nc39a0W1gVeSlPHezyVopri08la4I8pagNIyFByaC.hCQXWKetX3l.rYfzVUUHbN_5wS9XTEoktYUbiIOakNj4n_h9XM9NJJAaNxC7P61JGi5ub5SQv3zURjcE_UirIc'
}

function readExcel() {
    const wb = XLSX.readFile(__dirname + '/assets/COMISIONES_CRM_BTP.xlsx');
    const sheetName = wb.SheetNames[0]; // first sheet
    const ws = wb.Sheets[sheetName];
    // Convert to JSON (first row = headers by default)
    const rows = XLSX.utils.sheet_to_json(ws, {
        defval: null,       // keep empty cells as null
        raw: false,         // parse dates/numbers as JS values
        dateNF: 'yyyy-mm-dd'
    });
    // console.log(rows.slice(0, 5));
    return rows.slice(0, 1000);
}

function getConnection() {
    var conn = hana.createConnection();
    conn.connect({
        serverNode: `${globalThis.dbHost}:${globalThis.dbPort}`,
        uid: globalThis.dbUsername,
        pwd: globalThis.dbPassword,
        pooling: true
    });
    console.log(`Conectado a servidor ${globalThis.dbHost}:${globalThis.dbPort} / schema: ${globalThis.dbSchema}`);
    return conn;
}

async function consultarProgramaConfiguracion(conn, oFiltro) {
    let query;
    let rows;
    try {
        query = `SELECT * FROM "${globalThis.dbSchema}"."${globalThis.dbTablePrefix}PROGRAMACONFIGURACION_PROGRAMACONFIGURACION"`;
        query = `${query} WHERE 1 = 1`;
        query = `${query} AND ID_PROGRAMA = ${oFiltro.ID_PROGRAMA}`;
        query = `${query} AND COD_PRODUCTO = '${oFiltro.COD_PRODUCTO}'`;
        query = `${query} AND COD_SUBPRODUCTO = '${oFiltro.COD_SUBPRODUCTO}'`;
        query = `${query} AND ID_MCCI = ${oFiltro.ID_MCCI}`;
        query = `${query} AND COD_MCCI = '${oFiltro.COD_MCCI}'`;
        query = `${query} AND CODIGO_PLATAFORMA = '${oFiltro.CODIGO_PLATAFORMA}'`;
        // console.log("sql: ", query);
        rows = await utilDb.callSql(conn, query, []);
        return rows;
    } catch (e) {
        throw e;
    }
}

async function editarProgramaConfiguracion(iId, oParam, conn) {
    try {
        var data = utilMap.MapForEditarProgramaConfiguracion(iId, oParam);
        // oResponse = utils.updateDB(esquema, 'xs_visanet_afiliacion_prd::ProgramaConfiguracion.ProgramaConfiguracion', data,'ID');

        var aKeys = Object.keys(data).filter(e => e != 'ID');
        var aCampos = aKeys.map(x => `"${x}" = ?`);
        query = `UPDATE "${globalThis.dbSchema}"."${globalThis.dbTablePrefix}PROGRAMACONFIGURACION_PROGRAMACONFIGURACION" SET ${aCampos.join(',')} WHERE "ID" = ?`;
        console.log("sql: ", query);
        let params = [];
        for (const key of aKeys) {
            params.push(data[key]);
        }
        params.push(data['ID']);
        // console.log("batchInsert: ", batchInsert);
        await utilDb.callBatch(conn, query, [params]);
    } catch (e) {
        throw e;
    }
}

async function registrarProgramaConfiguracion(oParam, conn) {
    try {
        // var Id = utils.obtenerSecuencia(esquema, 'xs_visanet_afiliacion_prd::SequenceProgramaConfiguracion');
        var Id = 0;
        var data = await utilMap.MapForRegistrarProgramaConfiguracion(Id, oParam, conn);
        // oResponse = utils.insertDB(esquema, 'xs_visanet_afiliacion_prd::ProgramaConfiguracion.ProgramaConfiguracion', data);

        sql = `SELECT "${globalThis.dbSchema}"."PROGRAMACONFIGURACION_PROGRAMACONFIGURACION_SEQ".NEXTVAL FROM DUMMY`;
        // console.log("sql: ", query);
        rows = await utilDb.callSql(conn, sql, []);
        sequence = rows[0];
        data['ID'] = Object.values(sequence)[0];

        var aKeys = Object.keys(data)
        var aCampos = aKeys.map(x => `"${x}"`);
        query = `INSERT INTO "${globalThis.dbSchema}"."${globalThis.dbTablePrefix}PROGRAMACONFIGURACION_PROGRAMACONFIGURACION" (${aCampos.join(',')}) VALUES (${aCampos.map(e => "?").join(',')})`;
        console.log("sql: ", query);
        let params = [];
        for (const key of aKeys) {
            params.push(data[key]);
        }
        // console.log("batchInsert: ", batchInsert);
        await utilDb.callBatch(conn, query, [params]);
        return data;
    } catch (e) {
        throw e;
    }
}

async function actualizaProgramaConfiguracionMarca(oParamCondition, oParam, conn) {
    // const tx = cds.tx(req);
    // let locale = req.user.locale;
    // let bundle = textBundle.getTextBundle(locale);
    // var oResponse = {};
    // var pathQuery;
    var aItems = [];
    var aConditions = [];
    var aCampos = [];
    var aCamposIns = [];
    var camposValidos = {};
    camposValidos = oParam;

    try {
        let result = 0;

        //CAMPOS VARIABLES
        aItems.push([]);
        var camposAct = Object.getOwnPropertyNames(oParam);
        camposAct.forEach(function (campo) {
            if (campo === 'YYY') {
                //campos a no tomar
            } else {
                var str = '"' + campo + '" = ?';
                aCampos.push(str);
                aItems[0].push(oParam[campo]);
            }
        });

        //Conditions
        var camposCond = Object.getOwnPropertyNames(oParamCondition);
        camposCond.forEach(function (campo2) {
            // aItems[0].push(oParamCondition[campo2]);
            // var str = '"' + campo2 + '" = ?';
            // aConditions.push(str);
            if (typeof oParamCondition[campo2] == 'number') {
                aConditions.push(`${campo2} = ${oParamCondition[campo2]}`);
            } else {
                aConditions.push(`${campo2} = '${oParamCondition[campo2]}'`);
            }
        });

        aConditions.push('ID_ESTADO = 23');
        let pathFilter = aConditions.join(' AND ');
        query = `UPDATE "${globalThis.dbSchema}"."${globalThis.dbTablePrefix}PROGRAMACONFIGURACIONMARCA_PROGRAMACONFIGURACIONMARCA" SET ${aCampos.join(',')} WHERE ${pathFilter}`;
        // console.log("sql: ", query);
        result = await utilDb.callBatch(conn, query, aItems);

        if (result == 0) {
            aCampos = [];
            // aItems = [];
            // aItems.push([]);
            let oParamData = {};

            //Conditions
            camposCond.forEach(function (campo2) {
                aCampos.push(campo2);
                aCamposIns.push('?');
                // aItems[0].push(oParamCondition[campo2]);
                oParamData[campo2] = oParamCondition[campo2];
            });

            //CAMPOS VARIABLES
            camposAct.forEach(function (campo) {
                if (campo === 'YYY') {
                    //campos a no tomar
                } else {
                    if (campo === 'USUARIO_MODIFICACION') {
                        // aCampos.push("USUARIOCREADOR");
                        oParamData['USUARIO_CREACION'] = oParam[campo];
                    } else if (campo === 'TERMINAL_MODIFICACION') {
                        // aCampos.push("TERMINALCREACION");
                        oParamData['TERMINAL_CREACION'] = oParam[campo];
                    } else if (campo === 'FECHA_MODIFICACION') {
                        // aCampos.push("FECHACREACION");
                        oParamData['FECHA_CREACION'] = oParam[campo];
                    } else {
                        // aCampos.push(campo);
                        oParamData[campo] = oParam[campo];
                    }
                    aCamposIns.push('?');
                    // aItems[0].push(oParam[campo]);
                }
            });

            const data = oParamData;

            sql = `SELECT "${globalThis.dbSchema}"."PROGRAMACONFIGURACIONMARCA_PROGRAMACONFIGURACIONMARCA_SEQ".NEXTVAL FROM DUMMY`;
            // console.log("sql: ", query);
            rows = await utilDb.callSql(conn, sql, []);
            sequence = rows[0];
            data['ID'] = Object.values(sequence)[0];

            var aKeys = Object.keys(data)
            var aCampos = aKeys.map(x => `"${x}"`);
            query = `INSERT INTO "${globalThis.dbSchema}"."${globalThis.dbTablePrefix}PROGRAMACONFIGURACIONMARCA_PROGRAMACONFIGURACIONMARCA" (${aCampos.join(',')}) VALUES (${aCampos.map(e => "?").join(',')})`;
            // console.log("sql: ", query);
            let params = [];
            for (const key of aKeys) {
                params.push(data[key]);
            }
            // console.log("batchInsert: ", batchInsert);
            await utilDb.callBatch(conn, query, [params]);
        }
    } catch (e) {
        throw e;
    }
}

main();
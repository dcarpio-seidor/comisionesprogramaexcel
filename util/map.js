const utilDb = require('./database');

////
//// UTILS
////
function nullable(valor) {
    return valor ? valor : null;
}

////
//// CORE
////
async function consultarMCCI(oFiltro, conn) {
    // const tx = cds.tx(req);
    // let locale = req.user.locale;
    // let bundle = textBundle.getTextBundle(locale);
    var oResponse = {};
    var aParam = [];
    var aCampos = '*';
    var pathQuery;
    try {
        pathQuery = [];

        if (oFiltro.oData.COD_MCCI) {
            var arrayMcci = oFiltro.oData.COD_MCCI.split(',');
            var aFiltro = [];
            arrayMcci.forEach(function (obj) {
                aFiltro.push('?');
                aParam.push(obj);
            });
            // pathQuery = pathQuery + 'and "COD_MCCI" in (' + aFiltro.join() + ")";
            pathQuery.push(`COD_MCCI in (${aParam.map((x) => "'" + x + "'").join()})`);
        }

        // oResponse = utils.selectDB(esquema,paquete+ '::MCCI.MCCInternacional', aCampos,pathQuery,aParam);
        let pathFilter = pathQuery.join(' and ');
        // const authorization = token;
        // let oParamConsulta: ParamConsultaOdata = {
        //   entidad: '/MCCI_MCCInternacional',
        //   filtro: `?$filter=${pathFilter}`,
        // };
        // const consultaOdataResponse = await this.coreBaseService.consultaCoreOdata(
        //   oParamConsulta,
        //   authorization
        // );

        // const consultaOdataResponse = await tx.run(
        //     SELECT.from('Programa_Programa').columns(aCampos).where(pathFilter)
        // );
        query = `SELECT * FROM "${globalThis.dbSchema}"."${globalThis.dbTablePrefix}MCCI_MCCINTERNACIONAL"`;
        query = `${query} WHERE 1 = 1`;
        query = `${query} AND ${pathFilter}`;
        // console.log("sql: ", query);
        rows = await utilDb.callSql(conn, query, []);

        return rows;
    } catch (e) {
        throw e;
    }
}

function MapForEditarProgramaConfiguracion(iId, oParam) {
    var object = {
        ID: iId,
        ID_PROGRAMA: nullable(oParam.oData.ID_PROGRAMA),
        CODIGO_PLATAFORMA: nullable(oParam.oData.CODIGO_PLATAFORMA),
        COD_PRODUCTO: nullable(oParam.oData.COD_PRODUCTO),
        COD_SUBPRODUCTO: nullable(oParam.oData.COD_SUBPRODUCTO),
        ID_MCCI: nullable(oParam.oData.ID_MCCI),
        COD_MCCI: nullable(oParam.oData.COD_MCCI),
        INDICADOR: oParam.oData.APLICA ? 1 : 0,
        COM_CRED_OTRA_PLAT:
            parseInt(oParam.oData.COM_CRED_OTRA_PLAT) >= 0
                ? parseFloat(oParam.oData.COM_CRED_OTRA_PLAT)
                : null,
        COM_DEB_OTRA_PLAT:
            parseInt(oParam.oData.COM_DEB_OTRA_PLAT) >= 0
                ? parseFloat(oParam.oData.COM_DEB_OTRA_PLAT)
                : null,
        USUARIO_CREACION: oParam.oAuditRequest.sUsuario,
        TERMINAL_CREACION: oParam.oAuditRequest.sTerminal,
    };

    return object;
}

async function MapForRegistrarProgramaConfiguracion(iId, oParam, conn) {
    // const tx = cds.tx(req);
    // let locale = req.user.locale;
    // let bundle = textBundle.getTextBundle(locale);
    var object = {
        ID: iId,
        ID_PROGRAMA: nullable(oParam.oData.ID_PROGRAMA),
        CODIGO_PLATAFORMA: nullable(oParam.oData.CODIGO_PLATAFORMA),
        COD_PRODUCTO: nullable(oParam.oData.COD_PRODUCTO),
        COD_SUBPRODUCTO: nullable(oParam.oData.COD_SUBPRODUCTO),
        ID_MCCI: nullable(oParam.oData.ID_MCCI),
        COD_MCCI: nullable(oParam.oData.COD_MCCI),
        INDICADOR: oParam.oData.APLICA ? 1 : 0, //utils.nullable(oParam.oData.APLICA),
        COM_CRED_OTRA_PLAT:
            parseInt(oParam.oData.COM_CRED_OTRA_PLAT) >= 0
                ? parseFloat(oParam.oData.COM_CRED_OTRA_PLAT)
                : null,
        COM_DEB_OTRA_PLAT:
            parseInt(oParam.oData.COM_DEB_OTRA_PLAT) > 0
                ? parseFloat(oParam.oData.COM_DEB_OTRA_PLAT)
                : null,
        USUARIO_CREACION: oParam.oAuditRequest.sUsuario,
        TERMINAL_CREACION: oParam.oAuditRequest.sTerminal,
    };

    var oParam1 = {};
    oParam1.oData = {
        COD_MCCI: object.COD_MCCI,
    };

    var consultarMCCIResponse = await consultarMCCI(oParam1, conn);
    if (consultarMCCIResponse.length) {
        object['ID_MCCI'] = consultarMCCIResponse[0].IDMCC;
    }

    return object;
}

module.exports = {
    MapForEditarProgramaConfiguracion,
    MapForRegistrarProgramaConfiguracion,
};
-- -- -- -- -- -- -- -- -- -- -- --  -- -- -- -- -- -- 
-- -- -- I M P O R T S   &   E X P O R T S -- -- -- -- 
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- 

DROP TABLE IF EXISTS [canola].[imports_raw];
DROP TABLE IF EXISTS [canola].[exports_raw];

GO
-- // EXPORTS RAW DATA // -- 

--Set the options to support indexed views.
WITH CTE AS (
SELECT
    --DATEFROMPARTS(substring(RIGHT(REPLICATE('0', 8) + i."FECTRA", 8), 5, 4), 
    --substring(RIGHT(REPLICATE('0', 8) + i."FECTRA", 8), 3, 2), 
    --substring(RIGHT(REPLICATE('0', 8) + i."FECTRA", 8), 1, 2)) AS FECHA,
    CAST( COALESCE(NULLIF(
                /* Clean anythign that is not a number */
                canola.RemoveNonAlphaCharacters(
                /* In Chile commas are dots */
                REPLACE(i."CIF", ',', '.')
                )
                ,''),'0') AS NUMERIC(15,2)) AS CIF_MOD
    ,CAST( COALESCE(NULLIF(
                /* Clean anythign that is not a number */
                canola.RemoveNonAlphaCharacters(
                /* In Chile commas are dots */
                REPLACE(i."CIF_ITEM", ',', '.')
                )
                ,''),'0') AS NUMERIC(15,2)) AS CIF_ITEM_MOD
    ,CAST( COALESCE(NULLIF(
                /* Clean anythign that is not a number */
                canola.RemoveNonAlphaCharacters(
                /* In Chile commas are dots */
                REPLACE(i."FOB", ',', '.')
                )
                ,''),'0') AS NUMERIC(15,2)) AS FOB_MOD
    ,CAST( COALESCE(NULLIF(
                /* Clean anythign that is not a number */
                canola.RemoveNonAlphaCharacters(
                /* In Chile commas are dots */
                REPLACE(i."CANT_MERC", ',', '.')
                )
                ,''),'0') AS NUMERIC(15,2)) AS CANT_MERC_MOD
        ,CAST( COALESCE(NULLIF(
                /* Clean anythign that is not a number */
                canola.RemoveNonAlphaCharacters(
                /* In Chile commas are dots */
                REPLACE(i."PRE_UNIT", ',', '.')
                )
                ,''),'0') AS NUMERIC(15,2)) AS PRE_UNIT_MOD
        ,CAST( COALESCE(NULLIF(
                /* Clean anythign that is not a number */
                canola.RemoveNonAlphaCharacters(
                /* In Chile commas are dots */
                REPLACE(i."FLETE", ',', '.')
                )
                ,''),'0') AS NUMERIC(15,2)) AS FLETE_MOD
        ,CAST( COALESCE(NULLIF(
                /* Clean anythign that is not a number */
                canola.RemoveNonAlphaCharacters(
                /* In Chile commas are dots */
                REPLACE(i."MON_OTRO", ',', '.')
                )
                ,''),'0') AS NUMERIC(15,2)) AS MON_OTRO_MOD
        ,CAST( COALESCE(NULLIF(
                /* Clean anythign that is not a number */
                canola.RemoveNonAlphaCharacters(
                /* In Chile commas are dots */
                REPLACE(i."SEGURO", ',', '.')
                )
                ,''),'0') AS NUMERIC(15,2)) AS SEGURO_MOD
        ,CAST( COALESCE(NULLIF(
                /* Clean anythign that is not a number */
                canola.RemoveNonAlphaCharacters(
                /* In Chile commas are dots */
                REPLACE(i."TOT_PESO", ',', '.')
                )
                ,''),'0') AS NUMERIC(15,2)) AS TOT_PESO_MOD
    ,aca.aduana_nombre AS aduana_nombre
    ,accc.clausula_compra_nombre AS clausula_compra_nombre
    ,acdi.documento_ingreso_nombre AS documento_ingreso_nombre
    ,acm.moneda_nombre
    ,acp1.pais_nombre AS pais_nombre_origen
    ,acp2.pais_nombre AS pais_nombre_adquisicion
    ,acp3.pais_nombre AS pais_nombre_consignante
    ,acp4.pais_nombre AS pais_nombre_cia_transporte
    ,acpu1.puerto_nombre AS puerto_nombre_embarque
    ,acpu2.puerto_nombre AS puerto_nombre_desembarque
    ,act.transporte_nombre
    ,acu.unidad_nombre
    ,i.[NUMENCRIPTADO],i.[TIPO_DOCTO],i.[ADU],i.[FORM],i.[FECVENCI],i.[CODCOMUN],i.[NUM_UNICO_IMPORTADOR],i.[CODPAISCON],i.[DESDIRALM],i.[CODCOMRS],i.[ADUCTROL],i.[NUMPLAZO],i.[INDPARCIAL]
    ,i.[NUMHOJINS],i.[TOTINSUM],i.[CODALMA],i.[NUM_RS],i.[FEC_RS],i.[ADUA_RS],i.[NUMHOJANE],i.[NUM_SEC],i.[PA_ORIG],i.[PA_ADQ],i.[VIA_TRAN],i.[TRANSB],i.[PTO_EMB],i.[PTO_DESEM],i.[TPO_CARGA]
	,i.[ALMACEN],i.[FEC_ALMAC],i.[FECRETIRO],i.[NU_REGR],i.[ANO_REG],i.[CODVISBUEN],i.[NUMREGLA],i.[NUMANORES],i.[CODULTVB],i.[PAGO_GRAV],i.[FECTRA],i.[FECACEP],i.[GNOM_CIA_T],i.[CODPAISCIA]
    ,i.[NUMRUTCIA],i.[DIGVERCIA],i.[NUM_MANIF],i.[NUM_MANIF1],i.[NUM_MANIF2],i.[FEC_MANIF],i.[NUM_CONOC],i.[FEC_CONOC],i.[NOMEMISOR],i.[NUMRUTEMI],i.[DIGVEREMI],i.[GREG_IMP],i.[REG_IMP]
    ,i.[BCO_COM],i.[CODORDIV],i.[FORM_PAGO],i.[NUMDIAS],i.[VALEXFAB],i.[MONEDA],i.[MONGASFOB],i.[CL_COMPRA],i.[TOT_ITEMS],i.[FOB],i.[TOT_HOJAS],i.[COD_FLE],i.[FLETE],i.[TOT_BULTOS]
	,i.[COD_SEG],i.[SEGURO],i.[TOT_PESO],i.[CIF],i.[NUM_AUT],i.[FEC_AUT],i.[GBCOCEN],i.[ID_BULTOS],i.[TPO_BUL1],i.[CANT_BUL1],i.[TPO_BUL2],i.[CANT_BUL2],i.[TPO_BUL3],i.[CANT_BUL3],i.[TPO_BUL4]
    ,i.[CANT_BUL4],i.[TPO_BUL5],i.[CANT_BUL5],i.[TPO_BUL6],i.[CANT_BUL6],i.[TPO_BUL7],i.[CANT_BUL7],i.[TPO_BUL8],i.[CANT_BUL8],i.[CTA_OTRO],i.[MON_OTRO],i.[CTA_OTR1],i.[MON_OTR1]
    ,i.[CTA_OTR2],i.[MON_OTR2],i.[CTA_OTR3],i.[MON_OTR3],i.[CTA_OTR4],i.[MON_OTR4],i.[CTA_OTR5],i.[MON_OTR5],i.[CTA_OTR6],i.[MON_OTR6],i.[CTA_OTR7],i.[MON_OTR7],i.[MON_178],i.[MON_191]
	,i.[FEC_501],i.[VAL_601],i.[FEC_502],i.[VAL_602],i.[FEC_503],i.[VAL_603],i.[FEC_504],i.[VAL_604],i.[FEC_505],i.[VAL_605],i.[FEC_506],i.[VAL_606],i.[FEC_507],i.[VAL_607],i.[TASA]
    ,i.[NCUOTAS],i.[ADU_DI],i.[NUM_DI],i.[FEC_DI],i.[MON_699],i.[MON_199],i.[NUMITEM],i.[DNOMBRE],i.[DMARCA],i.[DVARIEDAD],i.[DOTRO1],i.[DOTRO2],i.[ATR_5],i.[ATR_6],i.[SAJU_ITEM]
	,i.[AJU_ITEM],i.[CANT_MERC],i.[MERMAS],i.[MEDIDA],i.[PRE_UNIT],i.[ARANC_ALA],i.[NUMCOR],i.[NUMACU],i.[CODOBS1],i.[DESOBS1],i.[CODOBS2],i.[DESOBS2],i.[CODOBS3],i.[DESOBS3],i.[CODOBS4]
    ,i.[DESOBS4],i.[ARANC_NAC],i.[CIF_ITEM],i.[ADVAL_ALA],i.[ADVAL],i.[VALAD],i.[OTRO1],i.[CTA1],i.[SIGVAL1],i.[VAL1],i.[OTRO2],i.[CTA2],i.[SIGVAL2],i.[VAL2],i.[OTRO3],i.[CTA3],i.[SIGVAL3]
    ,i.[VAL3],i.[OTRO4],i.[CTA4],i.[SIGVAL4],i.[VAL4],i.[fecha],i.[period_id],i.[reference_id]
    from canola.imports AS i
    left join canola.aduana_codigos_aduana AS aca ON isnull(i.ADU,-1) = aca.aduana_codigo
    left join canola.aduana_codigos_clausula_compra AS accc ON isnull(i.CL_COMPRA,-1) = accc.clausula_compra_codigo
    left join canola.aduana_codigos_documento_ingreso AS acdi ON isnull(i.TIPO_DOCTO,-1) = acdi.documento_ingreso_codigo
    left join canola.aduana_codigos_monedas AS acm ON isnull(i.MONEDA,-1) = acm.moneda_codigo
    left join canola.aduana_codigos_pais AS acp1 ON isnull(i.PA_ORIG,-1) = acp1.pais_codigo
    left join canola.aduana_codigos_pais AS acp2 ON isnull(i.PA_ADQ,-1) = acp2.pais_codigo
    left join canola.aduana_codigos_pais AS acp3 ON isnull(i.CODPAISCON,-1) = acp3.pais_codigo
    left join canola.aduana_codigos_pais AS acp4 ON isnull(i.CODPAISCIA,-1) = acp4.pais_codigo
    left join canola.aduana_codigos_puertos AS acpu1 ON isnull(i.PTO_EMB,-1) = acpu1.puerto_codigo
    left join canola.aduana_codigos_puertos AS acpu2 ON isnull(i.PTO_DESEM,-1) = acpu2.puerto_codigo
    left join canola.aduana_codigos_transporte AS act ON isnull(i.VIA_TRAN,-1) = act.transporte_codigo
    left join canola.aduana_codigos_unidad AS acu ON isnull(i.MEDIDA,-1) = acu.unidad_codigo
),
CTE2 AS (
SELECT
reference_id,NUMENCRIPTADO,NUMITEM
,FECHA,FECTRA, DATEADD(day, 1, EOMONTH(FECHA, -1)) AS fecha_month
,ARANC_NAC,ARANC_ALA
,DNOMBRE,DMARCA,DVARIEDAD,DOTRO1,DOTRO2,ATR_5,ATR_6
,CODOBS1,DESOBS1,CODOBS2,DESOBS2,CODOBS3,DESOBS3,CODOBS4,DESOBS4
,CONCAT(DNOMBRE,' ',DMARCA,' ',DVARIEDAD,' ',DOTRO1,' ',DOTRO2) AS DESC_COMPLETA
,CIF,CIF_MOD
,FOB,FOB_MOD
,CIF_ITEM,CIF_ITEM_MOD
,FLETE,FLETE_MOD
,MON_OTRO,MON_OTRO_MOD
,SEGURO,SEGURO_MOD
,MONEDA,moneda_nombre
,PRE_UNIT,PRE_UNIT_MOD
,CONCAT(moneda_nombre,' / ', unidad_nombre) AS PRE_UNIT_UOM
,CIF_ITEM_MOD/NULLIF(CANT_MERC_MOD,0) as PRE_UNIT_CIF
,CANT_MERC,CANT_MERC_MOD
,TOT_PESO,TOT_PESO_MOD
,MEDIDA,unidad_nombre
,TOT_BULTOS,TOT_ITEMS
,ADU,aduana_nombre
,CL_COMPRA,clausula_compra_nombre
,TIPO_DOCTO,documento_ingreso_nombre
,PA_ORIG,pais_nombre_origen
,PA_ADQ,pais_nombre_adquisicion
,CODPAISCON,pais_nombre_consignante
,CODPAISCIA,pais_nombre_cia_transporte
,PTO_EMB,puerto_nombre_embarque
,PTO_DESEM,puerto_nombre_desembarque
,VIA_TRAN,transporte_nombre
,FORM,FECVENCI,CODCOMUN,NUM_UNICO_IMPORTADOR,DESDIRALM,CODCOMRS,ADUCTROL,NUMPLAZO,INDPARCIAL,NUMHOJINS,TOTINSUM,CODALMA,NUM_RS,FEC_RS,ADUA_RS,NUMHOJANE,NUM_SEC,TRANSB,TPO_CARGA
,ALMACEN,FEC_ALMAC,FECRETIRO,NU_REGR,ANO_REG,CODVISBUEN,NUMREGLA,NUMANORES,CODULTVB,PAGO_GRAV,FECACEP,GNOM_CIA_T,NUMRUTCIA,DIGVERCIA,NUM_MANIF,NUM_MANIF1,NUM_MANIF2,FEC_MANIF
,NUM_CONOC,FEC_CONOC,NOMEMISOR,NUMRUTEMI,DIGVEREMI,GREG_IMP,REG_IMP,BCO_COM,CODORDIV,FORM_PAGO,NUMDIAS,VALEXFAB,MONGASFOB,TOT_HOJAS,COD_FLE,COD_SEG,NUM_AUT,FEC_AUT,GBCOCEN
,ID_BULTOS,TPO_BUL1,CANT_BUL1,TPO_BUL2,CANT_BUL2,TPO_BUL3,CANT_BUL3,TPO_BUL4,CANT_BUL4,TPO_BUL5,CANT_BUL5,TPO_BUL6,CANT_BUL6,TPO_BUL7,CANT_BUL7,TPO_BUL8,CANT_BUL8,CTA_OTRO
,CTA_OTR1,MON_OTR1,CTA_OTR2,MON_OTR2,CTA_OTR3,MON_OTR3,CTA_OTR4,MON_OTR4,CTA_OTR5,MON_OTR5,CTA_OTR6,MON_OTR6,CTA_OTR7,MON_OTR7,MON_178,MON_191,FEC_501,VAL_601,FEC_502,VAL_602
,FEC_503,VAL_603,FEC_504,VAL_604,FEC_505,VAL_605,FEC_506,VAL_606,FEC_507,VAL_607,TASA,NCUOTAS,ADU_DI,NUM_DI,FEC_DI,MON_699,MON_199,SAJU_ITEM,AJU_ITEM,MERMAS,NUMCOR,NUMACU,ADVAL_ALA
,ADVAL,VALAD,OTRO1,CTA1,SIGVAL1,VAL1,OTRO2,CTA2,SIGVAL2,VAL2,OTRO3,CTA3,SIGVAL3,VAL3,OTRO4,CTA4,SIGVAL4,VAL4
FROM CTE
)
SELECT * 
INTO canola.imports_raw
FROM CTE2;

GO
-- // IMPORTS RAW DATA // -- 

WITH CTE AS (
    SELECT 
    CAST( COALESCE(NULLIF(
                /* Clean anythign that is not a number */
                canola.RemoveNonAlphaCharacters(
                /* In Chile commas are dots */
                REPLACE(e."CANTIDADMERCANCIA", ',', '.')
                )
                ,''),'0') AS NUMERIC(15,2)) AS CANTIDADMERCANCIA_MOD
    ,CAST( COALESCE(NULLIF(
                /* Clean anythign that is not a number */
                canola.RemoveNonAlphaCharacters(
                /* In Chile commas are dots */
                REPLACE(e."FOBUS", ',', '.')
                )
                ,''),'0') AS NUMERIC(15,2)) AS FOBUS_MOD
    ,CAST( COALESCE(NULLIF(
                /* Clean anythign that is not a number */
                canola.RemoveNonAlphaCharacters(
                /* In Chile commas are dots */
                REPLACE(e."FOBUNITARIO", ',', '.')
                )
                ,''),'0') AS NUMERIC(15,2)) AS FOBUNITARIO_MOD
    ,CAST( COALESCE(NULLIF(
                /* Clean anythign that is not a number */
                canola.RemoveNonAlphaCharacters(
                /* In Chile commas are dots */
                REPLACE(e."VALORFLETE", ',', '.')
                )
                ,''),'0') AS NUMERIC(15,2)) AS VALORFLETE_MOD
    ,CAST( COALESCE(NULLIF(
                /* Clean anythign that is not a number */
                canola.RemoveNonAlphaCharacters(
                /* In Chile commas are dots */
                REPLACE(e."TOTALVALORFOB", ',', '.')
                )
                ,''),'0') AS NUMERIC(15,2)) AS TOTALVALORFOB_MOD
    ,CAST( COALESCE(NULLIF(
                /* Clean anythign that is not a number */
                canola.RemoveNonAlphaCharacters(
                /* In Chile commas are dots */
                REPLACE(e."PESOBRUTOTOTAL", ',', '.')
                )
                ,''),'0') AS NUMERIC(15,2)) AS PESOBRUTOTOTAL_MOD
    ,CAST( COALESCE(NULLIF(
                /* Clean anythign that is not a number */
                canola.RemoveNonAlphaCharacters(
                /* In Chile commas are dots */
                REPLACE(e."VALORLIQUIDORETORNO", ',', '.')
                )
                ,''),'0') AS NUMERIC(15,2)) AS VALORLIQUIDORETORNO_MOD
    ,CAST( COALESCE(NULLIF(
                /* Clean anythign that is not a number */
                canola.RemoveNonAlphaCharacters(
                /* In Chile commas are dots */
                REPLACE(e."VALORCLAUSULAVENTA", ',', '.')
                )
                ,''),'0') AS NUMERIC(15,2)) AS VALORCLAUSULAVENTA_MOD
    ,aca.aduana_nombre AS aduana_nombre
    ,act.transporte_nombre AS transporte_nombre
    ,acp4.pais_nombre AS paisciatransp_nombre
    ,acm.moneda_nombre AS moneda_nombre
    ,acu.unidad_nombre AS unidad_nombre
    ,e.[fecha],e.[period_id],e.[reference_id]
	,DATEADD(day, 1, EOMONTH(e.[fecha], -1)) AS fecha_month
	,CONCAT(NOMBRE,' ',ATRIBUTO1,' ',ATRIBUTO2,' ',ATRIBUTO3,' ',ATRIBUTO4,' ',ATRIBUTO5,' ',ATRIBUTO6) AS DESC_COMPLETA
    ,e.[FECHAACEPT],e.[NUMEROIDENT],e.[ADUANA],e.[TIPOOPERACION],e.[CODIGORUTEXPORTADORPPAL],e.[NRO_EXPORTADOR],e.[PORCENTAJEEXPPPAL],e.[COMUNAEXPORTADORPPAL],e.[CODIGORUTEXPSEC]
	,e.[NRO_EXPORTADOR_SEC],e.[PORCENTAJEEXPSECUNDARIO],e.[COMUNAEXPSECUNDARIO],e.[PUERTOEMB],e.[GLOSAPUERTOEMB],e.[REGIONORIGEN],e.[TIPOCARGA],e.[VIATRANSPORTE],e.[PUERTODESEMB]
    ,e.[GLOSAPUERTODESEMB],e.[PAISDESTINO],e.[GLOSAPAISDESTINO],e.[NOMBRECIATRANSP],e.[PAISCIATRANSP],e.[RUTCIATRANSP],e.[DVRUTCIATRANSP],e.[NOMBREEMISORDOCTRANSP],e.[RUTEMISOR]
    ,e.[DVRUTEMISOR],e.[CODIGOTIPOAUTORIZA],e.[NUMEROINFORMEEXPO],e.[DVNUMEROINFORMEEXP],e.[FECHAINFORMEEXP],e.[MONEDA],e.[MODALIDADVENTA],e.[CLAUSULAVENTA],e.[FORMAPAGO],e.[VALORCLAUSULAVENTA]
    ,e.[COMISIONESEXTERIOR],e.[OTROSGASTOS],e.[VALORLIQUIDORETORNO],e.[NUMEROREGSUSP],e.[ADUANAREGSUSP],e.[PLAZOVIGENCIAREGSUSP],e.[TOTALITEM],e.[TOTALBULTOS],e.[PESOBRUTOTOTAL]
    ,e.[TOTALVALORFOB],e.[VALORFLETE],e.[CODIGOFLETE],e.[VALORSEGURO],e.[CODIGOSEG],e.[VALORCIF],e.[NUMEROPARCIALIDAD],e.[TOTALPARCIALES],e.[PARCIAL],e.[OBSERVACION],e.[NUMERODOCTOCANCELA]
    ,e.[FECHADOCTOCANCELA],e.[TIPODOCTOCANCELA],e.[PESOBRUTOCANCELA],e.[TOTALBULTOSCANCELA],e.[NUMEROITEM],e.[NOMBRE],e.[ATRIBUTO1],e.[ATRIBUTO2],e.[ATRIBUTO3],e.[ATRIBUTO4]
    ,e.[ATRIBUTO5],e.[ATRIBUTO6],e.[CODIGOARANCEL],e.[UNIDADMEDIDA],e.[CANTIDADMERCANCIA],e.[FOBUNITARIO],e.[FOBUS],e.[CODIGOOBSERVACION1],e.[VALOROBSERVACION1],e.[GLOSAOBSERVACION1]
	,e.[CODIGOOBSERVACION2],e.[VALOROBSERVACION2],e.[GLOSAOBSERVACION2],e.[CODIGOOBSERVACION3],e.[VALOROBSERVACION3],e.[GLOSAOBSERVACION3],e.[PESOBRUTOITEM]
    FROM canola.exports AS e
	left join canola.aduana_codigos_aduana AS aca ON isnull(e.ADUANA,-1) = aca.aduana_codigo
	left join canola.aduana_codigos_monedas AS acm ON isnull(e.MONEDA,-1) = acm.moneda_codigo
    left join canola.aduana_codigos_pais AS acp4 ON isnull(e.PAISCIATRANSP,-1) = acp4.pais_codigo
    left join canola.aduana_codigos_transporte AS act ON isnull(e.VIATRANSPORTE,-1) = act.transporte_codigo
    left join canola.aduana_codigos_unidad AS acu ON isnull(e.UNIDADMEDIDA,-1) = acu.unidad_codigo
    ),
	CTE2 AS (
		SELECT
		reference_id
		FECHAACEPT,fecha,fecha_month,
		DESC_COMPLETA,
		CANTIDADMERCANCIA,CANTIDADMERCANCIA_MOD,
		FOBUS,FOBUS_MOD,
		FOBUNITARIO,FOBUNITARIO_MOD,
		VALORFLETE,VALORFLETE_MOD,
		TOTALVALORFOB,TOTALVALORFOB_MOD,
		PESOBRUTOTOTAL,PESOBRUTOTOTAL_MOD,
		VALORLIQUIDORETORNO,VALORLIQUIDORETORNO_MOD,
		VALORCLAUSULAVENTA,VALORCLAUSULAVENTA_MOD,
	ADUANA, aduana_nombre
    ,VIATRANSPORTE,transporte_nombre
    ,PAISCIATRANSP,paisciatransp_nombre
    ,MONEDA,moneda_nombre
    ,UNIDADMEDIDA,unidad_nombre
    ,period_id
    ,NUMEROIDENT,TIPOOPERACION,CODIGORUTEXPORTADORPPAL,NRO_EXPORTADOR,PORCENTAJEEXPPPAL,COMUNAEXPORTADORPPAL,CODIGORUTEXPSEC
	,NRO_EXPORTADOR_SEC,PORCENTAJEEXPSECUNDARIO,COMUNAEXPSECUNDARIO,PUERTOEMB,GLOSAPUERTOEMB,REGIONORIGEN,TIPOCARGA,PUERTODESEMB
    ,GLOSAPUERTODESEMB,PAISDESTINO,GLOSAPAISDESTINO,NOMBRECIATRANSP,RUTCIATRANSP,DVRUTCIATRANSP,NOMBREEMISORDOCTRANSP,RUTEMISOR
    ,DVRUTEMISOR,CODIGOTIPOAUTORIZA,NUMEROINFORMEEXPO,DVNUMEROINFORMEEXP,FECHAINFORMEEXP,MODALIDADVENTA,CLAUSULAVENTA,FORMAPAGO
    ,COMISIONESEXTERIOR,OTROSGASTOS,NUMEROREGSUSP,ADUANAREGSUSP,PLAZOVIGENCIAREGSUSP,TOTALITEM,TOTALBULTOS
    ,CODIGOFLETE,VALORSEGURO,CODIGOSEG,VALORCIF,NUMEROPARCIALIDAD,TOTALPARCIALES,PARCIAL,OBSERVACION,NUMERODOCTOCANCELA
    ,FECHADOCTOCANCELA,TIPODOCTOCANCELA,PESOBRUTOCANCELA,TOTALBULTOSCANCELA,NUMEROITEM,NOMBRE,ATRIBUTO1,ATRIBUTO2,ATRIBUTO3,ATRIBUTO4
    ,ATRIBUTO5,ATRIBUTO6,CODIGOARANCEL,CODIGOOBSERVACION1,VALOROBSERVACION1,GLOSAOBSERVACION1
	,CODIGOOBSERVACION2,VALOROBSERVACION2,GLOSAOBSERVACION2,CODIGOOBSERVACION3,VALOROBSERVACION3,GLOSAOBSERVACION3,PESOBRUTOITEM
		FROM CTE
	)

    SELECT * 
    INTO canola.exports_raw
    FROM CTE2;
GO


-- -- -- -- -- -- -- -- -- -- -- -- -- -- --
-- -- -- R E P O R T s   V I E W S -- -- -- -- 
-- -- -- -- -- -- -- -- -- -- -- -- -- -- --

DROP VIEW IF EXISTS [canola].[vw_imports_canola_trigo];
DROP VIEW IF EXISTS [canola].[vw_exports_canola_trigo];

DROP VIEW IF EXISTS [canola].[vw_imports_canola_report];
DROP VIEW IF EXISTS [canola].[vw_exports_canola_report];

--DROP VIEW IF EXISTS [canola].[vw_imports_canola_oil_report];

GO
-- // IMPORTS REPORT - CANOLA & TRIGO // -- 

-- Create Materialized view for CANOLA AND TRIGO
CREATE VIEW canola.vw_imports_canola_trigo AS
WITH CTE AS (
    (SELECT 'trigo' AS TIPO, * FROM canola.imports_raw
    WHERE 
    /* Trigo - codigo arancel */
    ARANC_NAC LIKE '0910%' OR ARANC_NAC LIKE '1001%' OR ARANC_NAC LIKE '1002%' OR ARANC_NAC LIKE '1003%' OR ARANC_NAC LIKE '1004%' OR ARANC_NAC LIKE '1005%' OR ARANC_NAC LIKE '1006%' OR ARANC_NAC LIKE '1007%')
    UNION
    (SELECT 'trigo' AS TIPO, * FROM canola.imports_raw
    WHERE 
    /* Trigo - nombre */
    DNOMBRE LIKE '%TRIGO%' OR DNOMBRE LIKE '%CENTENO%' OR DNOMBRE LIKE '%MAIZ%' OR DNOMBRE LIKE '%CEBADA%')
    UNION
    (SELECT 'canola' AS TIPO, * FROM canola.imports_raw
    WHERE 
    /* Canola - codigo arancel */
    ARANC_NAC LIKE '23063000%' OR ARANC_NAC LIKE '23064100%' OR ARANC_NAC LIKE '23064900%' OR ARANC_NAC LIKE '15141100%' OR ARANC_NAC LIKE '23064000%' OR ARANC_NAC LIKE '15141100' OR ARANC_NAC LIKE '1205%') 
    UNION
    (SELECT 'canola' AS TIPO, * FROM canola.imports_raw
    WHERE 
    /* Canola - nombre */
    DNOMBRE LIKE '%CANOLA%' OR DNOMBRE LIKE '%RAPS%' OR DNOMBRE LIKE '%COLZA%' OR DNOMBRE LIKE '%NABO%')
)
SELECT 'Imports' AS TRADE_TYPE, 
        TIPO2 = CASE   
         WHEN DNOMBRE LIKE '%ACEITE%' THEN 'aceite'  
         ELSE 'otro'  
        END,
        * 
FROM CTE
WHERE DNOMBRE NOT LIKE '%SOLUCION%' AND DNOMBRE NOT LIKE '%BENZOATO%' AND DNOMBRE NOT LIKE '%VESTIDO%' AND DNOMBRE NOT LIKE '%FERTILIZANTE%' AND DNOMBRE NOT LIKE '%EMPAQUE%' AND
	  DNOMBRE NOT LIKE '%MEZCLA%' AND DNOMBRE NOT LIKE '%ARROZ%' AND DNOMBRE NOT LIKE '%GALLETAS%' AND DNOMBRE NOT LIKE '%PICKLES%' AND DNOMBRE NOT LIKE '%ROASTED ONION%' AND
	  DNOMBRE NOT LIKE '%CANNABOOST%' AND DNOMBRE NOT LIKE '%MARGARINA%' AND DNOMBRE NOT LIKE '%ANILINAS%' AND DNOMBRE NOT LIKE '%CAPILAR%' AND DNOMBRE NOT LIKE '%FETTFAX%' AND
	  DNOMBRE NOT LIKE '%HIDROFOBICA%' AND DESOBS1 NOT LIKE '%BOTELLAS%' AND DESOBS1 NOT LIKE '%FIDEOS%' AND DESOBS1 NOT LIKE '%PALOMITA%' AND DESOBS1 NOT LIKE '%CONDIMENTO%' AND
	  DESOBS1 NOT LIKE '%CABLE%' AND DESOBS1 NOT LIKE '%PASTA%' AND DMARCA NOT LIKE '%SAZONADOR%' AND
cant_merc_mod > 2 AND 
ARANC_NAC NOT IN (
'7129099',
'9109100',
'10063010',
'10063020',
'10063090',
'19041000',
'19059000',
'19059090',
'20019090',
'20059990',
'21039010',
'21039090',
'21061010',
'21069090',
'22029960',
'22030000',
'22087000',
'29163190',
'29214100',
'30043212',
'30043910',
'31010000',
'31051090',
'32159000',
'33051010',
'33051020',
'33059020',
'33059090',
'33074990',
'34022090',
'38099190',
'39031990',
'39173290',
'39231010',
'39231090',
'39232100',
'39232990',
'39239090',
'39241000',
'39269090',
'40169390',
'40169900',
'42021100',
'42022210',
'42022220',
'42023900',
'42029220',
'42031000',
'42050000',
'44079110',
'44149000',
'44219990',
'46021900',
'47062000',
'47069100',
'48131000',
'48139000',
'48193000',
'48236990',
'48239099',
'49070090',
'49111010',
'56031210',
'56041000',
'56075090',
'56090000',
'58063200',
'61091011',
'61099021',
'61124100',
'61143000',
'61178000',
'62045300',
'62045900',
'62082100',
'62171000',
'63023200',
'63079000',
'63079090',
'64019200',
'64029990',
'64039110',
'64041900',
'64051000',
'69072290',
'70109010',
'72202000',
'73202000',
'73239200',
'73269000',
'82019000',
'82055900',
'82100000',
'83099020',
'84198100',
'84199000',
'84224000',
'84303900',
'84323100',
'84323900',
'84328000',
'84335200',
'84335990',
'84339000',
'84368000',
'84378000',
'84382000',
'84386000',
'84388090',
'84713010',
'84713020',
'85068090',
'85094011',
'85094019',
'85098000',
'85166090',
'85167990',
'85177000',
'85182900',
'85183090',
'85258020',
'85369019',
'85437090',
'85442000',
'85444200',
'87082990',
'90049080',
'90160000',
'90259000',
'90262090',
'91132000',
'91139000',
'92099400',
'94016910',
'94054090',
'95030090',
'95049000',
'95069100',
'95069110',
'95069910',
'95069990'


);-- ORDER BY cant_merc_mod

GO

CREATE VIEW canola.vw_exports_canola_trigo AS 
WITH CTE AS ( 
        (SELECT 'trigo' AS TIPO, * FROM canola.exports_raw 
        WHERE CODIGOARANCEL LIKE '0910%' OR CODIGOARANCEL LIKE '1001%' OR CODIGOARANCEL LIKE '1002%' OR CODIGOARANCEL LIKE '1003%' OR CODIGOARANCEL LIKE '1004%' OR 
			  CODIGOARANCEL LIKE '1005%' OR CODIGOARANCEL LIKE '1006%' OR CODIGOARANCEL LIKE '1007%')
        UNION
        (SELECT 'trigo' AS TIPO, * FROM canola.exports_raw 
        WHERE NOMBRE LIKE '%TRIGO%' OR NOMBRE LIKE '%CENTENO%' OR NOMBRE LIKE '%MAIZ%' OR NOMBRE LIKE '%CEBADA%')
        UNION
        (SELECT 'canola' AS TIPO, * FROM canola.exports_raw 
        WHERE CODIGOARANCEL LIKE '23063000%' OR CODIGOARANCEL LIKE '23064100%' OR CODIGOARANCEL LIKE '23064900%' OR CODIGOARANCEL LIKE '15141100%' OR 
              CODIGOARANCEL LIKE '23064000%' OR CODIGOARANCEL LIKE '15141100' OR CODIGOARANCEL LIKE '1205%')
        UNION
        (SELECT 'canola' AS TIPO, * FROM canola.exports_raw 
        WHERE NOMBRE LIKE '%CANOLA%' OR NOMBRE LIKE '%RAPS%' OR NOMBRE LIKE '%COLZA%' OR NOMBRE LIKE '%NABO%')
        )
SELECT 'Exports' AS TRADE_TYPE,
        TIPO2 = CASE   
        WHEN NOMBRE LIKE '%ACEITE%' THEN 'aceite'  
        ELSE 'otro'  
        END,
        * 
FROM CTE
WHERE NOMBRE NOT LIKE '%SOLUCION%' AND NOMBRE NOT LIKE '%VESTIDO%' AND NOMBRE NOT LIKE '%FERTILIZANTE%' AND NOMBRE NOT LIKE '%EMPAQUE%' AND NOMBRE NOT LIKE '%MEZCLA%' AND 
	    NOMBRE NOT LIKE '%NESTUM%' AND NOMBRE NOT LIKE '%NESTLE%' AND NOMBRE NOT LIKE '%CERVEZA%' AND NOMBRE NOT LIKE '%POLERA%' AND NOMBRE NOT LIKE '%FALDA%' AND
        NOMBRE NOT LIKE '%CAMISA%' AND NOMBRE NOT LIKE '%LAPICES%' AND NOMBRE NOT LIKE '%BOLIGRAFOS%' AND NOMBRE NOT LIKE '%BOVINOS%' AND
        ATRIBUTO1 NOT LIKE '%BOTELLAS%' AND ATRIBUTO1 NOT LIKE '%FIDEOS%' AND ATRIBUTO1 NOT LIKE '%PALOMITA%' AND ATRIBUTO1 NOT LIKE '%CONDIMENTO%' AND ATRIBUTO1 NOT LIKE '%CABLE%' AND
        ATRIBUTO1 NOT LIKE '%PASTA%' AND
        CANTIDADMERCANCIA_MOD > 2 AND 
        CODIGOARANCEL NOT IN (
    '10063010','10063020','10063090','19041000','19059090','22030000','29163190','29214100','30043212','30043910','31010000','31051090',
    '32159000','33051010','33051020','33059020','33059090','33074990','38099190','39231010','39231090','39232990','39239090','39241000',
	'39241000','39269090','40169390','42022210','42022220','42023900','42029220','42050000','44079110','44219990','46021900','47062000',
	'47069100','48131000','48236990','48239099','49070090','49111010','56041000','56075090','58063200','61091011','61124100','62045900',
	'62082100','62171000','63079000','64019200','64039110','64041900','69072290','70109010','72202000','73239200','73269000','82019000',
	'82100000','84198100','84199000','84224000','84303900','84323100','84323900','84328000','84335200','84335990','84339000','84339000',
    '84368000','84378000','84382000','84386000','84388090','84713010','84713020','85068090','85094011','85094019','85098000','85166090',
    '85167990','85177000','85182900','85183090','85258020','85369019','85437090','85442000','85444200','87082990','90049080','90160000',
    '90262090','91132000','91139000','94016910','95030090','95030090','95049000','95069110','95069990','90159000','62114300'
);

GO

CREATE VIEW canola.vw_imports_canola_report AS 
WITH i AS (
	SELECT *, 
	currency_code = CASE
		WHEN MONEDA = 6 THEN 'cad'
		WHEN MONEDA = 13 THEN 'usd'
		WHEN MONEDA = 36 THEN 'aud'
		WHEN MONEDA = 142 THEN 'eur'
		WHEN MONEDA = 200 THEN 'clp'
		ELSE 'other' END
	FROM [canola].[vw_imports_canola_trigo] 
	WHERE TIPO = 'canola' 
	AND VIA_TRAN IN (7,1) 
	AND TIPO2 != 'aceite' 
	AND MEDIDA = 6 
	AND CANT_MERC_MOD > 100 
	AND DESOBS1 NOT LIKE '%PIEZAS%' 
	AND DESOBS1 NOT LIKE '%UNIDAD%' 
	AND DESOBS1 NOT LIKE '%CAJA%' 
	AND DESOBS1 NOT LIKE '%CIENTOS%' 
	AND DESOBS1 NOT LIKE '%KIT%' 
),
cc1 AS (
	SELECT *, DATEADD(day, 1, EOMONTH(currency_date, -1)) AS currency_month
	FROM [canola].[currency_converter]
),
cc2 AS (
	SELECT 
		currency_code, 
		currency_month, 
		AVG(CAST(to_clp AS float)) as to_clp,
		AVG(CAST(to_usd AS float)) as to_usd,
		AVG(CAST(to_eur AS float)) as to_eur,
		AVG(CAST(to_cad AS float)) as to_cad,
		AVG(CAST(to_aud AS float)) as to_aud
	FROM cc1
	GROUP BY currency_code, currency_month
),
i2 AS (
	SELECT
		i.*,
		is_siembra = CASE
			WHEN DESC_COMPLETA LIKE '%SIEMB%' THEN 1
			WHEN DESC_COMPLETA LIKE '%SIEMBR%' THEN 1
			WHEN DESC_COMPLETA LIKE '%SIEBRA%' THEN 1
			WHEN DESC_COMPLETA LIKE '%SIEM B%' THEN 1
			WHEN DESC_COMPLETA LIKE '%SIE MB%' THEN 1
			WHEN DESC_COMPLETA LIKE '%SI EM%' THEN 1
			WHEN DESC_COMPLETA LIKE '%S IEM%' THEN 1
			WHEN CANT_MERC_MOD < 5000 THEN 1
			ELSE 0 END,
		cc2.to_clp,
		cc2.to_usd,
		cc2.to_eur,
		cc2.to_cad,
		cc2.to_aud
	FROM i
	LEFT JOIN cc2
	ON i.currency_code = cc2.currency_code AND i.fecha_month = cc2.currency_month
	WHERE i.FOB_MOD*cc2.to_usd > 1000
)
SELECT * FROM i2;


GO
CREATE VIEW canola.vw_exports_canola_report AS 
WITH e AS (
	SELECT *, 
	currency_code = CASE
		WHEN MONEDA = 6 THEN 'cad'
		WHEN MONEDA = 13 THEN 'usd'
		WHEN MONEDA = 36 THEN 'aud'
		WHEN MONEDA = 142 THEN 'eur'
		WHEN MONEDA = 200 THEN 'clp'
		ELSE 'other' END
	FROM [canola].[vw_exports_canola_trigo] 
	WHERE TIPO = 'canola' 
	AND TIPO2 != 'aceite' 
	AND VIATRANSPORTE IN (7,1) 
	AND UNIDADMEDIDA = 6 
	AND CANTIDADMERCANCIA_MOD > 100 
),
cc1 AS (
	SELECT *, DATEADD(day, 1, EOMONTH(currency_date, -1)) AS currency_month
	FROM [canola].[currency_converter]
),
cc2 AS (
	SELECT 
		currency_code, 
		currency_month, 
		AVG(CAST(to_clp AS float)) as to_clp,
		AVG(CAST(to_usd AS float)) as to_usd,
		AVG(CAST(to_eur AS float)) as to_eur,
		AVG(CAST(to_cad AS float)) as to_cad,
		AVG(CAST(to_aud AS float)) as to_aud
	FROM cc1
	GROUP BY currency_code, currency_month
),
e2 AS (
	SELECT
		e.*,
		is_siembra = CASE
			WHEN DESC_COMPLETA LIKE '%SIEMB%' THEN 1
			WHEN DESC_COMPLETA LIKE '%SIEMBR%' THEN 1
			WHEN DESC_COMPLETA LIKE '%SIEBRA%' THEN 1
			WHEN DESC_COMPLETA LIKE '%SIEM B%' THEN 1
			WHEN DESC_COMPLETA LIKE '%SIE MB%' THEN 1
			WHEN DESC_COMPLETA LIKE '%SI EM%' THEN 1
			WHEN DESC_COMPLETA LIKE '%S IEM%' THEN 1
			WHEN CANTIDADMERCANCIA_MOD < 5000 THEN 1
			ELSE 0 END,
		cc2.to_clp,
		cc2.to_usd,
		cc2.to_eur,
		cc2.to_cad,
		cc2.to_aud
	FROM e
	LEFT JOIN cc2
	ON e.currency_code = cc2.currency_code AND e.fecha_month = cc2.currency_month
)
SELECT * FROM e2;
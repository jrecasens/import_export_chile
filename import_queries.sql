
-- Remove all records that were not read properly.
-- Use ADUANA column to filter out all non integer identifiers.
-- DELETE FROM public.exports
-- WHERE "ADU" ~ '^[^0-9]+$';


-- Add reference_id
ALTER TABLE public.imports
ADD COLUMN IF NOT EXISTS reference_id TEXT;
UPDATE public.imports SET reference_id=coalesce("NUMENCRIPTADO",'Unknown')||'-'||coalesce("NUMITEM",'Unknown');
ALTER TABLE public.imports
ALTER COLUMN reference_id SET NOT NULL;

ALTER TABLE public.imports ALTER COLUMN "ADU" TYPE integer USING "ADU"::integer;
ALTER TABLE public.imports ALTER COLUMN "CL_COMPRA" TYPE integer USING "CL_COMPRA"::integer;
ALTER TABLE public.imports ALTER COLUMN "TIPO_DOCTO" TYPE integer USING "TIPO_DOCTO"::integer;
ALTER TABLE public.imports ALTER COLUMN "MONEDA" TYPE integer USING "MONEDA"::integer;
ALTER TABLE public.imports ALTER COLUMN "PA_ORIG" TYPE integer USING "PA_ORIG"::integer;
ALTER TABLE public.imports ALTER COLUMN "PA_ADQ" TYPE integer USING "PA_ADQ"::integer;
ALTER TABLE public.imports ALTER COLUMN "CODPAISCON" TYPE integer USING "CODPAISCON"::integer;
ALTER TABLE public.imports ALTER COLUMN "CODPAISCIA" TYPE integer USING "CODPAISCIA"::integer;
ALTER TABLE public.imports ALTER COLUMN "PTO_EMB" TYPE integer USING "PTO_EMB"::integer;
ALTER TABLE public.imports ALTER COLUMN "PTO_DESEM" TYPE integer USING "PTO_DESEM"::integer;
ALTER TABLE public.imports ALTER COLUMN "VIA_TRAN" TYPE integer USING "VIA_TRAN"::integer;
ALTER TABLE public.imports ALTER COLUMN "MEDIDA" TYPE integer USING "MEDIDA"::integer;


DROP INDEX IF EXISTS index_imports_reference_id;
CREATE INDEX index_imports_reference_id
ON public.imports (reference_id);

DROP INDEX IF EXISTS index_imports_ADU;
CREATE INDEX index_imports_ADU
ON public.imports ("ADU");

DROP INDEX IF EXISTS index_imports_CL_COMPRA;
CREATE INDEX index_imports_CL_COMPRA
ON public.imports ("CL_COMPRA");

DROP INDEX IF EXISTS index_imports_TIPO_DOCTO;
CREATE INDEX index_imports_TIPO_DOCTO
ON public.imports ("TIPO_DOCTO");

DROP INDEX IF EXISTS index_imports_MONEDA;
CREATE INDEX iindex_imports_MONEDA
ON public.imports ("MONEDA");

DROP INDEX IF EXISTS index_imports_PA_ORIG;
CREATE INDEX index_imports_PA_ORIG
ON public.imports ("PA_ORIG");

DROP INDEX IF EXISTS index_imports_PA_ADQ;
CREATE INDEX index_imports_PA_ADQ
ON public.imports ("PA_ADQ");

DROP INDEX IF EXISTS index_imports_CODPAISCON;
CREATE INDEX index_imports_CODPAISCON
ON public.imports ("CODPAISCON");

DROP INDEX IF EXISTS index_imports_CODPAISCIA;
CREATE INDEX index_imports_CODPAISCIA
ON public.imports ("CODPAISCIA");

DROP INDEX IF EXISTS index_imports_PTO_EMB;
CREATE INDEX iindex_imports_PTO_EMB
ON public.imports ("PTO_EMB");

DROP INDEX IF EXISTS index_imports_PTO_DESEM;
CREATE INDEX index_imports_PTO_DESEM
ON public.imports ("PTO_DESEM");

DROP INDEX IF EXISTS index_imports_VIA_TRAN;
CREATE INDEX index_imports_VIA_TRAN
ON public.imports ("VIA_TRAN");

DROP INDEX IF EXISTS index_imports_MEDIDA;
CREATE INDEX index_imports_MEDIDA
ON public.imports ("MEDIDA");


-- Create Materialized view for reporting
DROP MATERIALIZED VIEW IF EXISTS public.imports_raw_report CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.imports_raw_report
AS (
		select 
-- 	i."reference_id",
-- 	    i."ARANC_NAC",i."DNOMBRE", i."DMARCA", i."DESOBS1", i."GNOM_CIA_T",
-- 		CASE WHEN i."VIA_TRAN" = '1' THEN
-- 			'MARITIMA, FLUVIAL Y LACUSTRE'
-- 		 WHEN i."VIA_TRAN" = '4' THEN 
-- 			'AEREO'
-- 		 WHEN i."VIA_TRAN" = '5' THEN
-- 			'POSTAL'
-- 		 WHEN i."VIA_TRAN" = '6' THEN
-- 			'FERROVIARIO'
-- 		 WHEN i."VIA_TRAN" = '7' THEN
-- 			'CARRETERO / TERRESTRE'
-- 		 WHEN i."VIA_TRAN" = '8' THEN
-- 			'OLEODUCTOS, GASODUCTOS'
-- 		 WHEN i."VIA_TRAN" = '9' THEN
-- 			'TENDIDO ELECTRICO (Aereo, Subterraneo)'
-- 		 WHEN i."VIA_TRAN" = '10' THEN
-- 			'OTRA'
-- 		 WHEN i."VIA_TRAN" = '11' THEN
-- 			'COURIER/AEREO'
-- 		ELSE
-- 			'OTRA'
-- 		END AS "VIA_TRAN_MOD",
-- 		CASE WHEN i."TIPO_DOCTO" = '101' THEN
-- 			'IMPORTACION PAGO CONTADO NORMAL'
-- 		 WHEN i."TIPO_DOCTO" = '151' THEN 
-- 			'IMPORTACION PAGO CONTADO ANTICIPADO'
-- 		 WHEN i."TIPO_DOCTO" = '103' THEN
-- 			'IMPORTACION ABONA O CANCELA DAPI. PAGO CONTADO'
-- 		 WHEN i."TIPO_DOCTO" = '104' THEN
-- 			'IMPORTACION ABONA O CANCELA DAT. PAGO CONTADO'
-- 		 WHEN i."TIPO_DOCTO" = '134' THEN
-- 			'IMPORTACION ABONA O CANCEL DAPITS'
-- 		 WHEN i."TIPO_DOCTO" = '105' THEN
-- 			'IMPORTACION ABONA O CANCELA AD. TEMP. PERF. ACT. (DAPE)'
-- 		ELSE
-- 			'OTRA'
-- 		END AS "TIPO_DOCTO_MOD",
			TO_TIMESTAMP(lpad(i."FECTRA", 8, '0'),'DDMMYYYY')::DATE AS FECHA,
			ROUND(CAST(COALESCE(NULLIF(REGEXP_REPLACE(
			/* In Chile commas are dots */
			REPLACE(i."CIF", ',', '.')
			/* Clean anythign that is not a number */
			,'[^0-9.]+', '', 'g'),''),'0') AS numeric),6)::numeric(15,6) AS CIF_MOD,
			 ROUND(CAST(COALESCE(NULLIF(REGEXP_REPLACE(
			/* In Chile commas are dots */
			REPLACE(i."FOB", ',', '.')
			/* Clean anythign that is not a number */
			,'[^0-9.]+', '', 'g'),''),'0') AS numeric),6)::numeric(15,6) AS FOB_MOD,
			ROUND(CAST(COALESCE(NULLIF(REGEXP_REPLACE(
			/* In Chile commas are dots */
			REPLACE(i."CANT_MERC", ',', '.')
			/* Clean anythign that is not a number */
			,'[^0-9.]+', '', 'g'),''),'0') AS numeric),6)::numeric(15,6) AS CANT_MERC_MOD,
			ROUND(CAST(COALESCE(NULLIF(REGEXP_REPLACE(
			/* In Chile commas are dots */
			REPLACE(i."PRE_UNIT", ',', '.')
			/* Clean anythign that is not a number */
			,'[^0-9.]+', '', 'g'),''),'0') AS numeric),6)::numeric(15,6) AS PRE_UNIT_MOD
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
	        ,i.*
		from public.imports AS i
		left join public.aduana_codigos_aduana AS aca ON i."ADU" = aca.aduana_codigo
		left join public.aduana_codigos_clausula_compra AS accc ON i."CL_COMPRA" = accc.clausula_compra_codigo
		left join public.aduana_codigos_documento_ingreso AS acdi ON i."TIPO_DOCTO" = acdi.documento_ingreso_codigo
		left join public.aduana_codigos_monedas AS acm ON i."MONEDA" = acm.moneda_codigo
		left join public.aduana_codigos_pais AS acp1 ON i."PA_ORIG" = acp1.pais_codigo
		left join public.aduana_codigos_pais AS acp2 ON i."PA_ADQ" = acp2.pais_codigo
		left join public.aduana_codigos_pais AS acp3 ON i."CODPAISCON" = acp3.pais_codigo
		left join public.aduana_codigos_pais AS acp4 ON i."CODPAISCIA" = acp4.pais_codigo
		left join public.aduana_codigos_puertos AS acpu1 ON i."PTO_EMB" = acpu1.puerto_codigo
		left join public.aduana_codigos_puertos AS acpu2 ON i."PTO_DESEM" = acpu2.puerto_codigo
		left join public.aduana_codigos_transporte AS act ON i."VIA_TRAN" = act.transporte_codigo
		left join public.aduana_codigos_unidad AS acu ON i."MEDIDA" = acu.unidad_codigo

	)
WITH NO DATA;
REFRESH MATERIALIZED VIEW public.imports_raw_report WITH DATA;



DROP MATERIALIZED VIEW IF EXISTS public.imports_canola_trigo;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.imports_canola_trigo AS (
WITH CTE AS (
	(SELECT 'trigo' AS "TIPO", * FROM public.imports_raw_report
	WHERE 
	/* Trigo - codigo arancel */
	"ARANC_NAC" LIKE '0910%' OR 
	"ARANC_NAC" LIKE '1001%' OR 
	"ARANC_NAC" LIKE '1002%' OR 
	"ARANC_NAC" LIKE '1003%' OR 
	"ARANC_NAC" LIKE '1004%' OR 
	"ARANC_NAC" LIKE '1005%' OR 
	"ARANC_NAC" LIKE '1006%' OR 
	"ARANC_NAC" LIKE '1007%')
	UNION
	(SELECT 'trigo' AS "TIPO", * FROM public.imports_raw_report
	WHERE 
	/* Trigo - nombre */
	"DNOMBRE" LIKE '%TRIGO%' OR 
	"DNOMBRE" LIKE '%CENTENO%' OR 
	"DNOMBRE" LIKE '%MAIZ%' OR 
	"DNOMBRE" LIKE '%CEBADA%')
	UNION
	(SELECT 'canola' AS "TIPO", * FROM public.imports_raw_report
	WHERE 
	/* Canola - codigo arancel */
	"ARANC_NAC" LIKE '23063000%' OR 
	"ARANC_NAC" LIKE '23064100%' OR 
	"ARANC_NAC" LIKE '1205%')	
	UNION
	(SELECT 'canola' AS "TIPO", * FROM public.imports_raw_report
	WHERE 
	/* Canola - nombre */
	"DNOMBRE" LIKE '%CANOLA%' OR 
	"DNOMBRE" LIKE '%RAPS%' OR 
	"DNOMBRE" LIKE '%COLZA%' OR 
	"DNOMBRE" LIKE '%NABO%')
	)
SELECT 'Imports' AS "TRADE_TYPE", * 
FROM CTE
WHERE "DNOMBRE" NOT LIKE '%ACEITE%' AND
"DNOMBRE" NOT LIKE '%SOLUCION%' AND
"DNOMBRE" NOT LIKE '%VESTIDO%' AND
"DNOMBRE" NOT LIKE '%FERTILIZANTE%' AND
"DNOMBRE" NOT LIKE '%EMPAQUE%' AND
"DNOMBRE" NOT LIKE '%MEZCLA%' AND
"DNOMBRE" NOT LIKE '%ARROZ%' AND
"DNOMBRE" NOT LIKE '%GALLETAS%' AND
"DNOMBRE" NOT LIKE '%TORTILLA%' AND
"DNOMBRE" NOT LIKE '%MARGARINA%' AND
"DNOMBRE" NOT LIKE '%PICKLES%' AND
"DNOMBRE" NOT LIKE '%ROASTED ONION%' AND
"DESOBS1" NOT LIKE '%BOTELLAS%' AND
"DESOBS1" NOT LIKE '%FIDEOS%' AND
"DESOBS1" NOT LIKE '%PALOMITA%' AND
"DESOBS1" NOT LIKE '%CONDIMENTO%' AND
"DESOBS1" NOT LIKE '%CABLE%' AND
"DESOBS1" NOT LIKE '%PASTA%' AND
"cant_merc_mod" > 2 AND 
"ARANC_NAC" != ANY (ARRAY[
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
'38099190',
'39232990',
'39241000',
'40169390',
'42022210',
'42022220',
'42023900',
'42029220',
'42050000',
'44219990',
'48131000',
'48239099',
'49070090',
'49111010',
'56041000',
'56075090',
'58063200',
'61124100',
'62045900',
'62082100',
'62171000',
'63079000',
'64019200',
'64039110',
'64041900',
'72202000',
'73269000',
'84199000',
'84224000',
'84339000',
'84713010',
'84713020',
'85068090',
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
'90262090',
'91132000',
'91139000',
'94016910',
'95030090',
'95049000',
'95069110',
'95069990',
'10063010',
'10063020',
'10063090',
'19041000',
'19059090',
'22030000',
'39231010',
'39231090',
'39239090',
'39241000',
'39269090',
'44079110',
'46021900',
'47062000',
'47069100',
'48236990',
'61091011',
'69072290',
'70109010',
'73239200',
'82019000',
'82100000',
'84198100',
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
'85094011',
'85094019',
'85098000',
'85166090',
'85167990',
'95030090'

])
	
	
-- ORDER BY "cant_merc_mod"
)
WITH NO DATA;
REFRESH MATERIALIZED VIEW public.imports_canola_trigo WITH DATA;


-- DROP MATERIALIZED VIEW IF EXISTS public.imports_canola_trigo2;
-- CREATE MATERIALIZED VIEW IF NOT EXISTS public.imports_canola_trigo2 AS (
-- 	SELECT 
-- 	A."TIPO",
--     B."ARANC_NAC",
--     B."DNOMBRE",
--     A."DMARCA",
--     A."DESOBS1",
--     A."GNOM_CIA_T",
--     A."CODPAISCIA",
--     A."CODPAISCON",
--     A."PTO_EMB",
--     A."PTO_DESEM",
--     A."MONEDA",
--     A."VIA_TRAN_MOD",
--     A."TIPO_DOCTO_MOD",
--     A.fecha,
--     A.cif_mod,
--     A.fob_mod,
--     A.cant_merc_mod,
--     A.pre_unit_mod,
-- 	B.* 
-- 	FROM public.imports_canola_trigo AS A
-- 	LEFT JOIN public.imports AS B ON A.reference_id = B.reference_id
-- )
-- WITH NO DATA;
-- REFRESH MATERIALIZED VIEW public.imports_canola_trigo2 WITH DATA;

















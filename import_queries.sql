-- Add reference_id
ALTER TABLE public.imports
ADD COLUMN IF NOT EXISTS reference_id TEXT;
UPDATE public.imports SET reference_id="NUMENCRIPTADO"||'-'||"NUMITEM";
ALTER TABLE public.imports
ALTER COLUMN reference_id SET NOT NULL;

CREATE INDEX index_imports_reference_id
ON public.imports (reference_id);

-- Create Materialized view for reporting
DROP MATERIALIZED VIEW IF EXISTS public.imports_raw_report CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.imports_raw_report
AS (
		select "reference_id",
	    "ARANC_NAC","DNOMBRE", "DMARCA", "DESOBS1", "GNOM_CIA_T", "CODPAISCIA", "CODPAISCON","PTO_EMB","PTO_DESEM","MONEDA",
		CASE WHEN "VIA_TRAN" = '1' THEN
			'MARITIMA, FLUVIAL Y LACUSTRE'
		 WHEN "VIA_TRAN" = '4' THEN 
			'AEREO'
		 WHEN "VIA_TRAN" = '5' THEN
			'POSTAL'
		 WHEN "VIA_TRAN" = '6' THEN
			'FERROVIARIO'
		 WHEN "VIA_TRAN" = '7' THEN
			'CARRETERO / TERRESTRE'
		 WHEN "VIA_TRAN" = '8' THEN
			'OLEODUCTOS, GASODUCTOS'
		 WHEN "VIA_TRAN" = '9' THEN
			'TENDIDO ELECTRICO (Aereo, Subterraneo)'
		 WHEN "VIA_TRAN" = '10' THEN
			'OTRA'
		 WHEN "VIA_TRAN" = '11' THEN
			'COURIER/AEREO'
		ELSE
			'OTRA'
		END AS "VIA_TRAN_MOD",
		CASE WHEN "TIPO_DOCTO" = '101' THEN
			'IMPORTACION PAGO CONTADO NORMAL'
		 WHEN "TIPO_DOCTO" = '151' THEN 
			'IMPORTACION PAGO CONTADO ANTICIPADO'
		 WHEN "TIPO_DOCTO" = '103' THEN
			'IMPORTACION ABONA O CANCELA DAPI. PAGO CONTADO'
		 WHEN "TIPO_DOCTO" = '104' THEN
			'IMPORTACION ABONA O CANCELA DAT. PAGO CONTADO'
		 WHEN "TIPO_DOCTO" = '134' THEN
			'IMPORTACION ABONA O CANCEL DAPITS'
		 WHEN "TIPO_DOCTO" = '105' THEN
			'IMPORTACION ABONA O CANCELA AD. TEMP. PERF. ACT. (DAPE)'
		ELSE
			'OTRA'
		END AS "TIPO_DOCTO_MOD",
			TO_TIMESTAMP(lpad("FECTRA", 8, '0'),'DDMMYYYY')::DATE AS FECHA,
			ROUND(CAST(COALESCE(NULLIF(REGEXP_REPLACE(
			/* In Chile commas are dots */
			REPLACE("CIF", ',', '.')
			/* Clean anythign that is not a number */
			,'[^0-9.]+', '', 'g'),''),'0') AS numeric),6)::numeric(15,6) AS CIF_MOD,
			 ROUND(CAST(COALESCE(NULLIF(REGEXP_REPLACE(
			/* In Chile commas are dots */
			REPLACE("FOB", ',', '.')
			/* Clean anythign that is not a number */
			,'[^0-9.]+', '', 'g'),''),'0') AS numeric),6)::numeric(15,6) AS FOB_MOD,
			ROUND(CAST(COALESCE(NULLIF(REGEXP_REPLACE(
			/* In Chile commas are dots */
			REPLACE("CANT_MERC", ',', '.')
			/* Clean anythign that is not a number */
			,'[^0-9.]+', '', 'g'),''),'0') AS numeric),6)::numeric(15,6) AS CANT_MERC_MOD,
			ROUND(CAST(COALESCE(NULLIF(REGEXP_REPLACE(
			/* In Chile commas are dots */
			REPLACE("PRE_UNIT", ',', '.')
			/* Clean anythign that is not a number */
			,'[^0-9.]+', '', 'g'),''),'0') AS numeric),6)::numeric(15,6) AS PRE_UNIT_MOD
		from public.imports
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
SELECT * 
FROM CTE
WHERE "DNOMBRE" NOT LIKE '%ACEITE%' AND
"DNOMBRE" NOT LIKE '%SOLUCION%' AND
"DNOMBRE" NOT LIKE '%VESTIDO%' AND
"DNOMBRE" NOT LIKE '%FERTILIZANTE%' AND
"DNOMBRE" NOT LIKE '%EMPAQUE%' AND
"DNOMBRE" NOT LIKE '%MEZCLA%' AND
"DESOBS1" NOT LIKE '%BOTELLAS%' AND
"DESOBS1" NOT LIKE '%FIDEOS%' AND
"DESOBS1" NOT LIKE '%PALOMITA%' AND
"DESOBS1" NOT LIKE '%CONDIMENTO%' AND
"DESOBS1" NOT LIKE '%CABLE%' AND
"DESOBS1" NOT LIKE '%PASTA%' AND
"cant_merc_mod" > 1
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

















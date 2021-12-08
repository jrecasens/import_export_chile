-- Remove all records that were not read properly.
-- Use ADUANA column to filter out all non integer identifiers.
DELETE FROM public.exports
WHERE "ADUANA" ~ '^[^0-9]+$';

-- Add reference_id
ALTER TABLE public.exports
DROP COLUMN IF EXISTS "REFERENCE_ID";
ALTER TABLE public.exports
ADD COLUMN "REFERENCE_ID" TEXT;
--UPDATE public.exports SET reference_id="NUMEROIDENT"||'-'||"CODIGOARANCEL"||'-'||TRIM(LEFT("NOMBRE", strpos("NOMBRE", '~') - 1));
UPDATE public.exports SET "REFERENCE_ID" = coalesce("NUMEROIDENT",'Unknown')||'-'||coalesce("NUMEROITEM",'Unknown');
ALTER TABLE public.exports
ALTER COLUMN "REFERENCE_ID" SET NOT NULL;

ALTER TABLE public.exports ALTER COLUMN "ADUANA" TYPE integer USING "ADUANA"::integer;
ALTER TABLE public.exports ALTER COLUMN "MONEDA" TYPE integer USING "MONEDA"::integer;
ALTER TABLE public.exports ALTER COLUMN "PAISCIATRANSP" TYPE numeric USING "PAISCIATRANSP"::numeric;
ALTER TABLE public.exports ALTER COLUMN "PAISCIATRANSP" TYPE integer USING "PAISCIATRANSP"::integer;
ALTER TABLE public.exports ALTER COLUMN "VIATRANSPORTE" TYPE integer USING "VIATRANSPORTE"::integer;
ALTER TABLE public.exports ALTER COLUMN "UNIDADMEDIDA" TYPE numeric USING "UNIDADMEDIDA"::numeric;
ALTER TABLE public.exports ALTER COLUMN "UNIDADMEDIDA" TYPE integer USING "UNIDADMEDIDA"::integer;

ALTER TABLE public.exports ALTER COLUMN "FECHAACEPT" TYPE numeric USING "FECHAACEPT"::numeric;
ALTER TABLE public.exports ALTER COLUMN "FECHAACEPT" TYPE integer USING "FECHAACEPT"::integer;


DROP INDEX IF EXISTS index_exports_reference_id;
CREATE INDEX index_exports_reference_id
ON public.exports ("REFERENCE_ID");

DROP INDEX IF EXISTS index_exports_ADUANA;
CREATE INDEX index_exports_ADUANA
ON public.exports ("ADUANA");

DROP INDEX IF EXISTS index_exports_MONEDA;
CREATE INDEX index_exports_MONEDA
ON public.exports ("MONEDA");

DROP INDEX IF EXISTS index_exports_PAISCIATRANSP;
CREATE INDEX index_exports_PAISCIATRANSP
ON public.exports ("PAISCIATRANSP");

DROP INDEX IF EXISTS index_exports_VIATRANSPORTE;
CREATE INDEX index_exports_VIATRANSPORTE
ON public.exports ("VIATRANSPORTE");

DROP INDEX IF EXISTS index_exports_UNIDADMEDIDA;
CREATE INDEX index_exports_UNIDADMEDIDA
ON public.exports ("UNIDADMEDIDA");


-- Create Materialized view for reporting
DROP MATERIALIZED VIEW IF EXISTS public.exports_raw_report CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.exports_raw_report
AS (
	SELECT 
	--"REFERENCE_ID",
    TO_TIMESTAMP(lpad(e."FECHAACEPT"::TEXT, 8, '0'),'DDMMYYYY')::DATE AS "FECHA",
	ROUND(CAST(COALESCE(NULLIF(REGEXP_REPLACE(
	/* In Chile commas are dots */
	REPLACE(e."CANTIDADMERCANCIA", ',', '.')
	/* Clean anythign that is not a number */
	,'[^0-9.]+', '', 'g'),''),'0') AS numeric),6)::numeric(15,6) AS CANT_MERC_MOD,
		ROUND(CAST(COALESCE(NULLIF(REGEXP_REPLACE(
	/* In Chile commas are dots */
	REPLACE(e."FOBUS", ',', '.')
	/* Clean anythign that is not a number */
	,'[^0-9.]+', '', 'g'),''),'0') AS numeric),6)::numeric(15,6) AS FOBUS_MOD,
		ROUND(CAST(COALESCE(NULLIF(REGEXP_REPLACE(
	/* In Chile commas are dots */
	REPLACE(e."FOBUNITARIO", ',', '.')
	/* Clean anythign that is not a number */
	,'[^0-9.]+', '', 'g'),''),'0') AS numeric),6)::numeric(15,6) AS FOBUNITARIO_MOD,
		ROUND(CAST(COALESCE(NULLIF(REGEXP_REPLACE(
	/* In Chile commas are dots */
	REPLACE(e."VALORFLETE", ',', '.')
	/* Clean anythign that is not a number */
	,'[^0-9.]+', '', 'g'),''),'0') AS numeric),6)::numeric(15,6) AS VALORFLETE_MOD,
		ROUND(CAST(COALESCE(NULLIF(REGEXP_REPLACE(
	/* In Chile commas are dots */
	REPLACE(e."TOTALVALORFOB", ',', '.')
	/* Clean anythign that is not a number */
	,'[^0-9.]+', '', 'g'),''),'0') AS numeric),6)::numeric(15,6) AS TOTALVALORFOB_MOD,
		ROUND(CAST(COALESCE(NULLIF(REGEXP_REPLACE(
	/* In Chile commas are dots */
	REPLACE(e."PESOBRUTOTOTAL", ',', '.')
	/* Clean anythign that is not a number */
	,'[^0-9.]+', '', 'g'),''),'0') AS numeric),6)::numeric(15,6) AS PESOBRUTOTOTAL_MOD,
		ROUND(CAST(COALESCE(NULLIF(REGEXP_REPLACE(
	/* In Chile commas are dots */
	REPLACE(e."VALORLIQUIDORETORNO", ',', '.')
	/* Clean anythign that is not a number */
	,'[^0-9.]+', '', 'g'),''),'0') AS numeric),6)::numeric(15,6) AS VALORLIQUIDORETORNO_MOD,
		ROUND(CAST(COALESCE(NULLIF(REGEXP_REPLACE(
	/* In Chile commas are dots */
	REPLACE(e."VALORCLAUSULAVENTA", ',', '.')
	/* Clean anythign that is not a number */
	,'[^0-9.]+', '', 'g'),''),'0') AS numeric),6)::numeric(15,6) AS VALORCLAUSULAVENTA_MOD,
	aca.aduana_nombre AS "ADUANA_NOMBRE",
	act.transporte_nombre AS "TRANSPORTE_NOMBRE",
	acp4.pais_nombre AS PAISCIATRANSP_NOMBRE,
	acm.moneda_nombre AS "MONEDA_NOMBRE",
	acu.unidad_nombre AS "UNIDAD_NOMBRE",
-- 	e."NRO_EXPORTADOR",
-- 	e."NRO_EXPORTADOR_SEC",
-- 	e."GLOSAPUERTOEMB",
-- 	e."GLOSAPUERTODESEMB",
-- 	e."GLOSAPAISDESTINO",
-- 	e."NOMBRECIATRANSP",
-- 	e."RUTCIATRANSP",
-- 	e."NOMBREEMISORDOCTRANSP",
-- 	e."VALORCLAUSULAVENTA",
-- 	e."TOTALITEM",
-- 	e."TOTALBULTOS",
-- 	e."PESOBRUTOTOTAL",
-- 	e."TOTALVALORFOB",
-- 	e."VALORFLETE",
-- 	e."VALORCIF",
	e.*
	FROM public.exports AS e
	LEFT JOIN public.aduana_codigos_aduana AS aca ON e."ADUANA" = aca.aduana_codigo
	left join public.aduana_codigos_monedas AS acm ON e."MONEDA" = acm.moneda_codigo
	left join public.aduana_codigos_pais AS acp4 ON e."PAISCIATRANSP" = acp4.pais_codigo
	left join public.aduana_codigos_transporte AS act ON e."VIATRANSPORTE" = act.transporte_codigo
	left join public.aduana_codigos_unidad AS acu ON e."UNIDADMEDIDA" = acu.unidad_codigo

	)
WITH NO DATA;
REFRESH MATERIALIZED VIEW public.exports_raw_report WITH DATA;


DROP MATERIALIZED VIEW IF EXISTS public.exports_canola_trigo;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.exports_canola_trigo AS (
	
		WITH CTE AS (
		(SELECT 'trigo' AS "TIPO", * FROM public.exports_raw_report
		WHERE 
		/* Trigo - codigo arancel */
		"CODIGOARANCEL" LIKE '0910%' OR 
		"CODIGOARANCEL" LIKE '1001%' OR 
		"CODIGOARANCEL" LIKE '1002%' OR 
		"CODIGOARANCEL" LIKE '1003%' OR 
		"CODIGOARANCEL" LIKE '1004%' OR 
		"CODIGOARANCEL" LIKE '1005%' OR 
		"CODIGOARANCEL" LIKE '1006%' OR 
		"CODIGOARANCEL" LIKE '1007%')
		UNION
		(SELECT 'trigo' AS "TIPO", * FROM public.exports_raw_report
		WHERE 
		/* Trigo - nombre */
		"NOMBRE" LIKE '%TRIGO%' OR 
		"NOMBRE" LIKE '%CENTENO%' OR 
		"NOMBRE" LIKE '%MAIZ%' OR 
		"NOMBRE" LIKE '%CEBADA%')
		UNION
		(SELECT 'canola' AS "TIPO", * FROM public.exports_raw_report
		WHERE 
		/* Canola - codigo arancel */
		"CODIGOARANCEL" LIKE '23063000%' OR 
		"CODIGOARANCEL" LIKE '23064100%' OR 
		"CODIGOARANCEL" LIKE '1205%')	
		UNION
		(SELECT 'canola' AS "TIPO", * FROM public.exports_raw_report
		WHERE 
		/* Canola - nombre */
		"NOMBRE" LIKE '%CANOLA%' OR 
		"NOMBRE" LIKE '%RAPS%' OR 
		"NOMBRE" LIKE '%COLZA%' OR 
		"NOMBRE" LIKE '%NABO%')
		)
	SELECT 'Exports' AS "TRADE_TYPE",* 
	FROM CTE
	WHERE "NOMBRE" NOT LIKE '%ACEITE%' AND
	"NOMBRE" NOT LIKE '%SOLUCION%' AND
	"NOMBRE" NOT LIKE '%VESTIDO%' AND
	"NOMBRE" NOT LIKE '%FERTILIZANTE%' AND
	"NOMBRE" NOT LIKE '%EMPAQUE%' AND
	"NOMBRE" NOT LIKE '%MEZCLA%' AND
	"NOMBRE" NOT LIKE '%NESTUM%' AND
	"NOMBRE" NOT LIKE '%NESTLE%' AND
	"NOMBRE" NOT LIKE '%CERVEZA%' AND
	"NOMBRE" NOT LIKE '%POLERA%' AND
	"NOMBRE" NOT LIKE '%FALDA%' AND
	"NOMBRE" NOT LIKE '%CAMISA%' AND
	"NOMBRE" NOT LIKE '%LAPICES%' AND
	"NOMBRE" NOT LIKE '%BOLIGRAFOS%' AND
	"NOMBRE" NOT LIKE '%BOVINOS%' AND
	"ATRIBUTO1" NOT LIKE '%BOTELLAS%' AND
	"ATRIBUTO1" NOT LIKE '%FIDEOS%' AND
	"ATRIBUTO1" NOT LIKE '%PALOMITA%' AND
	"ATRIBUTO1" NOT LIKE '%CONDIMENTO%' AND
	"ATRIBUTO1" NOT LIKE '%CABLE%' AND
	"ATRIBUTO1" NOT LIKE '%PASTA%' AND
	"cant_merc_mod" > 1
	
)
WITH NO DATA;
REFRESH MATERIALIZED VIEW public.exports_canola_trigo WITH DATA;
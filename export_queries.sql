-- Add reference_id
ALTER TABLE public.exports
DROP COLUMN IF EXISTS "REFERENCE_ID";
ALTER TABLE public.exports
ADD COLUMN "REFERENCE_ID" TEXT;
--UPDATE public.exports SET reference_id="NUMEROIDENT"||'-'||"CODIGOARANCEL"||'-'||TRIM(LEFT("NOMBRE", strpos("NOMBRE", '~') - 1));
UPDATE public.exports SET "REFERENCE_ID"="NUMEROIDENT"||'-'||"NUMEROITEM";
ALTER TABLE public.exports
ALTER COLUMN "REFERENCE_ID" SET NOT NULL;


SELECT 
"REFERENCE_ID",
TO_TIMESTAMP(lpad("FECHAACEPT", 8, '0'),'DDMMYYYY')::DATE AS "FECHA", 
aca.aduana AS "ADUANA_NOMBRE",
*
FROM public.exports AS e
LEFT JOIN public.aduana_codigos_aduana AS aca ON e."ADUANA" = aca.codigo_aduana;

	
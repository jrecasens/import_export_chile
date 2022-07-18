-- -- -- -- -- -- -- -- -- -- -- --  
-- -- -- I M P O R T S -- -- -- -- 
-- -- -- -- -- -- -- -- -- -- -- -- 

DROP INDEX IF EXISTS index_imports_reference_id ON canola.imports;
DROP INDEX IF EXISTS index_imports_fecha ON canola.imports;
DROP INDEX IF EXISTS index_imports_period_id ON canola.imports;
DROP INDEX IF EXISTS index_imports_ADU ON canola.imports;
DROP INDEX IF EXISTS index_imports_CL_COMPRA ON canola.imports;
DROP INDEX IF EXISTS index_imports_TIPO_DOCTO ON canola.imports;
DROP INDEX IF EXISTS index_imports_MONEDA ON canola.imports;
DROP INDEX IF EXISTS index_imports_PA_ORIG ON canola.imports;
DROP INDEX IF EXISTS index_imports_PA_ADQ ON canola.imports;
DROP INDEX IF EXISTS index_imports_CODPAISCON ON canola.imports;
DROP INDEX IF EXISTS index_imports_CODPAISCIA ON canola.imports;
DROP INDEX IF EXISTS index_imports_PTO_EMB ON canola.imports;
DROP INDEX IF EXISTS index_imports_PTO_DESEM ON canola.imports;
DROP INDEX IF EXISTS index_imports_VIA_TRAN ON canola.imports;
DROP INDEX IF EXISTS index_imports_MEDIDA ON canola.imports;

DROP TABLE IF EXISTS [canola].[imports_raw];
DROP VIEW IF EXISTS [canola].[vw_imports_canola_trigo];
DROP FUNCTION IF EXISTS [canola].[RemoveNonAlphaCharacters];

UPDATE canola.imports SET fecha=CONVERT(CHAR(10),fecha,120);

ALTER TABLE canola.imports ALTER COLUMN "fecha" date;
ALTER TABLE canola.imports ALTER COLUMN reference_id VARCHAR(255) NOT NULL;
ALTER TABLE canola.imports ALTER COLUMN period_id VARCHAR(10) NOT NULL;
ALTER TABLE canola.imports ALTER COLUMN "ADU" integer;
ALTER TABLE canola.imports ALTER COLUMN "CL_COMPRA" integer;
ALTER TABLE canola.imports ALTER COLUMN "TIPO_DOCTO" integer;
ALTER TABLE canola.imports ALTER COLUMN "MONEDA" integer;
ALTER TABLE canola.imports ALTER COLUMN "PA_ORIG" integer;
ALTER TABLE canola.imports ALTER COLUMN "PA_ADQ" integer;
ALTER TABLE canola.imports ALTER COLUMN "CODPAISCON" integer;
ALTER TABLE canola.imports ALTER COLUMN "CODPAISCIA" integer;
ALTER TABLE canola.imports ALTER COLUMN "PTO_EMB" integer;
ALTER TABLE canola.imports ALTER COLUMN "PTO_DESEM" integer;
ALTER TABLE canola.imports ALTER COLUMN "VIA_TRAN" integer;
ALTER TABLE canola.imports ALTER COLUMN "MEDIDA" integer;


CREATE INDEX index_imports_reference_id ON canola.imports (reference_id);
CREATE INDEX index_imports_fecha ON canola.imports (fecha);
CREATE INDEX index_imports_period_id ON canola.imports (period_id);
CREATE INDEX index_imports_ADU ON canola.imports ("ADU");
CREATE INDEX index_imports_CL_COMPRA ON canola.imports ("CL_COMPRA");
CREATE INDEX index_imports_TIPO_DOCTO ON canola.imports ("TIPO_DOCTO");
CREATE INDEX index_imports_MONEDA ON canola.imports ("MONEDA");
CREATE INDEX index_imports_PA_ORIG ON canola.imports ("PA_ORIG");
CREATE INDEX index_imports_PA_ADQ ON canola.imports ("PA_ADQ");
CREATE INDEX index_imports_CODPAISCON ON canola.imports ("CODPAISCON");
CREATE INDEX index_imports_CODPAISCIA ON canola.imports ("CODPAISCIA");
CREATE INDEX index_imports_PTO_EMB ON canola.imports ("PTO_EMB");
CREATE INDEX index_imports_PTO_DESEM ON canola.imports ("PTO_DESEM");
CREATE INDEX index_imports_VIA_TRAN ON canola.imports ("VIA_TRAN");
CREATE INDEX index_imports_MEDIDA ON canola.imports ("MEDIDA");

GO
Create Function [canola].[RemoveNonAlphaCharacters](@Temp VarChar(1000))
Returns VarChar(1000)
AS
Begin

    Declare @KeepValues as varchar(50)
    Set @KeepValues = '%[^0-9.]+%'
    While PatIndex(@KeepValues, @Temp) > 0
        Set @Temp = Stuff(@Temp, PatIndex(@KeepValues, @Temp), 1, '')

    Return @Temp
End;
GO


-- -- -- -- -- -- -- -- -- -- -- --  
-- -- -- E X P O R T S -- -- -- -- 
-- -- -- -- -- -- -- -- -- -- -- -- 

DROP INDEX IF EXISTS index_exports_reference_id ON canola.exports;
DROP INDEX IF EXISTS index_exports_fecha ON canola.exports;
DROP INDEX IF EXISTS index_exports_period_id ON canola.exports;
DROP INDEX IF EXISTS index_exports_ADUANA ON canola.exports;
DROP INDEX IF EXISTS index_exports_MONEDA ON canola.exports;
DROP INDEX IF EXISTS index_exports_PAISCIATRANSP ON canola.exports;
DROP INDEX IF EXISTS index_exports_VIATRANSPORTE ON canola.exports;
DROP INDEX IF EXISTS index_exports_UNIDADMEDIDA ON canola.exports;

DROP TABLE IF EXISTS [canola].[exports_raw];
DROP VIEW IF EXISTS [canola].[vw_exports_canola_trigo];

UPDATE canola.exports SET fecha=CONVERT(CHAR(10),fecha,120);

ALTER TABLE canola.exports ALTER COLUMN "fecha" date;
ALTER TABLE canola.exports ALTER COLUMN reference_id VARCHAR(255) NOT NULL;
ALTER TABLE canola.exports ALTER COLUMN period_id VARCHAR(10) NOT NULL;
ALTER TABLE canola.exports ALTER COLUMN "ADUANA" integer;
ALTER TABLE canola.exports ALTER COLUMN "MONEDA" integer;
ALTER TABLE canola.exports ALTER COLUMN "PAISCIATRANSP" numeric;
ALTER TABLE canola.exports ALTER COLUMN "PAISCIATRANSP" integer;
ALTER TABLE canola.exports ALTER COLUMN "VIATRANSPORTE" integer;
ALTER TABLE canola.exports ALTER COLUMN "UNIDADMEDIDA" numeric;
ALTER TABLE canola.exports ALTER COLUMN "UNIDADMEDIDA" integer;
ALTER TABLE canola.exports ALTER COLUMN "FECHAACEPT" numeric;
ALTER TABLE canola.exports ALTER COLUMN "FECHAACEPT" integer;



CREATE INDEX index_exports_reference_id ON canola.exports (reference_id);
CREATE INDEX index_exports_fecha ON canola.exports (fecha);
CREATE INDEX index_exports_period_id ON canola.exports (period_id);
CREATE INDEX index_exports_ADUANA ON canola.exports ("ADUANA");
CREATE INDEX index_exports_MONEDA ON canola.exports ("MONEDA");
CREATE INDEX index_exports_PAISCIATRANSP ON canola.exports ("PAISCIATRANSP");
CREATE INDEX index_exports_VIATRANSPORTE ON canola.exports ("VIATRANSPORTE");
CREATE INDEX index_exports_UNIDADMEDIDA ON canola.exports ("UNIDADMEDIDA");



-- Remove all records that were not read properly.
-- Use ADUANA column to filter out all non integer identifiers.
-- DELETE FROM canola.exports
-- WHERE "ADU" ~ '^[^0-9]+$';

-- IF COL_LENGTH ('canola.imports','reference_id') IS NULL
-- BEGIN
--   ALTER TABLE canola.imports
--  ADD reference_id VARCHAR (255)
-- END;

-- IF COL_LENGTH ('canola.imports','fecha') IS NULL
-- BEGIN
--   ALTER TABLE canola.imports
--     ADD fecha DATE
-- END;

-- IF COL_LENGTH ('canola.imports','period_id') IS NULL
-- BEGIN
--   ALTER TABLE canola.imports
--     ADD period_id VARCHAR (30)
-- END;


-- UPDATE canola.imports SET reference_id=coalesce("NUMENCRIPTADO",'Unknown')+'-'+coalesce("NUMITEM",'Unknown');

-- UPDATE canola.imports SET fecha=DATEFROMPARTS(substring(RIGHT(REPLICATE('0', 8) + "FECTRA", 8), 5, 4), 
-- substring(RIGHT(REPLICATE('0', 8) + "FECTRA", 8), 3, 2), 
-- substring(RIGHT(REPLICATE('0', 8) + "FECTRA", 8), 1, 2));
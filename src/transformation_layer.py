import polars as pl
from datetime import date
from loguru import logger
import duckdb
from collections import Counter


class FBSTransformer:
    
    input_folder = "C:/Users/cgarcia/Documents/datos/crudos"
    output_folder = "C:/Users/cgarcia/Documents/datos/modelados"

    working_group_dict = {
        'TL': 'Tramite en línea',
        'DDB': 'Direccion de desarrollo bienestar', 
        'GCIG': 'Grupo de control interno de gestión', 
        'GGAFCC': 'Grupo de gestion admin Crédito y cartera', 
        'SDE': 'Subdirección de desarrollo y emprendimiento',
        'GGC': 'Grupo de gestion de cesantias', 
        'GGEC': 'Grupo de gestion educativa y colegio',
        'GGTHDO': 'Grupo de gestion de talento humano y desarrollo organizacional', 
        'DGC': 'Dirección de gestion corporativa',
        'GER': 'Gerencia', 'GBRCD': 'Grupo de bienestar y recreación, cultura y deporte', 
        'GTICS': 'Grupo de tecnología, informacion y comunicaciones',
        'GCMAIS': 'Grupo centro medico y atencion integral', 
        'OPL': 'Oficina de planeación', 
        'GSAGD': 'Grupo de seguimiento y atencion a gerencias dptales',
        'GGF': 'Grupo de gestion financiera', 
        'GAJ': 'Grupo de asuntos juridicos', 
        'GGA': 'Grupo de gestion administrativa',
        'SDBV': 'Subdirección de bienestar', 
        'GAUEGI': 'Grupo de atencion al usuario', 
        'OAD': 'Oficina de asuntos disciplinarios'
    }

    @classmethod
    def raw_creditos_(self, df: pl.DataFrame) -> None:    
        # Step 1: Delete columns where the word "duplicated" appears in the column name
        logger.debug("Step 0 -- Removing duplicated columns")
        df = df.select([col for col in df.columns if "duplicated" not in col])

        # Step 2: Convert interests into numeric
        logger.debug("Step 1 -- Converting interest rates to numeric format")
        df = df.with_columns(
            (
                pl.col('TasaInterés')
                .str.replace(r'\s*%', '')           # Elimina el '%' y cualquier espacio antes de él
                .str.strip_chars()
                .cast(pl.Float64, strict=False)     # Convierte el string a float
                / 100*1000000                       # Divide por 100 para obtener el decimal
            ).alias('TasaInterés')                  # Renombra la columna resultante al nombre original
        )

        # Step 3: Convert dates to correct date format for operations in polars
        logger.debug("Step 2 -- Converting date columns to datetime format for polars")
        date_columns = ['FechaIngreso', 'FechaSolicitud', 'Fecha Acta Aprobación', 'FechaGiro', 'FechaInicio', 'FechaLegalización', 'VencimientoCuota']
        df = df.with_columns(
            pl.col(date_columns)
            .str.strip_chars()               # 1. Remove spaces
            .str.split(" ").list.get(0)      # 2. Kill any time part (keep only "DD/MM/YYYY")
            .str.replace_all("-", "/")       # 3. Turn dashes into slashes
            .str.replace_all(r"\.", "/")     # 4. Turn dots into slashes
            .str.to_date("%d/%m/%Y", strict=False) # 5. Try the standard conversion
        )

        # Step 4: Create 'tiempos' columns
        logger.debug("Step 3 -- Creating time difference columns")
        df = df.with_columns(
            (pl.col('FechaGiro') - pl.col('FechaSolicitud')).dt.total_days().alias('tiempo_solicitud_giro').cast(pl.Int64),
            (pl.col('FechaInicio') - pl.col('FechaSolicitud')).dt.total_days().alias('tiempo_solicitud_inicio').cast(pl.Int64),
            (pl.col('FechaLegalización') - pl.col('FechaSolicitud')).dt.total_days().alias('tiempo_solicitud_legalizacion').cast(pl.Int64),
        )   

        # Step 4: Add current date
        logger.debug("Step 4 -- Adding current date column")
        current_date = date.today()
        df = df.with_columns(
            pl.lit(current_date)
            .alias('fecha_actual')
        )

        # Step 5: create variable 'tiempo_de_espera'. If 'fecha_giro' is null, do 'fecha_actual' - 'fecha_solicitud'. Finally, convert to days
        # and cast to Int64
        logger.debug("Step 5 -- Creating wait-time column")
        df = df.with_columns(
            pl.when(pl.col('FechaGiro').is_null())
            .then(
                (pl.col('fecha_actual') - pl.col('FechaSolicitud')).dt.total_days()
            )
            .alias('tiempo_de_espera')
            .cast(pl.Int64)
        )

        # Step 6. Cambiar los separadores en el Monto de , a .
        logger.debug("Step 6 -- Standardizing monetary value formats")
        cols = ["Monto", "Monto Aprobado", "Saldo"] #, "Monto Solicitado"
        df = df.with_columns(
            pl.col(cols).str.replace_all(",", ".").cast(pl.Float64)
        )

        logger.debug("Step 7 -- Changing date format to avoid issues when exporting to other file types.")
        date_columns += ['fecha_actual']
        df = df.with_columns(
            pl.col(date_columns)
            .dt.strftime("%d/%m/%Y")
        )
        
        return df

    @classmethod
    def raw_radicados_(self, df: pl.DataFrame) -> None:
        
        output_df = df.with_columns(
            pl.col('Fecha Radicacion').str.strptime(pl.Datetime, format="%d/%m/%Y %H:%M", strict=False).alias('Fecha Radicacion')
        )
        
        # Separate values in column "Destino". IN the split, the first value is the destination and the second value is the type of destination
        output_df = output_df.with_columns(
            pl.when(pl.col('Destino').str.contains("-"))
              .then(pl.col('Destino')
                .str.split_exact("-", 2)
                .struct.rename_fields(["cargo_destino", "cod_grupo_destino", "funcionario_destino"])
                .alias('array_destino')
            )
            .otherwise(
            # Si NO contiene guion (es un nombre completo)
            pl.struct([
                pl.lit(None, dtype=pl.String).alias("cargo_destino"),
                pl.lit("GAUEGI", dtype=pl.String).alias("cod_grupo_destino"),
                pl.lit(None, dtype=pl.String).alias("funcionario_destino"),
            ])
            )
        ).unnest('array_destino')

        # Map a dictionary to the column "cod_grupo_destino" to create a new column "grupo_destino".
        output_df = output_df.with_columns(
                pl.col("cod_grupo_destino")
                .replace_strict(self.working_group_dict, default=None)
                .alias("grupo_destino")
            )
        return output_df

    @staticmethod
    def modeled_radicados_(df):
        return df.with_columns(
                pl.col('Radicado').cast(pl.Int64),
                pl.col('Rpta').cast(pl.Int64)
            )

    @staticmethod
    def modeled_creditos_(df):
        df = df.with_columns(
            pl.all().replace({"": None})
        )
        return df


transformer = FBSTransformer()

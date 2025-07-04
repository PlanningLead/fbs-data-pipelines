import polars as pl
from datetime import date
from loguru import logger
import os


class FBSPreprocessing:
    
    input_folder = "C:/Users/cgarcia/Documents/datos/crudos"
    output_folder = "C:/Users/cgarcia/Documents/datos/modelados"

    working_group_dict = {
        'DDB': 'Direccion de desarrollo bienestar', 'GCIG': 'Grupo de control interno de gestión', 
        'GGAFCC': 'Grupo de gestion admin Crédito y cartera', 'SDE': 'Subdirección de desarrollo y emprendimiento',
        'GGC': 'Grupo de gestion de cesantias', 'GGEC': 'Grupo de gestion educativa y colegio',
        'GGTHDO': 'Grupo de gestion de talento humano y desarrollo organizacional', 'DGC': 'Dirección de gestion corporativa',
        'GER': 'Gerencia', 'GBRCD': 'Grupo de bienestar y recreación, cultura y deporte', 'GTICS': 'Grupo de tecnología, informacion y comunicaciones',
        'GCMAIS': 'Grupo centro medico y atencion integral', 'OPL': 'Oficina de planeación', 'GSAGD': 'Grupo de seguimiento y atencion a gerencias dptales',
        'GGF': 'Grupo de gestion financiera', 'GAJ': 'Grupo de asuntos juridicos', 'GGA': 'Grupo de gestion administrativa',
        'SDBV': 'Subdirección de bienestar', 'GAUEGI': 'Grupo de atencion al usuario', 'OAD': 'Oficina de asuntos disciplinarios'
    }

    @classmethod
    def credit_preprocessing(self, df: pl.DataFrame) -> None:    
        """
        Clean the DataFrame by converting date columns to datetime and creating an 'edad' column.
        
        Parameters:
        df (DataFrame): The DataFrame to clean.
        
        Returns:
        DataFrame: The cleaned DataFrame.
        """
        # Step 1: Convert interests into numeric
        logger.info("Step 1 -- Converting interest rates to numeric format")
        intereses = df['tasa_interes'].str.replace('%', '')
        temp_intereses = []
        for tax in intereses:
            if len(tax) == 6:
                tax = float(tax)/10000000
            elif len(tax) == 5:
                tax = float(tax)/1000000
            elif len(tax) == 7:
                tax = float(tax)/10000000
            temp_intereses.append(tax)
        
        df = df.with_columns(pl.Series(temp_intereses).alias('tasa_interes'))

        # Step 2: Convert dates to correct format
        logger.info("Step 2 -- Converting date columns to datetime format")
        date_columns = ['fecha_cargue', 'fecha_solicitud', 'fecha_acta_aprobacion', 'fecha_giro', 'fecha_inicio', 'fecha_legalizacion']

        df = df.with_columns(
            pl.col(date_columns[0]).str.to_date().alias(date_columns[0]),
            pl.col(date_columns[1]).str.to_date().alias(date_columns[1]),
            pl.col(date_columns[2]).str.to_date().alias(date_columns[2]),
            pl.col(date_columns[3]).str.to_date().alias(date_columns[3]),
            pl.col(date_columns[4]).str.to_date().alias(date_columns[4]),
            pl.col(date_columns[5]).str.to_date().alias(date_columns[5])
        )
        # Step 3: Create 'tiempos' columns
        logger.info("Step 3 -- Creating time difference columns")
        df = df.with_columns(
            (pl.col('fecha_giro') - pl.col('fecha_solicitud')).dt.total_days().alias('tiempo_solicitud_giro').cast(pl.Int64),
            (pl.col('fecha_inicio') - pl.col('fecha_solicitud')).dt.total_days().alias('tiempo_solicitud_inicio').cast(pl.Int64),
            (pl.col('fecha_legalizacion') - pl.col('fecha_solicitud')).dt.total_days().alias('tiempo_solicitud_legalizacion').cast(pl.Int64),
        )   

        # Step 4: Add current date
        logger.info("Step 4 -- Adding current date column")
        current_date = date.today()
        df = df.with_columns(
            pl.lit(current_date).alias('fecha_actual')
        )

        # Step 5: create variable 'tiempo_de_espera'. If 'fecha_giro' is null, do 'fecha_actual' - 'fecha_solicitud'. Finally, convert to days
        # and cast to Int64
        logger.info("Step 5 -- Creating wait-time column")
        df = df.with_columns(
            pl.when(pl.col('fecha_giro').is_null())
            .then(
                (pl.col('fecha_actual') - pl.col('fecha_solicitud')).dt.total_days()
            )
            .alias('tiempo_de_espera')
            .cast(pl.Int64)
        )
        return df

    @classmethod
    def save_results_into_local(self, file_name: str, df: pl.DataFrame) -> None:
        # Check if csv file already exists
        output_file_name = f"{self.output_folder}/modeled_{file_name.split("_")[1]}"

        if not output_file_name.endswith('.csv'):
            output_file_name += '.csv'

        try:
            assert pl.scan_csv(output_file_name).collect().shape[0]
            logger.warning(f"File {output_file_name} already exists. Overwriting it.")

            # delete the existing file
            os.remove(output_file_name)
            # Save the cleaned DataFrame to a new CSV file
            df.write_csv(f"{output_file_name}")
        except FileNotFoundError:
            logger.warning("Saving cleaned DataFrame to CSV")
            df.write_csv(f"{output_file_name}")
        finally:
            logger.info(f"Data saved to {output_file_name}")


    @classmethod
    def radicacion_preprocessing(self, df: pl.DataFrame) -> None:
        df = df.with_columns(
            pl.col('Fecha Radicacion').str.to_date().alias('fecha_radicacion')
        )
        # drop modified columns
        df = df.drop(['Fecha Radicacion'])
        df = df.rename({'fecha_radicacion': 'fecha_solicitud'})
        
        # Separate values in column "Destino". IN the split, the first value is the destination and the second value is the type of destination
        df = df.with_columns(
            pl.col('Destino')
            .str.split_exact("-", 2)
            .struct.rename_fields(["cargo_destino", "cod_grupo_destino", "funcionario_destino"])
            .alias('array_destino')
        ).unnest('array_destino')

        # Map a dictionary to the column "cod_grupo_destino" to create a new column "grupo_destino".
        df = df.with_columns(
            pl.col("cod_grupo_destino")
            .replace_strict(self.working_group_dict, default=None)
            .alias("grupo_destino")
        )

        return df


if __name__ == '__main__':
    file_name = "20250616_radicados_alfanet.csv"

    preprocessing = FBSPreprocessing()

    df = pl.read_csv(f"{preprocessing.input_folder}/{file_name}", encoding='latin1', separator=';', ignore_errors=True)
    logger.info(f"Processing file: {file_name}")

    df = preprocessing.radicacion_preprocessing(df)
    
    preprocessing.save_results_into_local(file_name, df)

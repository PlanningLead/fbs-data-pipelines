from loguru import logger
from gdrive_handler import get_drive_service, read_metadata, download_file_into_dataframe
import os, io
from transform_data import FBSPreprocessing
from datetime import date
import polars as pl


class dataPipeline:

    output_dict = {'raw': None, 'modeled': None}

    drive_service = get_drive_service()
    

    @classmethod
    def extract_data_from_drive(self, target: list, data_layer: str) -> dict:

        layers = {'raw': 'crudos', 'modeled': 'modelados'}
        return read_metadata(
            service=self.drive_service,
            target_drive_name='Planeacion',  
            target_parents=['3 Datos', layers[data_layer], None],
            target_folders=target
        )

    # Add upload date for each file

    # compare with modeled current file

    # Transform data

    # Create a table recording the differences

    # Add onto modeled file the current dataframe
    # for folder, file_dict in files.items():

    #     logger.debug(f"De la carpeta: {folder} se encontraron: {len(file_dict['files'])} archivos.")
    #     # Take the file with the most recent date
    #     file_to_download = file_dict['files'][0]

    #     # Download the file into a DataFrame
    #     df = download_file_into_dataframe(service, file_to_download['id'], is_shared_drive=True)
        
    #     # Extract modeled data from the other folders

    #     # Add both dataframes into a dictionary


    #     logger.debug(f"Archivo {file_to_download['name']} descargado a dataframe: Shape=({df.shape})")
    # logger.info("Retrieving Google Drive service...")
    # return output_dict

    @classmethod
    def check_for_changes_in_data(self):
        return None


    @staticmethod
    def transform_data(df: pl.DataFrame, target: str) -> pl.DataFrame:
        preprocessing = FBSPreprocessing()
        
        if target == 'credito':
            df = preprocessing.credit_preprocessing(df)
        elif target == 'radicados':
            df = preprocessing.radicacion_preprocessing(df)
        elif target == 'funcionariosCGR':
            df = preprocessing.funcionarios_cgr_preprocessing(df)
        else:
            logger.error(f"Target '{target}' not recognized for transformation.")
        
        return df

    @staticmethod
    def load_data_into_drive() -> None:
        return None


if __name__ == "__main__":
    logger.info("Starting ETL process...")
    
    target_folders = ['credito', 'radicados', 'funcionariosCGR']
    target = ['radicados']
    
    pipeline = dataPipeline()
    raw_files = pipeline.extract_data_from_drive(target=target, data_layer='raw')

    logger.info("Loading modeled data into google drive...")

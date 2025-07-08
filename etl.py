from loguru import logger
from transform_data import FBSPreprocessing
from datetime import datetime
import polars as pl

from gdrive_handler import (get_drive_service, 
                            read_metadata, 
                            download_file_into_dataframe, 
                            get_google_credentials_for_institutional_account)

from gsheets_handler import *



class dataPipeline:

    output_dict = {'raw': None, 'modeled': None}
    creds = get_google_credentials_for_institutional_account()
    drive_service = get_drive_service(creds=creds)
   

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
    @staticmethod
    def adjust_date_format():
        date_string_3 = "2021-11-24T16:05:51.399+0000"
        format_string_3 = "%Y-%m-%dT%H:%M:%S.%f%z"
        return datetime.strptime(date_string_3, format_string_3)
    # compare with modeled current file

    # Transform data
    @classmethod
    def transform_(self, target: list, files: dict) -> None:
        # Sort list of dictionaries by createdTime in descending order
        files = sorted(files['radicados']['files'], key=lambda x: x['createdTime'], reverse=True)
        a_file = files[0] if files else None

        if a_file:
            logger.info(f"Found raw file: {a_file['name']} with ID: {a_file['id']}")
            df = download_file_into_dataframe(service=pipeline.drive_service, file_id=a_file['id'], is_shared_drive=True)
            self.output_dict['raw'] = df

            # Transform the data
            transformed_df = self.preprocessing_(input_df=df, subject=target[0])
            self.output_dict['modeled'] = transformed_df
        logger.debug("Data extracted and transformed successfully.")
    
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
    def preprocessing_(input_df: pl.DataFrame, subject: str) -> pl.DataFrame:
        preprocessing = FBSPreprocessing()
        
        if subject == 'credito':
            df = preprocessing.credit_preprocessing(input_df)
        elif subject == 'radicados':
            df = preprocessing.radicacion_preprocessing(input_df)
        else:
            logger.error(f"Target '{subject}' not recognized for transformation.")
        return df

    @staticmethod
    def load_data_into_drive() -> None:
        return None


if __name__ == "__main__":
    logger.info("Starting ETL process...")
    # target_folders = ['credito', 'radicados', 'funcionariosCGR']
    target = ['radicados']
    
    pipeline = dataPipeline()
    
    # Extract data from Google Drive    raw_files = pipeline.extract_data_from_drive(target=target, data_layer='raw')
    raw_files = pipeline.extract_data_from_drive(target=target, data_layer='raw')
    # Sort list of dictionaries by createdTime in descending order
    pipeline.transform_(target=target, )

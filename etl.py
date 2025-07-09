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
   
    validation = {'file_a': None, 'file_b': None}

    @classmethod
    def extract_metadata_from_drive(self, target: list, data_layer: str) -> dict:
        layers = {'raw': 'crudos', 'modeled': 'modelados'}
        self.current_layer = data_layer

        return read_metadata(
            service=self.drive_service,
            target_drive_name='Planeacion',  
            target_parents=['3 Datos', layers[data_layer], None],
            target_folders=target,
            data_layer=self.current_layer,
        )

    # Add upload date for each file
    @staticmethod
    def adjust_date_format():
        date_string_3 = "2021-11-24T16:05:51.399+0000"
        format_string_3 = "%Y-%m-%dT%H:%M:%S.%f%z"
        return datetime.strptime(date_string_3, format_string_3)
    
    # sort data by date
    @staticmethod
    def sort_and_get_most_recent(files):
        files = sorted(files['radicados']['files'], key=lambda x: x['createdTime'], reverse=True)
        selected_file = files[0] if files else None
        return selected_file

    # Transform data
    @classmethod
    def transform_(self, target: list, files: dict) -> None:
        # Sort list of dictionaries by createdTime in descending order
        
        if self.validation['file_a'] is None:
            selected_file = self.sort_and_get_most_recent(files)
            logger.debug(f"Found raw file: {selected_file['name']} with ID: {selected_file['id']}")
            df = download_file_into_dataframe(
                service=self.drive_service, 
                file_id=selected_file['id'], 
                is_shared_drive=True
            )
            # Transform the data
            output_df = self.preprocessing_(input_df=df, subject=target[0])
        
            self.validation['file_a'] = output_df

            logger.debug("Raw data extracted & transformed successfully.")
        
        else:
            output_df = download_file_into_dataframe(
                service=self.drive_service, 
                file_id=files['location_id'], 
                is_shared_drive=True
            )
            self.validation['file_b'] = output_df

            logger.debug("Modeled data extracted successfully.")
        return output_df

    @classmethod
    def extract_data_from_file():
        return None
    
    @classmethod
    def check_for_changes_in_data(self):
        df_a = self.validation['file_a']
        df_b = self.validation['file_b']

        # use polars to merge dataframes
        
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
    pipeline = dataPipeline()

    # Extract data from Google Drive
    logger.info(f"ETL process for raw files in folder '{['radicados'][0]}'")
    raw_files = pipeline.extract_metadata_from_drive(target=['radicados'], data_layer='raw')
    transformed_file = pipeline.transform_(target=['radicados'], files=raw_files)
    
    logger.info(f"ETL process for modeled files in file '{['radicados'][0]}'")
    modeled_files = pipeline.extract_metadata_from_drive(target=['radicados'], data_layer='modeled')
    extracted_file = pipeline.transform_(target=['radicados'], files=raw_files)


    # Load into modeled files
    logger.info("Finishing ETL Process...")

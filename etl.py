from loguru import logger
from src.transformation_ import FBSPreprocessing
from datetime import datetime
import polars as pl

from src.gdrive_handler import (get_drive_service, 
                            read_metadata, 
                            download_csv_into_dataframe, 
                            get_gdrive_credentials_for_institutional_account)

from src.gsheets_handler import (get_gsheets_credentials_for_institutional_account,
                             get_sheets_service,
                             download_data_from_sheets)



class ETLDataPipeline:

    validation = {'file_a': None, 'file_b': None}

    @classmethod
    def start_drive_service(self) -> None:
        creds = get_gdrive_credentials_for_institutional_account()
        self.drive_service = get_drive_service(creds=creds)
        
    @classmethod
    def start_sheets_service(self) -> None:
        creds = get_gsheets_credentials_for_institutional_account()
        self.sheets_service = get_sheets_service(creds=creds)

    @classmethod
    def read_metadata_from_drive(self, target: list, data_layer: str) -> dict:
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
    def adjust_date_format(date_string, format_string):
        return datetime.strptime(date_string, format_string)

    # sort data by date
    @staticmethod
    def sort_and_get_most_recent(files):
        files = sorted(files['radicados']['files'], key=lambda x: x['createdTime'], reverse=True)
        selected_file = files[0] if files else None
        return selected_file

    # Extract data
    @classmethod
    def extract_(self, files: dict=None) -> tuple:
        df = pl.DataFrame()
        if self.current_layer == 'raw':
            selected_file = self.sort_and_get_most_recent(files)
            logger.debug(f"Found raw file: {selected_file['name']} with ID: {selected_file['id']}")
            df = download_csv_into_dataframe(
                service=self.drive_service, 
                file_id=selected_file['id'], 
                is_shared_drive=True
            )
        elif self.current_layer == 'modeled':
            selected_file = files['files'][0]
            df = download_data_from_sheets(
                service=self.sheets_service, 
                spreadsheet_id=selected_file['id'],
                range_name='modeled_radicados.csv'
            )

        return (df, selected_file)

    # Transform data
    @classmethod
    def transform_(self, dataframe: object, selected_file: dict) -> None:
        # Transform the data
        if self.current_layer == 'raw':
            clean_name = selected_file['name']
            if len(selected_file['name'].split("_")) > 1:
                clean_name = selected_file['name'].split("_")[1].split(".")[0]

            output_df = self.preprocessing_(input_df=dataframe, subject=clean_name)
            self.validation['file_a'] = output_df
            logger.debug("Raw data extracted & transformed successfully.")
        
        elif self.current_layer == 'modeled':
            self.validation['file_b'] = dataframe
            output_df = dataframe

        return output_df

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
    pipeline = ETLDataPipeline()

    # Extract data from Google Drive
    group = ['radicados']

    # Extract data for raw files
    pipeline.start_drive_service()
    logger.info(f"ETL process for raw files in folder '{group[0]}'")
    raw_metadata = pipeline.read_metadata_from_drive(target=group, data_layer='raw')
    df, extracted_file = pipeline.extract_(files=raw_metadata)
    transformed_file = pipeline.transform_(df, selected_file=extracted_file)

    # Extract data for modeled files
    pipeline.start_sheets_service()
    logger.info(f"ETL process for modeled files in file '{group[0]}'")
    modeled_metadata = pipeline.extract_metadata_from_drive(target=group, data_layer='modeled')
    df, extracted_file = pipeline.extract_(files=modeled_metadata[group[0]])
    extracted_file = pipeline.transform_(dataframe=df, selected_file=extracted_file)

    # TODO: Compare both files, raw transformed and modeled. 

    # TODO: Generate a log for changes between raw transformed and modeled files

    # TODO: Load resulting dataframe into modeled sheets. Save modeled metadata ID.
    logger.info("Finishing ETL Process...")

# Import libraries
from loguru import logger
from src.transformation_ import FBSPreprocessing
from datetime import datetime
import polars as pl
from src.gdrive_handler import (
    get_drive_service, 
    read_metadata, 
    download_csv_into_dataframe, 
    get_gdrive_credentials_for_institutional_account
)
from src.gsheets_handler import (
    get_gsheets_credentials_for_institutional_account,
    get_sheets_service,
    download_data_from_sheets,
    write_dataframe_to_sheet
)
from src.log_handler import authlog_table, get_table_updated

# Create object to handle the ETL process
class ETLDataPipeline:

    output = {}
    layers = {'raw': 'crudos', 'modeled': 'modelados'}

    @classmethod
    def get_ouptut(self) -> dict:
        return self.output
    
    @classmethod
    def get_files_metadata(self, target_name: str) -> dict:
        return [d for d in self.metadata['all']['files'] if d['name'] == target_name][0]

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
        self.current_layer = data_layer
        return read_metadata(
            service=self.drive_service,
            target_drive_name='Planeacion',  
            target_parents=['3 Datos', self.layers[data_layer], None],
            target_folders=target,
            data_layer=self.current_layer,
        )

    @staticmethod
    def sort_and_get_most_recent(files: dict) -> dict:
        files = sorted(files['files'], key=lambda x: x['createdTime'], reverse=True)
        selected_file = files[0] if files else None
        return selected_file

    @classmethod
    def extract_(self, files: dict=None, target: str=None) -> tuple:
        df = pl.DataFrame()
        if self.current_layer == 'raw':
            selected_file = self.sort_and_get_most_recent(files)
            df = download_csv_into_dataframe(
                service=self.drive_service, 
                file_id=selected_file['id'], 
                is_shared_drive=True
            )
    
        elif self.current_layer == 'modeled':
            selected_file = [f for f in files['files'] if f['name'] == target[0]][0]
            logger.info(f"Found raw file: {selected_file['name']} with ID: {selected_file['id']}")
            df = download_data_from_sheets(
                service=self.sheets_service, 
                spreadsheet_id=selected_file['id'],
                range_name='Hoja 1'
            )
        logger.info(f"Extract: {self.current_layer} file {selected_file['name']} processed succesfully")
        return (df, selected_file)

    # Transform data
    @classmethod
    def transform_(self, dataframe: object, selected_file: dict) -> None:
        # Transform the data
        clean_name = selected_file['name']

        if self.current_layer == 'raw':
            if len(selected_file['name'].split("_")) > 1:
                clean_name = selected_file['name'].split("_")[1].split(".")[0]

        output_df = self.preprocessing_(input_df=dataframe, subject=clean_name, layer=self.current_layer)
        self.output[self.current_layer] = output_df
        logger.info(f"Transform: {self.current_layer} data from {clean_name} processed successfully.")

    @staticmethod
    def preprocessing_(input_df: pl.DataFrame, subject: str, layer: str) -> pl.DataFrame:
        preprocessing = FBSPreprocessing()
        method_name = f"{layer}_{subject}_"

        method_to_call = getattr(preprocessing, method_name, None)

        try:
            df = method_to_call(input_df)
        except Exception as e:
            raise(f"Target '{subject}' not recognized for transformation. Method or subject doesn't exist. Error: {e}")
            
        return df

    @classmethod
    def load_(self, df: pl.DataFrame, spreadsheet_id: str) -> None:
        api_response = write_dataframe_to_sheet(
            service=self.sheets_service,
            dataframe=df, 
            spreadsheet_id=spreadsheet_id,
            sheet_name="Hoja 1",
            clear_existing=True
        )
        logger.debug(f"Load files into google sheets, response={api_response}")

    @classmethod
    def run_(self, target: list, data_layer: str) -> None:
        self.metadata = self.read_metadata_from_drive(target=target, data_layer=data_layer)
        (df, selected_file) = self.extract_(files=self.metadata, target=target)
        self.transform_(dataframe=df, selected_file=selected_file)
        logger.info(f"ETL process for {data_layer} files finished.")


if __name__ == "__main__":
    logger.info("Starting ETL process...")
    pipeline = ETLDataPipeline()

    target = ['creditos']
    layers = ['raw', 'modeled']

    pipeline.start_drive_service()
    pipeline.start_sheets_service()

    for l in layers:
        pipeline.run_(target=target, data_layer=l)

    raw_file = pipeline.get_ouptut()['raw']
    modeled_file = pipeline.get_ouptut()['modeled']

    data_dict = pl.read_excel(source="data_dictionary/Diccionario_FBS.xlsx", sheet_name=target[0])
    primary_key_column = data_dict.filter(pl.col("Jerarquia") == 'PK')["Nombre_columna"][0]

    # TODO: VALIDATE THIS
    target_cols = list(data_dict["Nombre_columna"])
    modeled = pipeline.get_ouptut()['modeled']
    

    log_df = authlog_table(
        id_col=primary_key_column,
        df_a=pipeline.get_ouptut()['modeled'], 
        df_b=pipeline.get_ouptut()['raw'], 
        log_root=target[0]
    )

    output_df = get_table_updated(
        df_a=pipeline.get_ouptut()['modeled'], 
        df_b=pipeline.get_ouptut()['raw']
    )

    auth_meta = pipeline.get_files_metadata(target_name='auditoria')
    target_meta = pipeline.get_files_metadata(target_name=target[0])
    
    pipeline.load_(df=output_df, spreadsheet_id=target_meta['id'])
    pipeline.load_(df=log_df, spreadsheet_id=auth_meta['id'])
    logger.info("ETL Process finished...")

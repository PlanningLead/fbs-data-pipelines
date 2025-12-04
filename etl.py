# Import libraries
from loguru import logger
import polars as pl
import pandas as pd
from src.transformation_layer import transformer
from src.extraction_layer import extractor
from src.gdrive_handler import read_metadata
from src.gsheets_handler import write_dataframe_to_sheet
# from src.db_manager import db_admin
from src.log_handler import (
    authlog_table, 
    get_table_updated, 
    map_data_types
)
from dotenv import load_dotenv


# Load environment variables
load_dotenv()

# Create object to handle the ETL process
class ETLDataPipeline:
    
    output = {}
    layers = {'raw': 'crudos', 'modeled': 'modelados'}

    @classmethod
    def get_ouptut(self) -> dict:
        return self.output
    
    @classmethod
    def filter_files_metadata(self, target_name: str, layer: str) -> dict:
        if layer == "raw":
            return [d for d in self.metadata['files'] if d['name'].split("_")[1].split(".")[0] == target_name][0]
        if layer == "modeled":
            return [d for d in self.metadata['files'] if d['name'] == target_name][0]
        else:
            return {}

    @classmethod
    def get_metadata(self, target: list, data_layer: str) -> None:
        self.current_layer = data_layer
        self.metadata = read_metadata(
            service=extractor.drive_service,
            target_drive_name='Planeacion',  
            target_parents=['3 Datos', self.layers[data_layer], None],
            target_folders=target,
            data_layer=self.current_layer,
        )

    @classmethod
    def extract_(self, files: dict=None, target: str=None) -> None:
        method_name = f"{self.current_layer}_data_extraction"
        method_to_call = getattr(extractor, method_name, None)

        try:
            self.df, self.selected_file = method_to_call(files=files, layer=self.current_layer, target=target)
        except Exception as e:
            self.df, self.selected_file = None, None
            logger.error(f"Target '{target[0]}' not recognized for extraction. Method or subject doesn't exist. Error: {e}")

        logger.info(f"Extract: {self.current_layer} file {self.selected_file['name']} saved into polars dataframe {self.df.shape}")

    @classmethod
    def transform_(self) -> None:
        # Transform the data
        clean_name = self.selected_file['name']

        if self.current_layer == 'raw':
            if len(self.selected_file['name'].split("_")) > 1:
                clean_name = self.selected_file['name'].split("_")[1].split(".")[0]

        method_name = f"{self.current_layer}_{clean_name}_"
        method_to_call = getattr(transformer, method_name, None)

        try:
            df = method_to_call(self.df)
        except Exception as e:
            logger.error(f"Target '{clean_name}' not recognized for transformation. Method or subject doesn't exist. Error: {e}")
        
        logger.info(f"Transform: {self.current_layer} data from {self.selected_file['name']} processed successfully.")    
        # db_admin.create_duckdb_table_from_dataframe(data=df, table_name=f"{self.current_layer}_{clean_name}")
        self.output[self.current_layer] = df

    @classmethod
    def load_(self, df: pl.DataFrame, spreadsheet_id: str) -> None:
        api_response = write_dataframe_to_sheet(
            service=extractor.sheets_service,
            dataframe=df, 
            spreadsheet_id=spreadsheet_id,
            sheet_name="Hoja 1",
            clear_existing=True
        )
        logger.info(f"Load files into google sheets, response={api_response}")
  
# Main ETL process
if __name__ == "__main__":
    logger.info("Starting ETL process...")
    pipeline = ETLDataPipeline()

    target = ['creditos']
    layers = ['raw']

    # Check table list
    dict_name = "credit_data_dictionary"
    data_dictionary = pl.read_excel("data_dictionary/Diccionario_FBS.xlsx")    
    primary_key_column = data_dictionary.filter(pl.col("Jerarquia") == 'PK')["Nombre_columna"][0]

    # Run the ETL process for each layer
    for l in layers:
        pipeline.get_metadata(target=target, data_layer=l)
        pipeline.extract_(files=pipeline.metadata, target=target)
        pipeline.transform_()

    pipeline.get_metadata(target=target, data_layer="modeled")
    target_meta = pipeline.filter_files_metadata(target_name=target[0], layer="modeled")    
    
    pipeline.load_(df=pipeline.output["raw"], spreadsheet_id=target_meta['id'])
    logger.info("ETL Process finished...")





    # ------------------------

    # db_tables = db_admin.get_table_list()

    # if not (dict_name in db_tables):
    #     db_admin.create_duckdb_table_from_excel(
    #         data_path="data_dictionary/Diccionario_FBS.xlsx", 
    #         table_name=dict_name, 
    #         sheet_name=target[0]
    #     )
    #     data_dictionary = db_admin.get_polars_from_duckdb_table(table_name="credit_data_dictionary")
    # else:
    #     data_dictionary = db_admin.get_polars_from_duckdb_table(table_name=dict_name)


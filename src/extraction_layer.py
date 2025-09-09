import polars as pl
from src.gdrive_handler import (
    download_csv_into_polars,
    get_gdrive_credentials_for_institutional_account,
    get_drive_service
)

from src.gsheets_handler import (
    download_sheets_into_df,
    get_gsheets_credentials_for_institutional_account,
    get_sheets_service
)


class FBSExtractor:

    def __init__(self):
        self.start_drive_service()
        self.start_sheets_service()
    
    @classmethod
    def start_drive_service(self) -> None:
        creds = get_gdrive_credentials_for_institutional_account()
        self.drive_service = get_drive_service(creds=creds)
        
    @classmethod
    def start_sheets_service(self) -> None:
        creds = get_gsheets_credentials_for_institutional_account()
        self.sheets_service = get_sheets_service(creds=creds)


    def raw_data_extraction(self, files: dict, layer: str, target: list) -> tuple[str, dict]:
        # Sort and get most recent file
        files = sorted(files['files'], key=lambda x: x['createdTime'], reverse=True)
        selected_file = files[0] if files else None

        df = download_csv_into_polars(
                service=self.drive_service, 
                file_id=selected_file['id'],
                file_name=selected_file['name'].split("_")[-1].split(".")[0], 
                is_shared_drive=True,
                data_layer=layer
            )
        return df, selected_file


    def modeled_data_extraction(self, files: dict, layer: str, target: list) -> tuple[pl.DataFrame, dict]:
        # Get specific file by name or target
        selected_file = [f for f in files['files'] if f['name'] == target[0]][0]
        
        df = download_sheets_into_df(
            service=self.sheets_service, 
            spreadsheet_id=selected_file['id'],
            range_name='Hoja 1'
        )
        return df, selected_file


extractor = FBSExtractor()

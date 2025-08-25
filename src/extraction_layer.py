from src.gdrive_handler import download_csv_into_duckdb
from src.gsheets_handler import download_data_from_sheets


class FBSExtractor:

    @staticmethod
    def raw_data_extraction(service, files: dict, target: list) -> tuple:
        # Sort and get most recent file
        files = sorted(files['files'], key=lambda x: x['createdTime'], reverse=True)
        selected_file = files[0] if files else None

        table_name, db_table = download_csv_into_duckdb(
                service=service, 
                file_id=selected_file['id'],
                file_name=selected_file['name'], 
                is_shared_drive=True
            )
        return (table_name, db_table)

    @staticmethod
    def modeled_data_extraction(service, files: dict, target: list) -> None:
        # Get specific file by name or target
        selected_file = [f for f in files['files'] if f['name'] == target[0]][0]
        df = download_data_from_sheets(
            service=service, 
            spreadsheet_id=selected_file['id'],
            range_name='Hoja 1'
        )
        return (df, None)


extractor = FBSExtractor()

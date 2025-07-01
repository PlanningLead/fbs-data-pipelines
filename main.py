import polars as pl
from data_cleaning import FBSPreprocessing
from loguru import logger


if __name__ == '__main__':
    # add file name as input argument
    file_name = input("Enter the file name to process (e.g., 20250603_creditos.csv): ")
    if not file_name:
        logger.error("No file name provided. Exiting the program.")
        exit(1)
    if not file_name.endswith('.csv'):
        logger.error("File name must end with '.csv'. Exiting the program.")
        exit(1)
    else:
        logger.info(f"Using provided file name: {file_name}")

    preprocessing = FBSPreprocessing()

    df = pl.read_csv(f"{preprocessing.input_folder}/{file_name}", encoding='latin1', separator=';', ignore_errors=True)
    logger.info(f"Processing file: {file_name}")

    df = preprocessing.credit_preprocessing(df)
    preprocessing.save_results_into_local(file_name, df)

    logger.info("Data cleaning completed successfully.")

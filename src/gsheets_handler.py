import pickle
import polars as pl
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from loguru import logger
import urllib.parse
from src.utils_ import column_row_match_analyzer, column_row_shape_match 


# Asegúrate de incluir el scope para Google Sheets
SCOPES = [
    'https://www.googleapis.com/auth/drive.readonly',  # Para la parte de lectura de Drive si la necesitas
    'https://www.googleapis.com/auth/spreadsheets'     # NUEVO: Para escribir en Sheets
]
INSTITUTIONAL_EMAIL = 'cgarcia@fbscgr.gov.co'

def build_auth_url_for_specific_user(authorization_url):
    """
    Callback para añadir 'login_hint' a la URL de autorización.
    """
    # Parsear la URL para poder añadir o modificar parámetros
    parsed_url = urllib.parse.urlparse(authorization_url)
    query_params = urllib.parse.parse_qs(parsed_url.query)

    # Añadir el login_hint
    query_params['login_hint'] = [INSTITUTIONAL_EMAIL]
    
    # Reconstruir la URL
    new_query = urllib.parse.urlencode(query_params, doseq=True)
    new_url = parsed_url._replace(query=new_query).geturl()
    
    print(f"Abriendo URL de autenticación para {INSTITUTIONAL_EMAIL}:\n{new_url}")
    return new_url


def get_gsheets_credentials_for_institutional_account(token_path: str = 'credentials/sheets_token.pickle'):
    creds = None
    # El archivo token.pickle almacena los tokens de acceso y refresco del usuario
    if os.path.exists(token_path):
        with open(token_path, 'rb') as token:
            creds = pickle.load(token)
            logger_msg = f"Credenciales cargadas desde {token_path}"
    
    # Si no hay credenciales (válidas) o si las credenciales existentes no son del usuario deseado,
    # permite al usuario iniciar sesión.
    # También puedes forzar la re-autenticación eliminando token.pickle
    # para asegurar que siempre se presente la pantalla de selección de cuenta.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            logger_msg = f"Credenciales refrescadas para {token_path}"
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials/google_credentials.json', SCOPES)
            
            # --- ¡LA CLAVE ESTÁ AQUÍ! ---
            # run_local_server acepta el callback para modificar la URL antes de abrirla.
            creds = flow.run_local_server(
                port=0,
                authorization_url_callback=build_auth_url_for_specific_user,
                prompt='select_account',  # Esto fuerza la pantalla de selección de cuenta
            )
        # Guarda las credenciales para la próxima ejecución
        logger_msg = f"Nuevas credenciales para Google Drive guardadas en {token_path}"
        with open(token_path, 'wb') as token:
            pickle.dump(creds, token)
    logger.debug(logger_msg)
    # Retorna las credenciales
    return creds


def get_sheets_service(creds=None):
    """Autentica y devuelve el objeto de servicio de Google Sheets."""
    if creds is None:
        logger.error("Credenciales no proporcionadas. Llama a get_google_credentials_for_institutional_account primero.")
    else:
        return build('sheets', 'v4', credentials=creds)


def download_data_from_sheets(service: object, spreadsheet_id: str, range_name: str):
    """
    Lee datos de un rango específico de una Google Sheet y los devuelve como un DataFrame de pandas.

    Args:
        spreadsheet_id (str): El ID de la hoja de cálculo de Google.
        range_name (str): El rango de celdas a leer (ej., 'Sheet1!A1:C10', 'Datos!A:A').
    Returns:
        pd.DataFrame: Un DataFrame de pandas con los datos, o None si hay un error o no hay datos.
    """

    try:
        # Llama a la API para obtener los valores del rango especificado
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            # valueRenderOption='FORMULA',
            valueRenderOption='FORMATTED_VALUE', # Lee los valores tal como los ves en la hoja
            # dateTimeRenderOption='FORMATTED_STRING' # Las fechas/horas se devuelven como strings
        ).execute()

        values = result.get('values', [])

        if not values:
            logger.warning(f"No se encontraron datos en el rango '{range_name}' de la hoja '{spreadsheet_id}'.")
            return pl.DataFrame() # Devuelve un DataFrame vacío
        
        # La primera fila serán los encabezados (asumiendo que tu hoja tiene encabezados)
        headers = values[0]
        # El resto serán los datos
        data = values[1:]

        # --- APLICAR RELLENO DE CELDAS VACÍAS AQUÍ ---
        shape_match_rate = column_row_match_analyzer(sample_size=20, headers=headers, data=data)

        if shape_match_rate < 1:
            data = column_row_shape_match(headers=headers, data=data)
        
        # Crea el DataFrame de pandas
        df = pl.DataFrame(data, schema=headers, nan_to_null=True, orient='row')
        logger.debug(f"Datos leídos con éxito. Dimensiones del DataFrame: {df.shape}")
        return df

    except Exception as e:
        logger.error(f"Error al leer datos de la hoja de cálculo '{spreadsheet_id}' en el rango '{range_name}': {e}")
        return None
    

def write_dataframe_to_sheet(service, dataframe, spreadsheet_id, sheet_name='Sheet1', start_cell='A1', clear_existing=True) -> dict:
    """
    Escribe un DataFrame de pandas en una Google Sheet existente.

    Args:
        dataframe (pd.DataFrame): El DataFrame a escribir.
        spreadsheet_id (str): El ID de la hoja de cálculo de Google.
        sheet_name (str): El nombre de la hoja (pestaña) dentro de la hoja de cálculo.
                          Por defecto es 'Sheet1'.
        start_cell (str): La celda de inicio para escribir los datos (ej., 'A1', 'B2').
                          Por defecto es 'A1'.
        clear_existing (bool): Si es True, borra el rango especificado antes de escribir.
                               Recomendado para evitar datos antiguos.
    Returns:
        dict: La respuesta de la API de Sheets o None si hay un error.
    """
    # Convertir el DataFrame a una lista de listas (incluyendo los encabezados)
    # Esto es el formato que la API de Sheets espera Convertir a str para evitar problemas de tipos
    data_to_write = dataframe.with_columns(pl.all().cast(pl.String)).rows()
    # Añadir los encabezados del DataFrame
    data_to_write.insert(0, dataframe.columns)

    # Definir el rango donde se escribirán los datos
    # Por ejemplo, si start_cell es 'A1' y sheet_name es 'Datos', el rango sería 'Datos!A1'
    range_name = f"{sheet_name}!{start_cell}"

    try:
        # 1. (Opcional) Borrar el contenido existente en el rango
        if clear_existing:
            logger.debug(f"Limpiando rango '{range_name}' en la hoja '{spreadsheet_id}'...")
            clear_body = {} # Un cuerpo vacío significa borrar todo el rango
            request = service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id, 
                range=range_name, 
                body=clear_body
            )
            response = request.execute()
            logger.warning(f"Rango limpiado: {response.get('clearedRange')}")

        # 2. Escribir los nuevos datos
        body = {
            'values': data_to_write
        }
        # valueInputOption: RAW significa que los valores se escriben tal cual.
        # USER_ENTERED significa que Sheets intentará interpretar el valor (ej. '1/2' como fecha).
        # Para DataFrames, RAW es generalmente lo que quieres.
        result = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption='RAW', # 'USER_ENTERED' o 'RAW'
            body=body
        ).execute()
        
        logger.debug(f"{result.get('updatedCells')} celdas actualizadas en la hoja '{sheet_name}'.")
        return result

    except Exception as e:
        print(f"Error al escribir en la hoja de cálculo '{spreadsheet_id}': {e}")
        return None

# --- Ejemplo de uso ---
if __name__ == '__main__':
    print('Hola mundo')

import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from loguru import logger
import urllib.parse

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


def get_google_credentials_for_institutional_account(token_path: str = 'drive_token.pickle'):
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
            flow = InstalledAppFlow.from_client_secrets_file(
                'google_credentials.json', SCOPES)
            
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
    logger.info(logger_msg)
    # Retorna las credenciales
    return creds


def get_sheets_service(creds=None):
    """Autentica y devuelve el objeto de servicio de Google Sheets."""
    if creds is None:
        logger.error("Credenciales no proporcionadas. Llama a get_google_credentials_for_institutional_account primero.")
    else:
        return build('sheets', 'v4', credentials=creds)


def write_dataframe_to_sheet(dataframe, spreadsheet_id, sheet_name='Sheet1', start_cell='A1', clear_existing=True):
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
    service = get_sheets_service()

    # Convertir el DataFrame a una lista de listas (incluyendo los encabezados)
    # Esto es el formato que la API de Sheets espera
    data_to_write = dataframe.astype(str).values.tolist() # Convertir a str para evitar problemas de tipos
    # Añadir los encabezados del DataFrame
    data_to_write.insert(0, dataframe.columns.tolist())

    # Definir el rango donde se escribirán los datos
    # Por ejemplo, si start_cell es 'A1' y sheet_name es 'Datos', el rango sería 'Datos!A1'
    range_name = f"{sheet_name}!{start_cell}"

    try:
        # 1. (Opcional) Borrar el contenido existente en el rango
        if clear_existing:
            print(f"Limpiando rango '{range_name}' en la hoja '{spreadsheet_id}'...")
            clear_body = {} # Un cuerpo vacío significa borrar todo el rango
            request = service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id, 
                range=range_name, 
                body=clear_body
            )
            response = request.execute()
            print(f"Rango limpiado: {response.get('clearedRange')}")

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
        
        print(f"{result.get('updatedCells')} celdas actualizadas en la hoja '{sheet_name}'.")
        return result

    except Exception as e:
        print(f"Error al escribir en la hoja de cálculo '{spreadsheet_id}': {e}")
        return None

# --- Ejemplo de uso ---
if __name__ == '__main__':
    print('Hola mundo')
    # 1. Crear un DataFrame de ejemplo (reemplaza esto con tu DataFrame real modelado)
    # data = {
    #     'Producto': ['Laptop', 'Mouse', 'Teclado', 'Monitor'],
    #     'Cantidad': [10, 50, 30, 5],
    #     'Precio_USD': [1200.50, 25.00, 75.99, 300.00],
    #     'Fecha_Actualizacion': [
    #         pd.to_datetime('2025-07-01'), 
    #         pd.to_datetime('2025-06-28'), 
    #         pd.to_datetime('2025-07-01'), 
    #         pd.to_datetime('2025-06-30')
    #     ]
    # }
    # df_to_write = pd.DataFrame(data)
    
    # print("DataFrame a escribir:")
    # print(df_to_write)

    # # 2. Configura el ID de tu hoja de cálculo y el nombre de la pestaña
    # # --- ¡CAMBIA ESTO CON TUS VALORES REALES! ---
    # target_spreadsheet_id = '1AbcDEfGhiJkLmNoPqRsTuVwXyZ0123456789' # ID de tu Google Sheet
    # target_sheet_name = 'Datos Procesados' # Nombre de la pestaña (ej. 'Sheet1', 'Hoja1', 'Resultados')
    # start_cell_to_write = 'A1' # Celda donde empezar a escribir (ej. 'A1' para toda la hoja)
    
    # # 3. Llamar a la función para escribir el DataFrame
    # if target_spreadsheet_id == '1AbcDEfGhiJkLmNoPqRsTuVwXyZ0123456789':
    #     print("\n*** ADVERTENCIA: Por favor, actualiza 'target_spreadsheet_id' con un ID real de tu Google Sheet para probar. ***\n")
    # else:
    #     print(f"\nIntentando escribir DataFrame en Google Sheet ID: {target_spreadsheet_id}, Pestaña: '{target_sheet_name}', Celda de inicio: '{start_cell_to_write}'")
    #     write_response = write_dataframe_to_sheet(
    #         df_to_write,
    #         target_spreadsheet_id,
    #         sheet_name=target_sheet_name,
    #         start_cell=start_cell_to_write,
    #         clear_existing=True # Esto es crucial para reemplazar datos
    #     )
        
    #     if write_response:
    #         print("\nDataFrame escrito con éxito en Google Sheets.")
    #     else:
    #         print("\nError al escribir DataFrame en Google Sheets.")
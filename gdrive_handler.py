# Retrieve data from google drive and load it into a polars dataframe
import polars as pl
from datetime import date
import io, os.path
from googleapiclient.http import MediaIoBaseDownload
from loguru import logger
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request


# Solo lectura de metadatos
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']


def get_drive_service():
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
            logger_msg = "Credenciales cargadas desde token.pickle"
            
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            logger_msg = "Credenciales refrescadas"
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
            logger_msg = "Nuevas credenciales guardadas en token.pickle"
    logger.info(logger_msg)
    return build('drive', 'v3', credentials=creds)


def list_all_shared_drives(service: object = None):
    """Lista todas las Unidades Compartidas a las que el usuario tiene acceso."""
    
    # Parámetros para la paginación
    page_token = None
    all_shared_drives = []

    while True:
        try:
            # drives().list() se usa para obtener Shared Drives
            results = service.drives().list(
                fields="nextPageToken, drives(id, name)",
                pageToken=page_token
            ).execute()
            
            items = results.get('drives', [])
            all_shared_drives.extend(items)
            
            page_token = results.get('nextPageToken', None)
            if not page_token:
                break # No hay más páginas

        except Exception as e:
            logger.error(f"Error al listar Unidades Compartidas: {e}")
            break
            
    if not all_shared_drives:
        logger.warning('No se encontraron Unidades Compartidas a las que tengas acceso.')
    
    return all_shared_drives


def list_files_and_folders(service, location_id=None, is_shared_drive=False, page_size=100, file_type=None, search_name=None):
    page_token = None
    all_files = []
    
    # Construir la consulta 'q'
    query_parts = ["trashed = false"] # Siempre excluimos la papelera

    if location_id:
        # Si se proporciona un location_id, buscamos elementos donde este ID sea un padre.
        query_parts.append(f"'{location_id}' in parents")

    else:
        pass

    if file_type:
        query_parts.append(f"mimeType = '{file_type}'")

    if search_name:
        query_parts.append(f"name = '{search_name}'")
    
    # Unir todas las partes de la consulta
    full_query = " and ".join(query_parts)
    while True:
        try:
            results = service.files().list(
                pageSize=page_size,
                fields="nextPageToken, files(id, name, mimeType, parents)",
                includeItemsFromAllDrives=is_shared_drive, # Incluir si es Unidad Compartida
                supportsAllDrives=is_shared_drive,         # Necesario para el anterior
                q=full_query,
                pageToken=page_token
            ).execute()
            
            items = results.get('files', [])
            all_files.extend(items)
            
            # Imprimir progreso si hay muchos archivos
            if len(all_files) % 1000 == 0 and len(all_files) > 0:
                print(f"  Archivos encontrados hasta ahora en esta ubicación: {len(all_files)}")

            page_token = results.get('nextPageToken', None)
            if not page_token:
                break

        except Exception as e:
            logger.error(f"Error al listar archivos en la ubicación {location_id}: {e}")
            break
            
    if not all_files:
        logger.warning('No se encontraron archivos o carpetas en la ubicación especificada.')

    return all_files


def read_metadata(service, target_drive_name: str=None, target_parents: list=[], target_folders: list=[]) -> list:
    # Get the ids and names of all shared drives
    shared_drives = list_all_shared_drives(service=service)

    # Find the ID with the target drive name
    target_drive_id = None
    
    for drive in shared_drives:
        if drive['name'] == target_drive_name:
            target_drive_id = drive['id']
            logger.info(f"Shared Drive '{target_drive_name}' found with ID: {target_drive_id}")
            break

    # Get all files and folders in the shared drive
    # Si no se proporciona un location_id, buscamos elementos en la raíz de Mi Unidad que no tienen padres (ya que un elemento en la raíz de Mi Unidad no tiene un parent_id).
    # Esto es más complejo de manejar con la API directamente para la raíz de Mi Unidad sin un parent_id específico. Una forma común de buscar en la raíz de Mi Unidad es buscar por 'root' en parents.
    # Sin embargo, el comportamiento de 'root' varía. Para simplicidad, si location_id es None, la función buscará en todos los archivos del usuario, pero puedes refinarlo para 'root' en 'parents' para Mi Unidad, lo cual es más específico y similar a la navegación web.

    for p in target_parents:
        if target_drive_id:
            files_and_folders = list_files_and_folders(
                service, 
                location_id=target_drive_id, 
                is_shared_drive=True,
                search_name=p
            )
        target_drive_id = files_and_folders[0]['id'] if p else None

    files_dict = {}
    for f in target_folders:
        # Search for a column value in a list of dictionaries
        folder_match = next((d for d in files_and_folders if d.get("name") == f), None)
        target_files = list_files_and_folders(
            service,
            location_id=folder_match['id'], 
            is_shared_drive=True,
            search_name=None
        )
        # Append to dictionary for each folder
        files_dict[f] = {'location_id': folder_match['id'], 'files': target_files} 
    logger.info("Data files and folders found successfully.")
    return files_dict


def download_file_into_dataframe(service, file_id, is_shared_drive=False):
    try:
        # Request para descargar el archivo.
        # supportsAllDrives es crucial si el archivo está en una Unidad Compartida.
        request = service.files().get_media(
            fileId=file_id,
            supportsAllDrives=is_shared_drive
        )

        # Objeto para manejar la descarga en chunks.
        # Create the BytesIO object that will receive the downloaded data
        download_buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(download_buffer, request)
        done = False

        while not done:
            status, done = downloader.next_chunk()

            # Add progress bar using tqdm library if needed
            logger.debug(f"Descargando {file_id}... Progreso: {int(status.progress() * 100)}%")
        logger.debug(f"\nDescarga de '{file_id}' completada. Tamaño en memoria: {download_buffer.tell()} bytes.")

        raw_bytes_content = download_buffer.getvalue() 

        polars_buffer = io.BytesIO(raw_bytes_content)
        polars_buffer.seek(0)  # Reset the buffer position to the beginning
        # Read the content directly from the new BytesIO object with pandas
        df = pl.read_csv(polars_buffer, encoding='latin-1', separator=';')
        return df

    except Exception as e:
        logger.error(f"Error al descargar el archivo '{file_id}': {e}")
        return False


if __name__ == '__main__':
    # Get the Google Drive service
    service = get_drive_service()

    target_folders = ['credito', 'radicados', 'funcionariosCGR']
    files = read_metadata(
        service=service,
        target_drive_name='Planeacion',  
        target_parents=['3 Datos', 'Crudos', None],
        target_folders=target_folders
    )

    for folder, file_dict in files.items():

        logger.debug(f"De la carpeta: {folder} se encontraron: {len(file_dict['files'])} archivos.")
        file_to_download = file_dict['files'][0] # Tomamos el primer CSV encontrado

        # 2. Define la ruta local donde guardar el CSV
        download_file_name = file_to_download['name'].split(".")[0].split("_")[-1]
        local_download_path = os.path.join(os.getcwd(), 'temp_data', f"{download_file_name}.parquet") # Guarda en el mismo directorio del script

        # 3. Descarga el archivo
        df = download_file_into_dataframe(service, file_to_download['id'], local_download_path, is_shared_drive=True)
        
        logger.debug(f"Archivo {file_to_download['name']} descargado a dataframe: Shape=({df.shape})")

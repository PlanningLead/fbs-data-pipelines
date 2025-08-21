import polars as pl
from datetime import datetime, date
import uuid
from loguru import logger


def map_data_types(dictionary, df):
    type_map = {
        "Integer": pl.Int64,
        "String": pl.String,
        "Timestamp": pl.Datetime,
        "Float": pl.Float64,
        "Bool": pl.String
        # Puedes añadir más tipos de datos aquí según lo necesites
    }

    tipos_en_string = {dictionary[i]["Nombre_columna"][0]: dictionary[i]["Tipo"][0] for i in range(dictionary.shape[0])}

    expresiones_de_cast = []
    for col, dtype_str in tipos_en_string.items():
        if col in df.columns:
            if dtype_str != "Timestamp":
                # Si la columna existe, creamos la expresión de cast y la agregamos a la lista
                expresion = pl.col(col).cast(type_map[dtype_str])
                expresiones_de_cast.append(expresion)
            else:
                # check if element is string or not
                if not isinstance(df[col].to_list()[0], date):
                    expresion = pl.col(col).str.strptime(type_map[dtype_str], format="%d/%m/%Y")
                    expresiones_de_cast.append(expresion)
                else:
                    expresion = pl.col(col).cast(type_map[dtype_str])
                    expresiones_de_cast.append(expresion)
        else:
            # Si la columna no existe, la omitimos y puedes imprimir un mensaje
            logger.warning(f"⚠️ Aviso: La columna '{col}' no se encontró y se omitirá del proceso de cast.")

    # 4. Aplicar los casts a las columnas que existen
    df_casteado = df.with_columns(expresiones_de_cast)
    return df_casteado


def authlog_table(df_raw, df_modeled, log_root: str, id_col: str, target_cols: list):
    # ----------------- Proceso de Detección ----------------
    if df_raw.shape[1] != df_modeled.shape[1]:
        logger.warning(f"Dimensions from modeled and raw data: modeled={df_modeled.shape} / raw={df_raw.shape}")

    # 1. Si la data cruda tiene mayor tamaño, 
    try:
        joined_df = df_raw.join(df_modeled, on=id_col, how='inner', suffix='_modeled')
    except Exception as e:
        logger.error(f"Failed join between raw and modeled. Error={e}")

    # Crea una expresión booleana que sea True si hay un cambio en CUALQUIER columna
    cambios_detectados_exp = pl.lit(False)
    for col in target_cols:
        cambios_detectados_exp = cambios_detectados_exp | (
            pl.col(col).is_not_null() & pl.col(f'{col}_modeled').is_not_null() & (pl.col(col) != pl.col(f'{col}_modeled'))
        )

    # 3. Filtrar para obtener el DataFrame de logs
    logs_df = joined_df.filter(
        (pl.col(f'{id_col}').is_not_null() & cambios_detectados_exp) # Registros modificados
    ).with_columns(
        # Nueva columna 'id_log' con un UUID único para cada fila
        pl.lit(str(uuid.uuid4()), dtype=pl.String).alias('id_log'),
        # Nueva columna 'fecha_modificacion' con la fecha y hora actual
        pl.lit(datetime.now(), dtype=pl.Datetime).alias('fecha_modificacion'),
        # Nueva columna 'fuente_de_log' con la naturaleza de la fuente
        pl.lit(log_root, dtype=pl.String).alias('fuente_log'),
        # Columna 'tipo_cambio' para categorizar el cambio
        pl.when(pl.col(f'{id_col}').is_null()).then(pl.lit('Nuevo'))
        .when(pl.col(f'{id_col}_b').is_null()).then(pl.lit('Eliminado'))
        .otherwise(pl.lit('Modificado'))
        .alias('tipo_cambio')
        )

    # Reordenar las columnas para mejor legibilidad
    cols_in_order = [
        'id_log',
        'fecha_modificacion',
        'tipo_cambio',
        'fuente_log',
        f'{id_col}', 
        f'{id_col}_modeled'
    ]

    for t in target_cols:
        cols_in_order.append(t)
        cols_in_order.append(f"{t}_modeled")
    
    
    logs_df = logs_df.select(cols_in_order)
    return logs_df

# ----------------- Proceso de Actualización -----------------
# 1. Identificar los `id_registro` de los registros que no han cambiado
#    Unimos ambos DataFrames con un join interno (inner) y filtramos por los que NO han cambiado.
def get_table_updated(df_a, df_b):
    id_col = 'Radicado'
    target_cols = ['Rpta', 'funcionario_destino']
    unchanged_ids = df_a.join(df_b, on=id_col, how='inner', suffix='_b').filter(
        ~(pl.col(f'{target_cols[0]}') != pl.col(f'{target_cols[0]}_b')) & ~(pl.col(f'{target_cols[1]}') != pl.col(f'{target_cols[1]}_b'))
    ).select(id_col).unique()

    # 2. Mantener solo los registros de 'A' que no han cambiado
    df_a_unchanged = df_a.join(unchanged_ids, on=id_col, how='semi')

    # 3. Mantener solo los registros de 'B' que son nuevos o modificados
    df_b_to_add = df_b.join(unchanged_ids, on=id_col, how='anti')

    # 4. Concatenar los registros no cambiados de A con los nuevos/modificados de B
    df_a_unchanged = df_a_unchanged.rename({'fecha_solicitud': 'Fecha Radicacion'})
    # 2. Obtener el orden de las columnas de la tabla A
    column_order = df_b_to_add.columns

    # 3. Usar .select() para reordenar las columnas del DataFrame B
    df_a_unchanged = df_a_unchanged.select(column_order)
    df_a_strings = df_a_unchanged.with_columns([
        pl.col(col).cast(pl.String) for col in column_order
    ])

    df_b_strings = df_b_to_add.with_columns([
        pl.col(col).cast(pl.String) for col in column_order
    ])
    df_actualizado = pl.concat([df_a_strings, df_b_strings])

    return df_actualizado.sort(id_col)


if __name__ == '__main__':
    # DataFrame A (datos ya publicados)
    df_a = pl.DataFrame({
        'id_registro': [1, 2, 3, 4],
        'valor1': ['A', 'B', 'C', 'D'],
        'valor2': [100, 200, 300, 400]
    })

    # DataFrame B (datos nuevos, con cambios y un registro nuevo)
    df_b = pl.DataFrame({
        'id_registro': [1, 2, 3, 5],
        'valor1': ['A', 'B_cambiado', 'C', 'E'],  # 'B' ha cambiado a 'B_cambiado'
        'valor2': [100, 205, 300, 500]             # 'valor2' para 'B' también cambió
    })

    logs_df = authlog_table(df_a, df_b, 'radicados')
    updated_df = get_table_updated(df_a, df_b)

    print("end of code")
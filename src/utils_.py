import random
from datetime import datetime


def column_row_match_analyzer(sample_size: int, headers: list, data: list):

    random_rows = random.choices(k=sample_size, population=range(len(data)))
    output = []
    for r in random_rows:
        output.append(len(data[r]) == len(headers))
    
    null_value_rate = 1 - (sum(output) / sample_size)
    return float(round(null_value_rate, 3))


def column_row_shape_match(headers: list, data: list):
    num_columns = len(headers)
    processed_data = []
    for row in data:
        # Asegura que cada fila tenga la misma cantidad de columnas que los encabezados
        # Si la fila es más corta, rellena con None para las columnas faltantes
        if len(row) < num_columns:
            row.extend([None] * (num_columns - len(row)))
        processed_data.append(row)
    return processed_data

def adjust_date_format(date_string, format_string):
    return datetime.strptime(date_string, format_string)
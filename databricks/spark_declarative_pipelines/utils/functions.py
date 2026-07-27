from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, DoubleType, StringType, BooleanType, TimestampType, MapType
from logger.logger import *
from pyspark.sql import types as ty

import json
import types

def set_utc_ingestion_date(landing_data):
  return landing_data.withColumn('utc_ingestion_date', current_timestamp())

def set_date_from_config(df, partition_date, column_name = 'date'):
  if partition_date != '':
    return df.withColumn(column_name, to_date(substring(partition_date, 0, 10)))
  else:
    return df

def get_schema(schema_dict: dict) -> str:
  """
  Función para retornar el esquema en formato string en base de un diccionario.

  Args:
    schema_dict (dict): Esquema del dataframe en formato de diccionario.

  Returns:
      str: Retorna el esquema en formato string.
  """
  schema_string = ''
  for index, item in enumerate(schema_dict):
    
    if item['type'].upper().startswith('MAP'):
      schema_string += f"{item['name']} {item['type'].upper().replace('_', ',').replace(' ', '')}"

    else:
      schema_string += f"{item['name']} {item['type'].upper()}"

    if 'generated_col' in item and len(item['generated_col']) > 0:
      schema_string += f" GENERATED ALWAYS AS ({item['generated_col']})"
    if 'is_nullable' in item and not item['is_nullable']:
      schema_string += " NOT NULL"
    if index < len(schema_dict) - 1:
      schema_string += ', '

  return schema_string

def load_config_paths(base_repo_path: str, utils_json_path: str) -> list:
  """
  Función para retornar la configuración completa de las tablas que se encuentran en los archivos JSON.

  Args:
    base_repo_path (str): Ruta base correspondiente al repositorio del troncal de la división
    utils_json_path (str): Ruta relativa del archivo JSON que contiene todas las rutas de las tablas.

  Returns:
      list: Retorna una lista con todas las configuraciones de las tablas, el cual se detalla la frecuencia del trigger
            columna utilizada para realizar consulta SQL, si corresponde a una carga completa, ventana de tiempo a utilizar para la carga incremental.
  """
  path_list = []

  json_file = read_json(base_repo_path + '/' + utils_json_path)
  
  for index in range(0, len(json_file['json_paths'])):
    if (json_file['json_paths'][index].get('table_name') is not None and
      json_file['json_paths'][index].get('compute_type') is not None):
        table_config = {}
        table_config['table_name'] = json_file['json_paths'][index]['table_name']
        table_config['path'] = base_repo_path + '/' + json_file['json_paths'][index]['path']
        table_config['compute_type'] = json_file['json_paths'][index]['compute_type']
        path_list.append(table_config)
    else:
      path_list.append(base_repo_path + '/' + json_file['json_paths'][index]['path'])

  return path_list

def merge_two_dicts(x: dict, y: dict) -> dict:
  z = x.copy()
  z.update(y)
  return z

def get_expectations(expectations_repo_source, expectations_repo_division, table_config, expect_type):    
  rules_source = get_rules(expectations_repo_source, table_config, 'source', expect_type)
  rules_division = get_rules(expectations_repo_division, table_config, 'faena', expect_type)
    
  return merge_two_dicts(rules_source, rules_division)

def get_rules(path_repo_json, table_config, expect_hierarchy_level, expect_type, path_config_expectations=None):
  faena = table_config['faena']
  data_source = table_config['data_source']
  stage = table_config['output']['stage']
  rules = {}

  if expect_hierarchy_level == 'source' and not path_config_expectations:
    json_file = open(f'{path_repo_json}/{data_source}/{stage}/expectations/config-sdp-expectations-{stage}-{data_source}.json')
  elif expect_hierarchy_level == 'faena' and not path_config_expectations:
    json_file = open(f'{path_repo_json}/{data_source}/{stage}/expectations/config-sdp-expectations-{stage}-{data_source}-{faena}.json')
  elif path_config_expectations:
    json_file = open(f'{path_repo_json}/{path_config_expectations}')

  data = json.load(json_file)

  for row in data['rules']:
    if (row['table'] == table_config['topic'] and 
        row['expect_type'] == expect_type and 
        row['stage'] == stage and
        row['active']):
      rules[row['description'] + f' (level: {expect_hierarchy_level})'] = row['expect_expresion']

  json_file.close()

  return rules

def get_nested_schema(output_schema_dict, output_nested_schema_dict=None):
  """
  Genera un esquema en SQL a partir de una lista de columnas y sus tipos de datos,
  soportando tipos básicos (string, integer, etc.) y tipos complejos (struct, array).
  El segundo diccionario (output_nested_schema_dict) es opcional para definir estructuras anidadas.

  Args:
    output_schema_dict (list[dict]): Lista de diccionarios que definen las columnas del esquema.
      Cada diccionario debe tener las siguientes claves:
        - "name" (str): Nombre de la columna.
        - "type" (str): Tipo de dato de la columna (e.g., "string", "integer", "struct", "array").
        - "is_nullable" (bool, opcional): Si la columna es nullable (opcional, por defecto es True).
        - "generated_col" (str): Expresión opcional para columnas generadas automáticamente.
        - "partition_order" (int): Orden de partición (opcional, no se usa en la lógica actual).

    output_nested_schema_dict (list[dict], optional): Lista de diccionarios que definen la estructura anidada
      para columnas tipo struct o array. Si no se proporciona, las columnas de tipo
      struct o array serán tratadas como columnas básicas sin estructura interna.
      Cada diccionario debe tener las siguientes claves:
        - "ns" (str): Nombre del espacio de nombres asociado a la columna en output_schema_dict.
        - "name" (str): Nombre del campo dentro del espacio de nombres.
        - "type" (str): Tipo de dato del campo.

  Returns:
      str: Una cadena que representa el esquema SQL generado. Incluye las definiciones de columnas
      con sus tipos de datos y cláusulas de columnas generadas cuando corresponda.
  """
  nested_structures = {}
  if output_nested_schema_dict:
    for item in output_nested_schema_dict:
      ns = item['ns']
      if ns not in nested_structures:
        nested_structures[ns] = []
      nested_structures[ns].append(f"{item['name']} {item['type'].upper()}")

  schema_string = ""
  for index, item in enumerate(output_schema_dict):
    column_name = item['name']
    column_type = item['type'].lower()

    if column_type in ['struct', 'array']:
      if output_nested_schema_dict and column_name in nested_structures:
        nested_fields = ", ".join(nested_structures[column_name])
        if column_type == 'struct':
          schema_string += f"{column_name} STRUCT<{nested_fields}>"
        elif column_type == 'array':
          schema_string += f"{column_name} ARRAY<STRUCT<{nested_fields}>>"
      else:
        schema_string += f"{column_name} {column_type.upper()}"
    else:
      schema_string += f"{column_name} {column_type.upper()}"

    if len(item['generated_col']) > 0:
      schema_string += f" GENERATED ALWAYS AS ({item['generated_col']})"

    if 'is_nullable' in item and not item['is_nullable']:
      schema_string += " NOT NULL"

    if index < len(output_schema_dict) - 1:
      schema_string += ", "

  return schema_string

def read_json(file_path: str) -> dict:
  """
  Función para leer un archivo JSON desde el Datalake y retornarlo como un diccionario.

  Args:
    file_path (string): Ruta del archivo JSON.

  Returns:
      dict: Un diccionario con todos los valores del archivo JSON.
  """
  with open(file_path, 'r') as file:
      return json.load(file)

def get_partitions(output_schema_dict: dict) -> list:
  """
  Función para generar lista con las columnas de partición del Delta.

  Args:
    output_schema_dict (dict): Esquema del dataframe en formato de diccionario.

  Returns:
      partition_list: Una lista con las columnas que se utilizarán para particionar los datos en el Delta.
  """
  partition_dict = {}  

  for index, item in enumerate(output_schema_dict):    
    if 'partition_order' in item and item['partition_order'] != 0:
      values_list = []
      values_list.append(item['name'])
      values_list.append(item['partition_order'])
      partition_dict[index] = values_list
      
  sorted_dict = dict(sorted(partition_dict.items(), key=lambda item: item[1]))    

  partition_list = [value[0] for value in sorted_dict.values()]

  return partition_list

def get_schema_map_type(type_str: str):
  """
  Función para mapear los tipos de datos de esquema en formato string a tipo de datos de PySpark.

  Args:
    type_str (str): Tipo de dato del esquema en formato string.

  Returns:
      pyspark.sql.types: Retorna el tipo de dato pero en formato PySpark.
  """
  type_str = type_str.replace(' ', '').upper()
  
  if type_str == 'INTEGER':
      return IntegerType()
  elif type_str == 'STRING':
      return StringType()
  elif type_str == 'DATE':
      return DateType()
  elif type_str == 'TIMESTAMP':
      return TimestampType()
  elif type_str == 'FLOAT':
      return FloatType()
  elif type_str == 'DOUBLE':
      return DoubleType()
  elif type_str == 'BOOLEAN':
      return BooleanType()
  elif type_str == 'MAP<STRING,DOUBLE>':
      return MapType(StringType(), DoubleType())
  elif type_str == 'MAP<STRING,BOOLEAN>':
      return MapType(StringType(), BooleanType())
  else:
      raise ValueError(f"Unsupported type: {type_str}")

def get_schema_structype(schema_str: str):
  """
  Función para convertir el esquema desde formato string a tipo StructType (sin la propiedad de nullable)

  Args:
    schema_str (str): Esquema del dataframe en formato string.

  Returns:
      StructType: Esquema del dataframe en formato StructType.
  """
  schema_str = schema_str.strip("'")
  fields = schema_str.split(", ")

  fields_list = []
  for field in fields:
    if ' NOT NULL' in field:
      field = field.replace(' NOT NULL', '')

    name, data_type = field.split(' ')
      
    if 'NOT NULL' in data_type:
      data_type = data_type.replace("NOT NULL", "").strip()
      
    fields_list.append(StructField(name, get_schema_map_type(data_type), nullable=True))

  return StructType(fields_list)

def get_bad_records_path(config: dict, environment: str) -> str:
  """
  Función para retornar la ruta del directorio donde se almacenarán los registros erróneos.

  Args:
    config (dict): Diccionario con las configuraciones del JSON.
    environment (string): Ambiente de databricks a utilizar.

  Returns:
      string: Ruta del directorio donde se almacenarán los registros erróneos.
  """
  external_location_env = f'lakecoreeastus2{environment}01'

  return (f"abfss://{config['output']['stage']}@{external_location_env}.dfs.core.windows.net/" \
          f"{config['division']}/" \
          f"{config['process']}/" \
          f"{config['data_source']}/bad_records/" \
          f"{config['topic']}")

def exec_custom_functions(module_ref: types.ModuleType, data: dict, config: dict, parent_function:str = 'parent_function') -> DataFrame:
  """
  Función para ejecutar las funciones personalizadas.

  Args:
    module_ref (types.ModuleType): Referencia a las funciones personalizadas que son importadas desde el notebook que ejecuta la presente función.
    data (dict): Diccionario con los datos de entrada.
    config (dict): Diccionario con las configuraciones de la tabla.

  Returns:
      pyspark.sql.connect.dataframe.DataFrame: Retorna un Dataframe de PySpark.
  """
  
  logger = Logger(name='custom_functions')  

  try:
    if hasattr(module_ref, parent_function):
      logger.info(f"{parent_function} found")
      function = getattr(module_ref, parent_function)      
      df = function(data=data, config=config)
      logger.info(f"{parent_function} executed")

    else:
      logger.info(f"module {module_ref} or parent function {parent_function} doesn't exist")

  except Exception as e:
    logger.error(f"Error {e}")
    
  return df


def rename_columns(dataframe: DataFrame, mapping_dict: dict) -> DataFrame:
    # Aplicar renombramiento iterando sobre el diccionario de mapeo
    for old_name, new_name in mapping_dict.items():
        dataframe = dataframe.withColumnRenamed(old_name, new_name)
    
    return dataframe
  

def get_volume_stage_path(config, environment):
    """
    Función para retornar la ruta del volumen en Unity Catalog donde se encuentran los datos en etapa stage.

    Args:
        config (dict): Diccionario con las configuraciones del JSON.
        environment (str): Ambiente de Databricks a utilizar (ej: 'dev', 'preprod', 'prod').

    Returns:
        str: Ruta completa al volumen stage correspondiente al tópico indicado.
    """
    catalog = f"{environment}_{config['input']['uc_catalog']}"
    schema = config['input']['uc_schema']
    volume_name = config['input']['uc_volume']
    topic = config['topic']

    return f"/Volumes/{catalog}/{schema}/{volume_name}/{topic}"
  
  ## Silver functions

def convert_timestamp_local_tz(landing_data, column_list):
    for column_name in column_list:
      landing_data = landing_data.withColumn(column_name + '_cl',  from_utc_timestamp(col(column_name) ,'America/Santiago'))

    return landing_data

def clean_columns(landing_data):
    for colname in landing_data.columns:        
      new_colname = colname.lower().replace(' ', '_').strip()
      landing_data = landing_data.withColumnRenamed(colname, new_colname)

    return landing_data
 
def cast_columns(
    df: DataFrame,
    schema: ty.StructType,
    timestamp_format: str | None = None,
) -> DataFrame:
    for field in schema.fields:
        if field.name not in df.columns:
            continue

        column = fa.col(field.name)
        target_type = field.dataType
        current_type = df.schema[field.name].dataType

        if current_type == target_type:
            continue

        if isinstance(target_type, ty.TimestampType):
            column = fa.regexp_replace(column, '"', "")
            column = fa.to_timestamp(column, timestamp_format)
        else:
            column = column.cast(target_type)

        df = df.withColumn(field.name, column)

    return df

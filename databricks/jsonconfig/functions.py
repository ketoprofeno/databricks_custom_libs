from pyspark.sql import functions as fa
from pyspark.sql import types as ty
from os import path
import re
import json

def low(name):
    return name.replace('-', '_')

def read_json(file_path):
    # with open('/dbfs' + file_path, 'r') as file:
    with open(file_path, 'r') as file:
        return json.load(file)

def get_latest_file(path, *paths):
    fullpath = os.path.join(path, *paths)

class JSONConfig(dict):
    _MNT_PREFIX = '/mnt'
    _MANDATORY = ['division', 'process',
                  'data_source', 'topic', 'input', 'output']
    # _MANDATORY = ['data_source', 'topic']

    _DEFAULTS = {
        'input': {
            'path': 'STAGE',
            'bad_records_path': 'BAD',
            'checkpoint_path': 'CHECKPOINT/in',
            'utils_path': 'UTILS'
        },
        'output': {
            'path': 'OUT',
            'bad_records_path': 'BAD',
            'checkpoint_path': 'CHECKPOINT/out',
            'utils_path': 'UTILS'
        }
    }

    def _is_ok(self, config):
        for section in self._MANDATORY:
            if section not in config:
                print(
                    "The section '{0}' must be defined in the config input file".format(man))
                return False
        return True

    def _check_path(self, key, subkey):
        if subkey in self[key]:
            self[key][subkey] = path.join(
                self[key]['base_path'], self[key][subkey])
        else:
            self[key][subkey] = path.join(
                self[key]['base_path'], self._DEFAULTS[key][subkey])
        print("Using {0}.{1}: {2}".format(key, subkey, self[key][subkey]))

    # def _setup(self, division, process):
    def _setup(self):
        # Base paths
        input_topic = self['topic'] if 'topic' not in self['input'] else self['input']['topic']
        output_topic = self['topic'] if 'topic' not in self['output'] else self['output']['topic']
        self['input']['base_path'] = path.join(
            self._MNT_PREFIX, self['input']['stage'], self['division'], self['process'], self['data_source'], input_topic)
        self['output']['base_path'] = path.join(
            self._MNT_PREFIX, self['output']['stage'], self['division'], self['process'], self['data_source'], output_topic)

        # Paths
        self._check_path('input', 'path')
        self._check_path('input', 'bad_records_path')
        self._check_path('input', 'checkpoint_path')
        self._check_path('input', 'utils_path')
        self._check_path('output', 'path')
        self._check_path('output', 'bad_records_path')
        self._check_path('output', 'checkpoint_path')
        self._check_path('output', 'utils_path')

        # Partitions
        if 'schema' in self['output']:
            partitions = filter(
                lambda field: field['partition_order'] > 0, self['output']['schema'])
            sorted_partitions = sorted(
                partitions, key=lambda field: field['partition_order'])
            self['output']['partitions'] = [field['name']
                                            for field in sorted_partitions]
            
            if self['output']['partition_table'] == False:
                self['output']['partitions'] = []

    # def __init__(self, division, process, config):
    def __init__(self, config):
        if self._is_ok(config):
            self.update(config)
            # TODO Mejorar el mensaje: Ejemplo: Taking 'trusted' 'respuestas' from 'encuestas_salud_covid' to 'refined' 'encuestas'
            print("Processing '{0}' from '{1}'".format(
                self['topic'], self['data_source']))
            print("Taking '{0}' to '{1}'".format(
                self['input']['stage'], self['output']['stage']))
            # self._setup(division, process)
            self._setup()

    def _get_spark_type(self, str_type):
        if str_type == 'string':
            return ty.StringType()
        elif str_type == 'long':
            return ty.LongType()
        elif str_type == 'double':
            return ty.DoubleType()
        elif str_type == 'integer' or str_type == 'int':
            return ty.IntegerType()
        elif str_type == 'boolean':
            return ty.BooleanType()
        elif str_type == 'timestamp':
            return ty.TimestampType()
        elif str_type == 'date':
            return ty.DateType()
        elif str_type == 'variant':
            return ty.ArrayType(ty.MapType(ty.StringType(), ty.StringType()))
        elif str_type == 'float':
            return ty.FloatType()
        elif str_type == 'binary':
            return ty.BinaryType()
        elif str_type == 'struct':
            return ty.StructType()
        elif str_type.startswith('decimal'):
            return decimal_datatype(str_type)
        elif str_type == 'map':
            return ty.MapType(ty.StringType(), ty.StringType())
        else:
            raise NotImplementedError("Unknow type '{}'".format(str_type))

    def get_output_path_for_dlt(self, stage):
        """
        Construye y retorna la ruta de salida en el Data Lake que servirá de input para un flujo de DLT,
        basándose en los parámetros definidos en el archivo de configuración.

        La ruta se construye considerando la etapa, división, proceso, fuente de datos,
        etapa específica y el tópico de salida.

        Args:
            stage (str): Nombre de la sub-etapa específica dentro del flujo DLT (por ejemplo, 'stage').

        Returns:
            str: Ruta completa en el Data Lake donde se almacenarán los datos procesados.
        """
        output_topic = self['topic'] if 'topic' not in self['output'] else self['output']['topic']
        return path.join(
            self._MNT_PREFIX, self['output']['stage'], self['division'], self['process'], self['data_source'], stage, output_topic)

    def get_schema(self, stage):
        #     """ Expected input format:
        #       [
        #         {'name': 'tag_name', 'type': 'string', 'is_nullable': False},
        #         {'name': 'event_type', 'type': 'string', 'is_nullable': False},
        #         {'name': 'timestamp', 'type': 'string', 'is_nullable': False},
        #         {'name': 'value', 'type': 'string', 'is_nullable': False}
        #       ]
        #     """
        """ Expected input format:
          [
            {'name': 'servidor', 'type': 'string', 'is_nullable': False},
            {'name': 'tag', 'type': 'string', 'is_nullable': False},
            {'name': 'timestamp', 'type': 'string', 'is_nullable': False},
            {'name': 'value', 'type': 'string', 'is_nullable': False}
          ]
        """
        spark_field_list = [ty.StructField(field['name'], self._get_spark_type(field['type']), field['is_nullable'])
                            for field in self[stage]['schema']]
        return ty.StructType(spark_field_list)
    
    def get_nested_schema(self, stage):
        spark_field_list = []
        for field in self[stage]['schema']:
            if field['type'] == 'array':
                array_field_list = [ty.StructField(ar_field['name'], self._get_spark_type(ar_field['type']), ar_field['is_nullable'])
                                    for ar_field in self[stage]['nested_schema'] if ar_field['ns'] == field['name']]
                spark_field_list.append(ty.StructField(field['name'], ty.ArrayType(ty.StructType(array_field_list)), field['is_nullable']))
            elif field['type'] == 'struct':
                struct_field_list = [ty.StructField(st_field['name'], self._get_spark_type(st_field['type']), st_field['is_nullable'])
                                    for st_field in self[stage]['nested_schema'] if st_field['ns'] == field['name']]
                spark_field_list.append(ty.StructField(field['name'], ty.StructType(struct_field_list), field['is_nullable']))
            else:
                spark_field_list.append(ty.StructField(field['name'], self._get_spark_type(field['type']), field['is_nullable']))
        return ty.StructType(spark_field_list)

    def create_output_table(self, recreate=False):
        if 'schema' in self['output']:
            sql_create_database_statement = "CREATE DATABASE IF NOT EXISTS {}"
            sql_create_table_statement = """
      CREATE TABLE IF NOT EXISTS {database}.{table} ({fields_list})
      USING {format}
      LOCATION '{path}'
      PARTITIONED BY ({partitions_list})
      """

            db_name = "{0}_{1}".format(
                low(self['output']['stage']), low(self['data_source']))
            self['output']['database'] = db_name
            self['output']['table'] = low(
                self['topic'] if 'topic' not in self['output'] else self['output']['topic'])
            self['output']['fields_list'] = ", ".join(["{0} {1}".format(
                field['name'], field['type']) for field in self['output']['schema']])
            self['output']['partitions_list'] = ", ".join(
                self['output']['partitions'])

            if recreate:
                sql_drop_database = "DROP TABLE IF EXISTS {0}.{1}"
                spark.sql(sql_drop_database.format(
                    self['output']['database'], self['output']['table']))
                print(
                    f"Table '{self['output']['database']}.{self['output']['table']}' deleted")

            spark.sql(sql_create_database_statement.format(db_name))
            spark.sql(sql_create_table_statement.format(**self['output']))
            print(
                f"Table '{self['output']['database']}.{self['output']['table']}' created")
        else:
            print("Provide a schema in the 'output' section to map a database")

def timezone_from_timestamp(column_name):
    return (fa.concat(
            fa.lit("GMT"),
            fa.regexp_extract(column_name, "(\+|\-)\d+:\d+$", 1),
            fa.hour(fa.regexp_extract(column_name, "(\d+:\d+)$", 1))))
    
def decimal_datatype(str_type):
    match = re.match(r"decimal\((\d+),\s*(\d+)\)", str_type)
    if match:
        precision = int(match.group(1))
        scale = int(match.group(2))
        return ty.DecimalType(precision, scale)
    else:
        raise ValueError("Invalid format for decimal type: '{}'".format(str_type))
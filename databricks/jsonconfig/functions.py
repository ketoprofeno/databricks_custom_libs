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

       
_SPARK_TYPES: dict[str, ty.DataType] = {
    "string": ty.StringType(),
    "long": ty.LongType(),
    "double": ty.DoubleType(),
    "integer": ty.IntegerType(),
    "int": ty.IntegerType(),
    "boolean": ty.BooleanType(),
    "timestamp": ty.TimestampType(),
    "date": ty.DateType(),
    "float": ty.FloatType(),
    "binary": ty.BinaryType(),
    "map": ty.MapType(ty.StringType(), ty.StringType()),
    "variant": ty.ArrayType(
        ty.MapType(ty.StringType(), ty.StringType())
    ),
    "struct": ty.StructType(),
}


def get_spark_type(data_type: str) -> ty.DataType:
    normalized = data_type.strip().lower()

    if normalized.startswith("decimal"):
        return decimal_datatype(normalized)

    try:
        return _SPARK_TYPES[normalized]
    except KeyError as exc:
        raise ValueError(
            f"Tipo de dato no soportado: {data_type}"
        ) from exc


def get_struct_schema(schema_definition: list[dict]) -> ty.StructType:
    return ty.StructType([
        ty.StructField(
            field["name"],
            get_spark_type(field["type"]),
            field.get("is_nullable", True),
        )
        for field in schema_definition
    ])
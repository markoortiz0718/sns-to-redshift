import base64
import json
import os
from typing import Any, Union

SNS_TOPIC_NAME = os.environ["SNS_TOPIC_NAME"]
SCHEMA_TYPE = dict[str, Union[list[str], dict[str, str]]]


def get_schema(schema_file: str) -> SCHEMA_TYPE:
    with open(schema_file, "r") as f:
        schema = json.load(f)
    if "required_columns" in schema.keys():
        assert all(
            isinstance(column_name, str) for column_name in schema["required_columns"]
        )
    if "rename_columns" in schema.keys():
        for old_column_name, new_column_name in schema["rename_columns"].items():
            assert isinstance(old_column_name, str) and isinstance(new_column_name, str)
    if "cast_values" in schema.keys():
        for column_name, cast_type in schema["cast_values"].items():
            assert isinstance(column_name, str) and isinstance(cast_type, str)
    return schema


def validate_columns(sns_message: dict[str, Any], schema: SCHEMA_TYPE) -> None:
    "Apparently all Redshift column names are lowercase"
    column_names = set(column_name.lower() for column_name in sns_message.keys())
    for required_column in schema["required_columns"]:
        assert required_column.lower() in column_names  # not case sensitive


def rename_columns(sns_message: dict[str, Any], schema: SCHEMA_TYPE) -> dict[str, Any]:
    "Apparently all Redshift column names are lowercase"
    for old_column_name, new_column_name in schema["rename_columns"].items():
        found_match = False
        for key in sns_message.keys():
            if key.lower() == old_column_name.lower():  # not case sensitive
                sns_message[new_column_name] = sns_message.pop(key)
                found_match = True
                break
        if not found_match:
            raise RuntimeError(
                f'Did not find column name "{old_column_name}" in '
                f"sns_message: {sns_message}"
            )
    return sns_message


def cast_values(sns_message: dict[str, Any], schema: SCHEMA_TYPE) -> dict[str, Any]:
    "Apparently all Redshift column names are lowercase"
    for column_name, cast_type in schema["cast_values"].items():
        found_match = False
        for key, value in sns_message.items():
            if key.lower() == column_name.lower():  # not case sensitive
                sns_message[key] = eval(cast_type)(
                    value
                )  ### probably need to deal with datetime
                found_match = True
                break
        if not found_match:
            raise RuntimeError(
                f'Did not find column name "{column_name}" in '
                f"sns_message: {sns_message}"
            )
    return sns_message


def lambda_handler(event, context) -> dict[str, list[dict[str, str]]]:
    """WARNING: If json record's column names don't match the column names
    in Redshift table, then the Redshift load/COPY command will still be successful,
    but the column in Redshift table will be filled with NULLs. To avoid that,
    define the table columns as NOT NULL, so that a bad load/COPY will fail
    instead of success. The other way is to do data quality monitoring such as
    with dbt to check if there are NULLs in the columns.
    """
    # print(event)
    # print("# of records:", len(event["records"]))
    records = []
    for record in event["records"]:
        sns_message = json.loads(base64.b64decode(record["data"]).decode("utf-8"))
        # print("sns_message", sns_message)

        schema = get_schema(schema_file=f"{SNS_TOPIC_NAME}.json")  # hard coded
        if "required_columns" in schema.keys():
            validate_columns(sns_message=sns_message, schema=schema)
        if "rename_columns" in schema.keys():
            sns_message = rename_columns(sns_message=sns_message, schema=schema)
        if "cast_values" in schema.keys():
            sns_message = cast_values(sns_message=sns_message, schema=schema)

        json_string = json.dumps(sns_message) + "\n"  # need newline
        encoded_message = base64.b64encode(json_string.encode("utf-8")).decode("utf-8")
        record = {
            "recordId": record["recordId"],
            "result": "Ok",
            "data": encoded_message,
        }
        records.append(record)
    return {"records": records}

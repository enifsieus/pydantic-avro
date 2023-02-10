import json
from typing import Optional, Union


def avsc_to_graphql(schema: dict) -> str:
    """Generate python code of pydantic of given Avro Schema"""
    if "type" not in schema or schema["type"] != "record":
        raise AttributeError("Type not supported")
    if "name" not in schema:
        raise AttributeError("Name is required")
    if "fields" not in schema:
        raise AttributeError("fields are required")

    classes = {}

    def get_graphql_type(t: Union[str, dict]) -> str:
        """Returns graphql type for given avro type."""
        optional = False
        if isinstance(t, str):
            if t == "string":
                py_type = "String"
            elif t == "long" or t == "int":
                py_type = "Int"
            elif t == "boolean":
                py_type = "Boolean"
            elif t == "double" or t == "float":
                py_type = "Float"
            elif t in classes:
                py_type = t
            else:
                raise NotImplementedError(f"Type {t} not supported yet")
        elif isinstance(t, list):
            c = t.copy()

            if "null" in t:
                optional = True
                c.remove("null")

            if len(c) == 1:
                py_type = get_graphql_type(c[0])
            else:
                py_type = union_type_to_graphql(c)
        elif t.get("logicalType") == "uuid":
            py_type = "UUID"
        elif t.get("logicalType") == "decimal":
            py_type = "Decimal"
        elif t.get("logicalType") == "timestamp-millis" or t.get("logicalType") == "timestamp-micros":
            py_type = "DateTime"
        elif t.get("logicalType") == "time-millis" or t.get("logicalType") == "time-micros":
            py_type = "Time"
        elif t.get("logicalType") == "date":
            py_type = "Date"
        elif t.get("type") == "enum":
            enum_type_to_graphql(t)
            py_type = t.get("name")
        elif t.get("type") == "string":
            py_type = "String"
        elif t.get("type") == "array":
            sub_type = get_graphql_type(t.get("items"))
            py_type = f"[{sub_type}]"
        elif t.get("type") == "record":
            record_type_to_graphql(t)
            py_type = t.get("name")
        elif t.get("type") == "map":
            py_type = "JSONObject"
        else:
            raise NotImplementedError(
                f"Type {t} not supported yet, "
                f"please report this at https://github.com/godatadriven/pydantic-avro/issues"
            )
        if optional:
            return py_type
        else:
            return f"{py_type}!"

    def union_type_to_graphql(t: list) -> str:
        """Convert a single avro Union type to a graphql Union"""
        names = list(map(get_graphql_type, t))
        union_name = f'Union{"".join(names).replace("!", "")}'
        value = " | ".join(names).replace("!", "")
        current = f"union {union_name} = {value}"
        classes[union_name] = current
        return union_name

    def enum_type_to_graphql(schema: dict) -> str:
        """Convert a single avro Enum type to a graphql Enum"""
        name = schema["name"]
        current = f"enum {name} {{\n"

        if len(schema["symbols"]) > 0:
            for symbol in schema["symbols"]:
                current += f"    {symbol}\n"

        current += "}\n"
        classes[name] = current

    def record_type_to_graphql(schema: dict):
        """Convert a single avro record type to a graphql Type"""
        name = schema["name"]
        current = f"type {name} "

        if len(schema["fields"]) > 0:
            # todo(mje): Naive federation logic - if there is a  field named id then use that as the Entity Key
            field = next((x for x in schema["fields"] if x["name"] == "id"), None)
            if field is not None:
                key_name = field["name"]
                current += f'@key(fields: "{key_name}") '

            current += "{\n"

            for field in schema["fields"]:
                n = field["name"]
                t = get_graphql_type(field["type"])
                current += f"    {n}: {t}\n"

            current += "}\n"
        classes[name] = current

    record_type_to_graphql(schema)

    file_content = """
extend schema @link(url: "https://specs.apollo.dev/federation/v2.0", import: ["@key", "@shareable"])

scalar Date
scalar Decimal
scalar JSONObject
scalar Time
scalar UUID


"""
    file_content += "\n\n".join(classes.values())

    return file_content


def convert_graphql(avsc_path: str, output_path: Optional[str] = None):
    with open(avsc_path, "r") as fh:
        avsc_dict = json.load(fh)
    file_content = avsc_to_graphql(avsc_dict)
    if output_path is None:
        print(file_content)
    else:
        with open(output_path, "w") as fh:
            fh.write(file_content)

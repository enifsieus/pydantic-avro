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
        ##todo(mje): Do we want to add support for GraphQL ID scalar? JSONObject?
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
            if "null" in t and len(t) == 2:
                optional = True
                c = t.copy()
                c.remove("null")
                py_type = get_graphql_type(c[0])
            else:
                ##todo(mje): Unions can't be inline, so we need to generate a union name
                ##eg. union SearchResult = Conference | Festival | Concert | Venue
                ##then py_type = SearchResult
                py_type = f"Union[{','.join([ 'None' if e == 'null' else get_graphql_type(e) for e in t])}]"
        ##todo(mje): Review all of these https://the-guild.dev/graphql/scalars
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
            ##todo(mje): Similar to Union, Enum can't be inline, need to generate a new enum name,
            ##eg. enum Episode {
            # NEWHOPE
            # EMPIRE
            # JEDI}
            ##then py_type = Episode
            enum_name = t.get("name")
            if enum_name not in classes:
                enum_class = f"class {enum_name}(str, Enum):\n"
                for s in t.get("symbols"):
                    enum_class += f'    {s} = "{s}"\n'
                classes[enum_name] = enum_class
            py_type = enum_name
        elif t.get("type") == "string":
            py_type = "String"
        elif t.get("type") == "array":
            sub_type = get_graphql_type(t.get("items"))
            py_type = f"[{sub_type}]"
        elif t.get("type") == "record":
            record_type_to_graphql(t)
            py_type = t.get("name")
        elif t.get("type") == "map":
            # There are no Dicts in graphql so JSONObject is our analog. We lose type enforcement though.
            # value_type = get_graphql_type(t.get("values"))
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

    def record_type_to_graphql(schema: dict):
        """Convert a single avro record type to a pydantic class"""
        name = schema["name"]
        current = f"type {name} "

        if len(schema["fields"]) > 0:
            # todo(mje): We need some way of identifying Keys for types for federation
            # Naive - if one of the fields is named id then use that as the Key
            field = next((x for x in schema["fields"] if x["name"] == "id"), None)
            if field is not None:
                key_name = field["name"]
                current += f"@key(fields: \"{key_name}\") "

            current += "{\n"

            for field in schema["fields"]:
                n = field["name"]
                t = get_graphql_type(field["type"])
                current += f"    {n}: {t}\n"

                # Defaults aren't supported by graphQL types - instead they must be specified in the query
                # https://github.com/graphql/graphql-js/issues/345
                # default = field.get("default")
                # if "default" not in field:
                # current += f"    {n}: {t}\n"
                # elif isinstance(default, (bool, type(None))):
                #     current += f"    {n}: {t} = {default}\n"
                # else:
                #     current += f"    {n}: {t} = {json.dumps(default)}\n"
            # if len(schema["fields"]) == 0:
            #     current += "    pass\n"

            current += "}\n"

        classes[name] = current

    record_type_to_graphql(schema)

    file_content = """
extend schema @link(url: "https://specs.apollo.dev/federation/v2.0", import: ["@key", "@shareable"])

scalar Date
scalar Decimal
scalar DateTime
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

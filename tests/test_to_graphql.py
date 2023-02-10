from pydantic_avro.avro_to_graphql import avsc_to_graphql
from ariadne import gql


def test_avsc_to_graphql_empty():
    graphql = avsc_to_graphql({"name": "Test", "type": "record", "fields": []})
    print(graphql)
    expected = """
extend schema @link(url: "https://specs.apollo.dev/federation/v2.0", import: ["@key", "@shareable"])

scalar Date
scalar Decimal
scalar JSONObject
scalar Time
scalar UUID


type Test"""
    assert expected in graphql
    assert gql(graphql)


def test_avsc_to_graphql_primitive():
    graphql = avsc_to_graphql(
        {
            "name": "Test",
            "type": "record",
            "fields": [
                {"name": "col1", "type": "string"},
                {"name": "col2", "type": "int"},
                {"name": "col3", "type": "long"},
                {"name": "col4", "type": "double"},
                {"name": "col5", "type": "float"},
                {"name": "col6", "type": "boolean"},
            ],
        }
    )
    expected = """
type Test {
    col1: String!
    col2: Int!
    col3: Int!
    col4: Float!
    col5: Float!
    col6: Boolean!
}"""

    assert expected in graphql
    assert gql(graphql)


def test_avsc_to_graphql_map():
    graphql = avsc_to_graphql(
        {
            "name": "Test",
            "type": "record",
            "fields": [
                {"name": "col1", "type": {"type": "map", "values": "string", "default": {}}},
            ],
        }
    )
    expected = """
type Test {
    col1: JSONObject!
}"""
    assert expected in graphql
    assert gql(graphql)

##todo(mje):
## GraphQL does not support Map types so we have two choices - return them as non-queryable JSON structures,
## Or we have custom types of the form type Custom {key: String, value: graphql type} which need custom resolvers
def test_avsc_to_graphql_map_nested_object():
    graphql = avsc_to_graphql(
        {
            "name": "Test",
            "type": "record",
            "fields": [
                {
                    "name": "col1",
                    "type": {
                        "type": "map",
                        "values": {"type": "record", "name": "Nested", "fields": [{"name": "col1", "type": "string"}]},
                        "default": {},
                    },
                },
            ],
        }
    )

    expected = """
type Test {
    col1: JSONObject!
}"""
    assert expected in graphql
    assert gql(graphql)
#     assert "class Test(BaseModel):\n" "    col1: Dict[str, Nested]" in graphql
#     assert "class Nested(BaseModel):\n" "    col1: str" in graphql


def test_avsc_to_graphql_map_nested_array():
    graphql = avsc_to_graphql(
        {
            "name": "Test",
            "type": "record",
            "fields": [
                {
                    "name": "col1",
                    "type": {
                        "type": "map",
                        "values": {
                            "type": "array",
                            "items": "string",
                        },
                        "default": {},
                    },
                },
            ],
        }
    )

    expected = """
type Test {
    col1: JSONObject!
}"""
    assert expected in graphql
    assert gql(graphql)
#     assert "class Test(BaseModel):\n" "    col1: Dict[str, List[str]]" in graphql


def test_avsc_to_graphql_logical():
    graphql = avsc_to_graphql(
        {
            "name": "Test",
            "type": "record",
            "fields": [
                {
                    "name": "col1",
                    "type": {"type": "int", "logicalType": "date"},
                },
                {
                    "name": "col2",
                    "type": {"type": "long", "logicalType": "time-micros"},
                },
                {
                    "name": "col3",
                    "type": {"type": "long", "logicalType": "time-millis"},
                },
                {
                    "name": "col4",
                    "type": {"type": "long", "logicalType": "timestamp-micros"},
                },
                {
                    "name": "col5",
                    "type": {"type": "long", "logicalType": "timestamp-millis"},
                },
            ],
        }
    )
    expected = """
type Test {
    col1: Date!
    col2: Time!
    col3: Time!
    col4: DateTime!
    col5: DateTime!
}"""
    assert expected in graphql
    assert gql(graphql)


def test_avsc_to_graphql_complex():
    graphql = avsc_to_graphql(
        {
            "name": "Test",
            "type": "record",
            "fields": [
                {
                    "name": "col1",
                    "type": {
                        "name": "Nested",
                        "type": "record",
                        "fields": [],
                    },
                },
                {
                    "name": "col2",
                    "type": {
                        "type": "array",
                        "items": "int",
                    },
                },
                {
                    "name": "col3",
                    "type": {
                        "type": "array",
                        "items": "Nested",
                    },
                },
            ],
        }
    )

    expected = """
type Nested"""
    assert expected in graphql

    expected = """
type Test {
    col1: Nested!
    col2: [Int!]!
    col3: [Nested!]!
}"""
    assert expected in graphql
    assert gql(graphql)

##todo(mje): There are no defaults in graphql schemas for reads
# def test_default():
#     graphql = avsc_to_graphql(
#         {
#             "name": "Test",
#             "type": "record",
#             "fields": [
#                 {"name": "col1", "type": "string", "default": "test"},
#                 {"name": "col2_1", "type": ["null", "string"], "default": None},
#                 {"name": "col2_2", "type": ["string", "null"], "default": "default_str"},
#                 {"name": "col3", "type": {"type": "map", "values": "string"}, "default": {"key": "value"}},
#                 {"name": "col4", "type": "boolean", "default": True},
#                 {"name": "col5", "type": "boolean", "default": False},
#             ],
#         }
#     )
#     assert (
#         "class Test(BaseModel):\n"
#         '    col1: str = "test"\n'
#         "    col2_1: Optional[str] = None\n"
#         '    col2_2: Optional[str] = "default_str"\n'
#         '    col3: Dict[str, str] = {"key": "value"}\n'
#         "    col4: bool = True\n"
#         "    col5: bool = False\n" in graphql
#     )

def test_enums():
    graphql = avsc_to_graphql(
        {
            "name": "Test",
            "type": "record",
            "fields": [
                {"name": "c1", "type": {"type": "enum", "symbols": ["passed", "failed"], "name": "Status"}},
            ],
        }
    )
    print(graphql)
    expected = """
type Test {
    c1: Status!
}"""
    assert expected in graphql

    expected = """
enum Status {
    passed
    failed
}"""
    assert expected in graphql
    assert gql(graphql)


def test_enums_reuse():
    graphql = avsc_to_graphql(
        {
            "name": "Test",
            "type": "record",
            "fields": [
                {"name": "c1", "type": {"type": "enum", "symbols": ["passed", "failed"], "name": "Status"}},
                {"name": "c2", "type": "Status"},
            ],
        }
    )
    print(graphql)
    expected = """
type Test {
    c1: Status!
    c2: Status!
}"""
    assert expected in graphql

    expected = """
enum Status {
    passed
    failed
}"""
    assert expected in graphql
    assert gql(graphql)


def test_unions():
    graphql = avsc_to_graphql(
        {
            "type" : "record",
            "name" : "Test",
            "fields" : [
                {
                    "name" : "a_union",
                    "type" : [
                        "null", "long", "string",
                        {
                            "type" : "record",
                            "name" : "ARecord",
                            "fields" : [
                                {
                                    "name" : "values",
                                    "type" : {
                                        "type" : "map",
                                        "values" : "string"
                                    }
                                }
                            ]
                        }
                    ]
                }
            ]
        }
    )

    assert "union UnionIntStringARecord = Int | String | ARecord" in graphql
    assert "a_union: UnionIntStringARecord" in graphql
    assert gql(graphql)

from ariadne import gql

from pydantic_avro.avro_to_graphql import avsc_to_graphql


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

    assert "type Nested" in graphql

    expected = """
type Test {
    col1: Nested!
    col2: [Int!]!
    col3: [Nested!]!
}"""
    assert expected in graphql
    assert gql(graphql)


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
            "type": "record",
            "name": "Test",
            "fields": [
                {
                    "name": "a_union",
                    "type": [
                        "null",
                        "long",
                        "string",
                        {
                            "type": "record",
                            "name": "ARecord",
                            "fields": [{"name": "values", "type": {"type": "map", "values": "string"}}],
                        },
                    ],
                }
            ],
        }
    )

    assert "union UnionIntStringARecord = Int | String | ARecord" in graphql
    assert "a_union: UnionIntStringARecord" in graphql
    assert gql(graphql)

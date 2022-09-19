from debussy_concert.core.entities.table import BigQueryTable
from debussy_concert.core.service.lakehouse.google_cloud import (
    GoogleCloudLakeHouseService,
)


def get_example_table():
    table_dict = {
        "fields": [
            {
                "name": "user_id",
                "description": "id of the user",
                "data_type": "STRING",
                "constraint": "REQUIRED",
            },
            {
                "name": "user_email",
                "description": "email of the user",
                "data_type": "STRING",
                "constraint": "REQUIRED",
            },
            {
                "name": "user_addresses",
                "description": "home and work addresses of the user",
                "data_type": "RECORD",
                "constraint": "REPEATED",
                "fields": [
                    {"name": "home", "data_type": "STRING"},
                    {"name": "work", "data_type": "STRING"},
                ],
            },
        ]
    }
    return BigQueryTable.load_from_dict(table_dict)


def test_get_table_schema():
    table = get_example_table()
    expected_schema = [
        {
            "name": "user_id",
            "description": "id of the user",
            "type": "STRING",
            "mode": "REQUIRED",
            "policy_tags": {"names": []},  # defaults to empty list
            "fields": None,  # defaults do None
        },
        {
            "name": "user_email",
            "description": "email of the user",
            "type": "STRING",
            "mode": "REQUIRED",
            "policy_tags": {"names": []},
            "fields": None,
        },
        {
            "name": "user_addresses",
            "description": "home and work addresses of the user",
            "type": "RECORD",
            "mode": "REPEATED",
            "policy_tags": {"names": []},
            "fields": [
                {
                    "name": "home",
                    "description": None,  # defaults do None
                    "type": "STRING",
                    "mode": None,  # defaults do None
                    "fields": None,
                    "policy_tags": {"names": []},
                },
                {
                    "name": "work",
                    "description": None,
                    "type": "STRING",
                    "mode": None,
                    "fields": None,
                    "policy_tags": {"names": []},
                },
            ],
        },
    ]
    schema = GoogleCloudLakeHouseService.get_table_schema(table)
    # print(schema)
    assert schema == expected_schema

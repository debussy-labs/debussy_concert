# -*- coding: utf-8 -*-
import json
from airflow.models import BaseOperator
from google.cloud import datastore


class DotzDatastoreClient:
    """Datastore client for common datastore operations"""

    def __init__(self, project_id, namespace):
        self.client = datastore.Client(project=project_id, namespace=namespace)

    def query(self, kind, filter=None):
        query = self.client.query(kind=kind)
        if filter:
            query.add_filter(*filter)
        result = list(query.fetch())
        return result

    def create_or_update_entity_from_dict(self, kind, entity_dict, key_value=None):
        # replaces an existing entity if key_value is given, else creates a new one
        if key_value:
            key = self.client.key(kind, key_value)
        else:
            key = self.client.key(kind)
        entity = datastore.Entity(key=key)
        for key, value in entity_dict.items():
            entity[key] = value
            self.client.put(entity)

    def delete(self, entity):
        self.client.delete(entity.key)

    def update(self, entity):
        self.client.put(entity)


class DatastoreGetEntityOperator(BaseOperator):
    ui_color = "#0aab3d"

    def __init__(self, project, namespace, kind, filters, **kwargs):

        super().__init__(**kwargs)
        self.project = project
        self.namespace = namespace
        self.kind = kind
        self.filters = filters

    @property
    def ds_client(self):
        return DotzDatastoreClient(self.project, self.namespace)

    def execute(self, context):
        self.log.info(f"Getting entity for {self.kind} where {self.filters}")
        entity = self.ds_client.query(self.kind, self.filters)[0]
        entity_dict = {}
        # a way of making an entity an its id available in a single structure
        entity_dict["entity"] = dict(entity)
        entity_dict["id"] = entity.id
        return json.dumps(entity_dict, default=str)


class DatastoreUpdateEntityOperator(BaseOperator):
    ui_color = "#0aab3d"
    template_fields = ("entity_json_str",)

    def __init__(self, project, namespace, kind, entity_json_str, **kwargs):

        super().__init__(**kwargs)
        self.project = project
        self.namespace = namespace
        self.kind = kind
        self.entity_json_str = entity_json_str

    @property
    def ds_client(self):
        return DotzDatastoreClient(self.project, self.namespace)

    @property
    def entity_dict(self):
        # never make this an atribute instead of a property, bad things will happen
        # (airflow makes entity_json_str properties enclosed by single quotes, which break
        # json deserializer)
        return json.loads(self.entity_json_str)

    def execute(self, context):
        self.ds_client.create_or_update_entity_from_dict(
            self.kind,
            entity_dict=self.entity_dict["entity"],
            key_value=self.entity_dict["id"],
        )
        return json.dumps(self.entity_dict, default=str)

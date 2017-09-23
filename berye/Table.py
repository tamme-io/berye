import json
import yaml
import os
import sys

from Database import Database

schema_extensions = (".json", ".yaml", ".yml")


class Table(object):

    def __init__(self, table_name, database=None):
        self.name = table_name
        self.columns = []
        self.schema_file = None
        self.file_format = None
        self.schema_dict = {}
        if database is None:
            self.database = Database()
        else:
            self.database = database
        return None


    def exists(self):
        rows = self.database.query(
            "SELECT table_name FROM information_schema.tables;"
        )
        tables = map(lambda x: x[0], rows)
        return self.name in tables


    def parseSchema(self):
        self.getSchemaFile()
        if self.schema_file is None:
            return None



        return None


    def getSchemaFile(self):
        if self.schema_file is not None:
            return self.schema_file
        current_directory = os.getcwd()
        schema_files = os.listdir("%s/schema/" % current_directory)
        possible_schema_file_names = map(lambda x: self.name + x, schema_extensions)
        matching_files = filter(lambda x: x in possible_schema_file_names, files)
        if len(matching_files) == 0:
            return None
        self.schema_file = "%s/schema/%s" % (current_directory, matching_files[0])
        if ".json" in self.schema_file:
            self.file_format = "json"
        else:
            self.file_format = "yaml"
        return self.schema_file


    def create(self):
        return None


    def migrate(self):
        return None


    def initFromDatabase(self):
        return None


    def drop(self):
        return None

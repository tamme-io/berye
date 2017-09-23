import json
import os
import sys

from Database import Database

DEFAULT_DATABASE_TYPE = "mysql"

class Berye(object):

    def __init__(self, database_type=DEFAULT_DATABASE_TYPE):
        self.database_type = database_type
        self.database = Database(database_type)
        return None

    def create(self):
        self.database.create()
        return None

    def migrate(self):
        # check for the database, if it's not there create it
        database = Database()
        if not database.exists():
            database.create()

        # for each table that we find, check if it exists

        # if it doesn't exist, create it

        # if it does exist, check if the schema matches

        # if the schema doesn't match, update it

        return None

    def query(self, query):
        return None

    def drop(self):
        self.database.drop()
        return None

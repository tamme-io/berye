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

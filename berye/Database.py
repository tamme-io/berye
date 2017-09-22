import os
import sys
import yaml
import json


import mysql.connector
from Config import Config



class Database(object):

    def __init__(self, database_type):
        self.database_type = database_type
        self.connection = None
        return None

    def configure(self, host=None, port=None, database=None, username=None, password=None):
        self.config = Config(host=host, port=port, database=database, username=username, password=password)
        return self.config

    def connect(self):
        if self.connection is not None and not self.connection.closed():
            return self.connection
        if self.format == "mysql":
            self.connection = mysql.connector.connect(
                user=self.config.username,
                password=self.config.password,
                host=self.config.host,
                database=self.config.database,
                port=self.config.port
            )
        else:
            print "%s not currently supported" % self.database_type
        return self.connection

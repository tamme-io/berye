import os
import sys
import yaml
import json


import mysql.connector
from Config import Config



class Database(object):

    def __init__(self, database_type="mysql"):
        self.database_type = database_type
        self.config = None
        self.connection = None
        self.super_connection = None
        return None


    def configure(self, host=None, port=None, database=None, username=None, password=None):
        if self.config is not None:
            return self.config
        self.config = Config(host=host, port=port, database=database, username=username, password=password)
        return self.config


    def exists(self):
        self.configure()
        self.connectToSuper()
        cursor = self.super_connection.cursor()
        cursor.execute("SHOW DATABASES;")
        rows = []
        for row in cursor:
            rows.append(row)
        print rows
        databases = map(lambda x: x[0], rows)
        return self.config.database in databases


    def create(self):
        self.configure()
        self.connectToSuper()
        create_cursor = self.super_connection.cursor()
        create_cursor.execute("CREATE DATABASE %s" % self.config.database)
        self.super_connection.commit()
        rows = []
        for row in create_cursor:
            rows.append(row)
        return rows


    def connect(self):
        if self.connection is not None and not self.connection.closed():
            return self.connection
        self.configure()
        if self.database_type == "mysql":
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


    def connectToSuper(self):
        if self.super_connection is not None:
            return self.super_connection
        self.configure()
        if self.database_type == "mysql":
            self.super_connection = mysql.connector.connect(
                user=self.config.username,
                password=self.config.password,
                host=self.config.host,
                port=self.config.port
            )
        else:
            print "%s not currently supported" % self.database_type
        return self.super_connection


    def query(self, query):
        self.connect()
        cursor = self.connection.cursor()
        cursor.execute(query)
        self.connection.ocmmit()
        rows = []
        for row in cursor:
            rows.append(row)
        return rows


    def drop(self):
        self.configure()
        self.connectToSuper()
        drop_cursor = self.super_connection.cursor()
        drop_cursor.execute("DROP DATABASE %s" % self.config.database)
        self.super_connection.commit()
        rows = []
        for row in drop_cursor:
            rows.append(row)
        return rows

import os
import sys
import yaml
import json


import mysql.connector
from Config import Config


LOGICAL_IDS_NAME = "berye_logical_ids"

class Database(object):

    def __init__(self, database_type="mysql"):
        self.database_type = database_type
        self.config = None
        self.connection = None
        self.super_connection = None
        self.tables = []
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
        self.createLogicalIdTable()
        return rows


    def logicalTableExists(self):
        return (LOGICAL_IDS_NAME in self.tableNames())


    def tableNames(self):
        querystring = "SHOW TABLES;"
        rows = self.query(querystring)
        table_names = map(lambda x: x.get("Tables_in_" + self.config.database), rows)
        return table_names


    def createLogicalIdTable(self):
        querystring = "CREATE TABLE " + LOGICAL_IDS_NAME + " "
        querystring += """
            (
            table_name VARCHAR(255),
            column_name VARCHAR(255),
            logical_id VARCHAR(255),
            id INT NOT NULL AUTO_INCREMENT UNIQUE PRIMARY KEY,
            INDEX (id),
            INDEX (table_name),
            INDEX (column_name),
            INDEX (logical_id)
            );
        """
        print "Query"
        print querystring
        rows = self.query(querystring)
        return rows


    def migrate(self):
        # TODO : THIS IS AN UGLY HACK, FIX THIS
        from berye.Table import Table
        if not self.exists():
            self.create()
        # get all of the schema files
        current_directory = os.getcwd()
        schema_files = os.listdir("%s/schema/" % current_directory)
        schema_files = filter(lambda x: "berye_config" not in x, schema_files)
        table_names = map(lambda x: x.split(".")[0], schema_files)
        for table_name in table_names:
            if table_name not in self.tables:
                self.tables.append(Table(table_name, database=self))

        existing_tables = filter(lambda x: x.exists(), self.tables)
        new_tables = filter(lambda x: x.exists() == False, self.tables)

        # migrate all of the tables
        for table in self.tables:
            table.migrate()

        # migrate all of the foreign keys
        for table in new_tables:
            table.createForeignKeys()
        for table in existing_tables:
            table.addNewForeignKeys()

        # find any tables that need to be dropped
        table_names.append(LOGICAL_IDS_NAME)
        database_table_names = self.tableNames()
        superfluous_tables = filter(lambda x: x not in table_names, database_table_names)
        for table_name in superfluous_tables:
            dropquery = "DROP TABLE %s;" % table_name
            print dropquery
            self.query(dropquery)

        return None


    def connect(self):
        if self.connection is not None and self.connection.is_connected():
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
        print "Query:: %s" % query
        self.connect()
        cursor = self.connection.cursor(buffered=True, dictionary=True)
        cursor.execute(query)

        self.connection.commit()
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

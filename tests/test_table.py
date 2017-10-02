import pytest
import os
import sys
import yaml
import json

from time import sleep

# TEST FILES
source_path = os.path.realpath(os.path.dirname(__file__)+"/../berye")
print "source path: %s" % str(source_path)
sys.path.append(source_path)

import Database
import Column
import Table


class TestTable(object):


    def test_CreateDB(self):
        # putting the sleep in here to give the database time to handle any
        # previous database drop
        sleep(5)

        database = Database.Database()
        database.configure()
        rows = database.create()
        assert database.exists()

    def test_createTable(self):
        table = Table.Table("test_object")
        sql = table.create()
        assert table.exists()

    def test_createForeignKeys(self):
        object_table = Table.Table("test_object")
        object_table.parseSchema()
        subject_table = Table.Table("people")
        subject_table.create()
        object_table.createForeignKeys()

        # Check that the keys are in the database
        keys = object_table.getDatabaseForeignKeys()
        print keys
        assert object_table.foreignKeysExist()
        for key in keys:
            # this will send extra queries to the database than required
            # that is preferable because it ensures that our code is
            # handling the right number of requests.
            assert key in object_table.getDatabaseForeignKeys()


    def test_DropDB(self):
        database = Database.Database()
        database.configure()
        rows = database.drop()
        assert database.exists() == False

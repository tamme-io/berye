import pytest
import os
import sys
import yaml
import json

# TEST FILES
source_path = os.path.realpath(os.path.dirname(__file__)+"/../berye")
print "source path: %s" % str(source_path)
sys.path.append(source_path)

import Database


class TestDatabase(object):


    def test_Create(self):
        database = Database.Database()
        database.configure()
        rows = database.create()
        assert database.exists()

    def test_Connect(self):
        database = Database.Database()
        database.configure()
        database.connect()
        assert 1

    def test_Drop(self):
        database = Database.Database()
        database.configure()
        rows = database.drop()
        assert database.exists() == False

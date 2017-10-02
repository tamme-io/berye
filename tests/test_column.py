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
import Column


class TestColumn(object):


    def test_SqlBase(self):
        column = Column.Column({
            "logical_id" : "A Thing",
            "column_name" : "a_thing",
            "data_type" : "INT",
            "index" : True,
            "primary_key" : False,
            "auto_increment" : False,
            "comment" : "This is a test column",
            "unique" : True,
            "not_null" : True,
            "check" : " > 16",
            "default" : 10
        })
        sql = column.sql()
        assert sql == "a_thing INT NOT NULL DEFAULT 10 COMMENT 'This is a test column' UNIQUE CHECK (a_thing  > 16)"

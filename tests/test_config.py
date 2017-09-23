import pytest
import os
import sys
import yaml
import json

# TEST FILES
source_path = os.path.realpath(os.path.dirname(__file__)+"/../berye")
print "source path: %s" % str(source_path)
sys.path.append(source_path)

import Config


manual_config_data = {
    "host" : "manualhost",
    "port" : 0000,
    "database" : "manualdatabase",
    "username" : "manualusername",
    "password" : "manualpassword"
}

class TestConfig(object):

    def test_manualConfig(self):
        config = Config.Config(**manual_config_data)
        for key in manual_config_data.keys():
            assert getattr(config, key) == manual_config_data[key]

    def test_schemaConfig(self):
        config = Config.Config()
        assert config.host == "localhost"
        assert config.port == 3306
        assert config.database == "testberyedatabase"
        assert config.username == "root"
        assert config.password == ""

    def test_envConfig(self):
        '''
        We plan on being able to use env variables only to setup the config,
        but that can be a later issue. The first thing that we need to figure
        out is how we're going to do the connections etc.
        '''
        assert 1

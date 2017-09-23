import os
import json
import yaml
import sys
import re

class Config(object):

    def __init__(self, host=None, port=None, database=None, username=None, password=None):
        self.file_name = self.configFileName()
        self.config_dict = None

        if self.file_name is not None:
            # is it json or yaml?
            self.format = "yaml"
            if ".json" in self.file_name:
                self.format = "json"

            self.configDict()
        else:
            self.config_dict = {}

        # look for the mains
        if host is None:
            self.host = self.config_dict.get("host", "")
        else:
            self.host = host

        if port is None:
            self.port = self.config_dict.get("port", 3306)
        else:
            self.port = port

        if database is None:
            self.database = self.config_dict.get("database", "")
        else:
            self.database = database

        if username is None:
            self.username = self.config_dict.get("username", "")
        else:
            self.username = username

        if password is None:
            self.password = self.config_dict.get("password", "")
        else:
            self.password = password

        return None

    def configFileName(self):
        # we need to get the config in here from the appropriate file
        current_directory = os.getcwd()
        try:
            schema_files = os.listdir('%s/schema' % current_directory)
        except OSError:
            return None
        config_files = filter(lambda x: x.split(".")[0] == "berye_config", schema_files)
        if len(config_files) == 0:
            print "No Config Available"
            # TODO : Perhaps we're better off throwing an error here becuase
            # without the config we're going to have no way of proceeding.
            return None
        config_file_name = config_files[0]
        return config_file_name


    def configDict(self):

        if self.config_dict is not None:
            return self.config_dict

        if self.file_name is None:
            self.config_dict = {}
            return self.config_dict

        current_directory = os.getcwd()

        # get the file and convert it to a native dict
        with open("%s/schema/%s" % (current_directory, self.file_name)) as f:
            if self.format == "yaml":
                self.config_dict = yaml.load(f)
            else:
                self.config_dict = json.load(f)

        # replace any variables
        self.parseConfigVariable(self.config_dict)


        return self.config_dict


    '''
    Config Variables will be able to come in a number of forms
    but for the moment we're just going to handle the ability to put in
    ENV variables so that we can store database passwords etc in them
    '''
    def parseConfigVariable(self, o):
        if isinstance(o, dict):
            for key in o.keys():
                o[key] = self.parseConfigVariable(o[key])
        elif isinstance(o, list) or isinstance(o, tuple):
            for item in o:
                item = self.parseConfigVariable(item)
        elif isinstance(o, str):
            o = self.parseConfigString(o)
        elif isinstance(o, unicode):
            try:
                item = str(o)
                o = self.parseConfigString(item)
            except Exception as exception:
                print "There was an error trying to parse %u: %s" % (o, str(exception))
                return None
        return o


    def parseConfigString(self, o):
        env_start = '\${'
        env_end = '}'
        env_var_refs = re.findall(env_start + '(.*?)' + env_end, o, re.DOTALL)
        for ref in env_var_refs:
            o = o.replace("${" + ref + env_end, self.parseEnvVariable(ref))
        return o


    def parseEnvVariable(self, variable_name):
        variable_name = variable_name.upper()
        return os.environ.get(variable_name, "")

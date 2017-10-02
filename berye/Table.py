import json
import yaml
import os
import sys

from Database import Database
from Column import Column

schema_extensions = (".json", ".yaml", ".yml")
LOGICAL_IDS_NAME = "berye_logical_ids"


'''
There are also some table options in here that I need to think about
Auto Increment
Average Row length
Default Character Set
Checksum
Collate
Comment
Compression
Connection
Data Directory
Delay Key write
Encryption
Engine
Insert Method
Key Block Size
Max Rows
Min Rows
Pack Keys
Password
Row Format
Stats Auto Recalc
Stats Persistent
Stats Sample Pages
Tablespace
Union

When creating we need to make sure that we're doing it in order:

make sure that all of the columns are there first and foremost
then create the indexes
then we will add the foreign keys afterwards, this is probably the best way of
doing it.

'''


class Table(object):


    def __init__(self, table_name, database=None):
        self.name = table_name
        self.columns = []
        self.database_columns = []
        self.references = []
        self.schema_file = None
        self.file_format = None
        self.schema_dict = {}
        if database is None:
            self.database = Database()
        else:
            self.database = database
        return None


    def exists(self):
        rows = self.database.query(
            "SELECT table_name FROM information_schema.tables;"
        )
        print rows
        tables = map(lambda x: x["table_name"], rows)
        return self.name in tables


    def parseSchema(self):
        self.getSchemaFile()
        if self.schema_file is None:
            return None

        with open(self.schema_file) as f:
            if self.file_format == "json":
                self.schema_dict = json.load(f)
            else:
                self.schema_dict = yaml.load(f)

        self.object_name = self.schema_dict.get("object_name", None)
        self.raw_column_data = self.schema_dict.get("columns", [])
        self.references = self.schema_dict.get("references", [])
        self.columns = []
        for column_data in self.raw_column_data:
            self.columns.append(Column(column_data, database=self.database, table=self))
        # handling for primary key
        primary_key_columns = filter(lambda x: x.primary_key == True, self.columns)
        if len(primary_key_columns) == 1:
            return None
        elif len(primary_key_columns) > 1:
            # There are too many primary keys, throw an exception
            raise Exception("Too Many Primary Keys", "%s has %s primary keys defined" % (self.name, str(len(primary_key_columns))))
        else:
            # There are no primary key columns

            # if this is the case then first we need to check if there is a
            # column called id or {table_name}_id and we should just assume
            # that this was the intended id, otherwise we create one
            likely_id_fields = filter(
                lambda x: (x.column_name == "id" or x.column_name == self.name + "_id"),
                self.columns
            )
            if len(likely_id_fields) > 0:
                likely_id_fields.sort(key=len)
                likely_id_fields[0].primary_key = True
                likely_id_fields[0].not_null = True
                likely_id_fields[0].auto_increment = True
            else:
                self.columns.append(Column(
                    {
                        "column_name" : "id",
                        "logical_id" : "id",
                        "auto_increment" : True,
                        "data_type" : "INT",
                        "primary_key" : True,
                        "not_null" : True
                    },
                    database=self.database,
                    table=self
                ))
        return None


    def getSchemaFile(self):
        if self.schema_file is not None:
            return self.schema_file
        current_directory = os.getcwd()
        schema_files = os.listdir("%s/schema/" % current_directory)
        possible_schema_file_names = map(lambda x: self.name + x, schema_extensions)
        matching_files = filter(lambda x: x in possible_schema_file_names, schema_files)
        if len(matching_files) == 0:
            return None
        self.schema_file = "%s/schema/%s" % (current_directory, matching_files[0])
        if ".json" in self.schema_file:
            self.file_format = "json"
        else:
            self.file_format = "yaml"
        return self.schema_file


    def foreignKeysExist(self):
        return (len(self.missingForeignKeys()) == 0 and len(self.superfluousForeignKeys()) == 0)


    def missingForeignKeys(self):
        keys = self.getDatabaseForeignKeys()
        schema_keys = filter(lambda x: x.foreign_key == True, self.columns)
        schema_keys = map(lambda x: x.foreign_key_name, schema_keys)
        return filter(lambda x: x not in keys, schema_keys)


    def superfluousForeignKeys(self):
        keys = self.getDatabaseForeignKeys()
        schema_keys = filter(lambda x: x.foreign_key == True, self.columns)
        schema_keys = map(lambda x: x.foreign_key_name, schema_keys)
        return filter(lambda x: x not in schema_keys, keys)


    def getDatabaseForeignKeys(self):
        self.database.query("USE INFORMATION_SCHEMA;")
        query = """
        SELECT CONSTRAINT_NAME FROM KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = '%s'
        AND TABLE_NAME = '%s'
        AND REFERENCED_COLUMN_NAME IS NOT NULL
        """ % (self.database.config.database, self.name)
        rows = self.database.query(query)

        # TODO : remove this once we know that the new cursor style is working
        print rows
        keys = map(lambda x: x['CONSTRAINT_NAME'], rows)
        return keys


    '''

    Migrate the core of the table

    '''

    def migrate(self):
        if not self.exists():
            self.create()
            return True

        self.update()

        return False

    def create(self):
        if len(self.columns) == 0:
            self.parseSchema()
        if self.database.logicalTableExists() == False:
            self.database.createLogicalIdTable()
        index_columns = filter(lambda x: x.index == True, self.columns)
        sql = "CREATE TABLE %s (%s" % (
            self.name,
            ", ".join(map(lambda x: x.sql(), self.columns))
        )
        if len(index_columns) > 0:
            sql += ", %s" % ", ".join(map(lambda x: "INDEX(%s)" % x.column_name, index_columns))
        sql += ");"
        print sql
        rows = self.database.query(sql)
        print rows
        self.createLogicalIds()
        return sql


    def createLogicalIds(self):
        return map(lambda x: x.updateLogicalId(), self.columns)


    def update(self):
        # work out what's there that's the same and what's there that isn't
        # TODO : we need to work out how the update is going to work, we
        # should have some kind of thing previously
        if len(self.columns) == 0:
            self.parseSchema()
        self.databaseDefinition()
        map(lambda x: x.findPreviousColumn(self.database_columns), self.columns)

        unchanged_columns = filter(lambda x: x.previous_column is not None, self.columns)

        new_columns = filter(lambda x: x.previous_column is None, self.columns)
        removeable_columns = filter(lambda x: x.future_column is None, self.database_columns)
        changed_columns = filter(lambda x: x.unchanged == False, unchanged_columns)
        unchanged_columns = filter(lambda x: x.unchanged == True, unchanged_columns)

        # remove columns
        self.removeColumns(removeable_columns)

        # add columns
        self.addColumns(new_columns)

        # change columns ??
        self.changeColumns(changed_columns)

        self.createLogicalIds()

        return None


    def removeColumns(self, removeable_columns):
        if len(removeable_columns) == 0:
            return None
        drop_string = ", ".join(map(lambda x: x.dropString(), removeable_columns))
        return self.alterTable(drop_string)


    def addColumns(self, new_columns):
        if len(new_columns) == 0:
            return None
        add_string = ", ".join(map(lambda x: x.addString(), new_columns))
        return self.alterTable(add_string)


    def changeColumns(self, changed_columns):
        return map(lambda x: x.changeColumn(), changed_columns)


    def addNewForeignKeys(self):
        columns_to_migrate = filter(lambda x: (x.unchanged == False and "foreign_key" in x.changes), self.columns)
        return map(lambda x: x.addForeignKey(), columns_to_migrate)


    def alterTable(self, alter_string):
        sqlquery = """
        ALTER TABLE %s %s;
        """ % (self.name, alter_string)
        return self.database.query(sqlquery)

    '''

    Migrate the foreign keys on the table

    '''


    def createForeignKeys(self):

        foreign_keys = filter(lambda x: x.foreign_key == True, self.columns)
        # TODO : We are going to need a thing in here that figures out whether
        # or not the key needs to be created.
        if len(foreign_keys) == 0:
            return None

        key_sql = map(lambda x: x.createForeignKeySql(), foreign_keys)
        sql = "ALTER TABLE %s %s;" % (
            self.name,
            ", ".join(key_sql)
        )
        print sql
        rows = self.database.query(sql)
        print rows
        return sql


    # TODO : move these around later
    def databaseIndexes(self):
        # get the indexes
        indexquery = "SHOW INDEX FROM %s" % self.name
        rows = self.database.query(indexquery)
        rows = filter(lambda x: x['Key_name'] != "PRIMARY", rows)
        return rows


    def databaseForeignKeys(self):
        keysquery = """
        SELECT
          TABLE_NAME,COLUMN_NAME,CONSTRAINT_NAME,
          REFERENCED_TABLE_NAME,REFERENCED_COLUMN_NAME
        FROM
          INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE
          TABLE_NAME = '%s';
        """ % self.name
        rows = self.database.query(keysquery)
        return rows


    def databaseConstraints(self):
        constraintquery = """
        SELECT *
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
        WHERE CONSTRAINT_SCHEMA = '%s'
        AND TABLE_NAME = '%s'
        AND CONSTRAINT_TYPE = 'UNIQUE'
        """ % (self.database.config.database, self.name)
        rows = self.database.query(constraintquery)
        return rows


    def databaseDefinition(self):
        if len(self.database_columns) > 0:
            return self.database_columns

        indexes = self.databaseIndexes()
        foreign_keys = self.databaseForeignKeys()
        constraints = self.databaseConstraints()

        # get the columns
        rows = self.database.query("SHOW FULL COLUMNS FROM %s" % self.name)
        for row in rows:
            potential_indexes = filter(lambda x: x.get("COLUMN_NAME", "") == row['Field'], indexes)
            potential_keys = filter(lambda x: x.get("Column_name", "") == row['Field'], foreign_keys)
            potential_constraints = filter(lambda x: x.get("CONSTRAINT_NAME", "") == row['Field'], constraints)

            # we need to get the logical id (this feels like it should be a batch thing)

            column_def = {
                "column_name" : row['Field'],
                "data_type" : row['Type'].upper(),
                "index" : (len(potential_indexes) > 0), # can't find this yet either
                "primary_key" : (row['Key'] == "PRI"),
                "auto_increment" : ("auto_increment" in row['Extra']),
                "comment" : row['Comment'],
                "unique" : (len(potential_constraints) > 0), # can't see this yet either
                "not_null" : (row['Null'] != "NO"),
                "default" : row['Default'],
                "foreign_key" : (len(potential_keys))
            }
            # took the logical_id out of the def because I am handling it in the
            # model a couple of lines down
            if len(potential_keys) > 0:
                column_def['reference_table'] = potential_keys[0]['REFERENCED_TABLE_NAME']
                column_def['reference_column'] = potential_keys[0]['REFERENCED_COLUMN_NAME']
                column_def['foreign_key_name'] = potential_keys[0]['CONSTRAINT_NAME']

            col = Column(column_def, database=self.database, table=self)
            col.retrieveLogicalId()

            self.database_columns.append(col)
        print rows
        print len(rows)
        print len(self.database_columns)

        return self.database_columns


    def initFromDatabase(self):
        return None


    def drop(self):
        dropquery = "DROP TABLE %s;" % self.name
        self.database.query(dropquery)
        return None

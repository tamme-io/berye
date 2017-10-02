import json
import yaml
import os
import sys


from Database import Database

LOGICAL_IDS_NAME = "berye_logical_ids"


'''
So this is where most of the translation difficulty is:

_constraints:_
not_null
unique
primary_key
foreign_key
check (logical expression)
default (value)


_attributes:_
auto_increment
storage {disk | memory | default}
comment string
column_format {fixed | dynamic | default}

what about references?



'''


class Column(object):


    def __init__(self, column_data, database=None, table=None):
        self.table = table
        self.database = database
        self.logical_id = column_data.get("logical_id")

        self.column_name = column_data.get("column_name")
        self.data_type = column_data.get("data_type")

        # this might be covered in the data type
        self.max_length = column_data.get("max_length")

        # this will be covered by the second pass
        self.index = column_data.get("index", False)

        # theoretically this should be covered in the first pass, I think?
        self.primary_key = column_data.get("primary_key", False)


        self.auto_increment = column_data.get("auto_increment", False)
        self.comment = column_data.get("comment")
        self.unique = column_data.get("unique", False)
        self.not_null = column_data.get("not_null", False)
        self.check = column_data.get("check")
        self.default = column_data.get("default")
        self.references = column_data.get("references", [])

        # Handling Foreign Keys
        self.foreign_key = column_data.get("foreign_key", False)
        self.reference_table = column_data.get("reference_table")
        self.reference_column = column_data.get("reference_column", "id")

        self.previous_column = None
        self.future_column = None

        self.unchanged = True
        self.changes = []

        if self.reference_table:
            key_name = "%s_%s" % (self.reference_table, self.reference_column)
            self.foreign_key_name = column_data.get("foreign_key_name", key_name)
        else:
            self.foreign_key_name = column_data.get("foreign_key_name")

        return None


    def sql(self):
        sql = ("%s %s" % (self.column_name, self.data_type))

        if self.not_null:
            sql += " NOT NULL"

        if self.default is not None:
            sql += (" DEFAULT %s" % self.default)

        if self.auto_increment:
            sql += " AUTO_INCREMENT"

        if self.primary_key:
            sql += " PRIMARY KEY"

        if self.comment is not None:
            sql += (" COMMENT '%s'" % self.comment)

        if self.unique:
            sql += " UNIQUE"

        if self.check is not None:
            sql += (" CHECK (%s %s)" % (self.column_name, self.check))

        return sql


    def createForeignKeySql(self):
        if not self.foreign_key:
            return None
        sql = "ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)" % (
            self.foreign_key_name,
            self.column_name,
            self.reference_table,
            self.reference_column
        )
        return sql


    def dropForeignKeySql(self):
        if not self.foreign_key:
            return None
        return ""


    def findPreviousColumn(self, database_columns):
        previous_columns = filter(lambda x: x.logical_id == self.logical_id, database_columns)
        print "---"
        print "FINDING PREVIOUS COLUMN"
        print "%s Database Columns To Search" % str(len(database_columns))
        print "---"
        if len(previous_columns) > 0:
            print "FOUND A PREVIOUS COLUMN"
            print previous_columns[0].column_name
            self.previous_column = previous_columns[0]
            self.previous_column.future_column = self
            print "***"
            return self.previous_column
        return None


    def matchesPreviousColumn(self):
        attributes_to_match = ("data_type", "column_name", "index",
            "primary_key", "auto_increment", "unique", "not_null", "default")
        # what are the things that we are going to want to see in here?
        differences = []

        for attribute in attributes_to_match:
            if getattr(self, attribute) != getattr(self.previous_column, attribute):
                differences.append(attribute)

        # TODO : this is code smell, find a better way to put this.
        foreign_key_attributes_to_match = ("foreign_key", "reference_table", "reference_column")
        for attribute in foreign_key_attributes_to_match:
            if getattr(self, attribute) != getattr(self.previous_column, attribute):
                if "foreign_key" not in differences:
                    differences.append("foreign_key")

        self.changes = differences
        if len(self.changes) > 0:
            self.unchanged = False

        return self.unchanged


    def changeColumn(self):
        print "About to change the column: %s" % self.column_name
        print "Changes to make: %s" % str(self.changes)

        # remove foreign key if necessary
        if "foreign_key" in self.changes:
            self.removeForeignKey()

        # remove the index if necessary
        if "index" in self.changes and self.previous_column.index == True:
            self.removeIndex()

        modify_triggers = ("column_name", "data_type", "not_null", "default", "auto_increment", "primary_key", "unique")
        if any((x in self.changes for x in modify_triggers)):
            # NB : This will fail if it's an incompatible data type, instead of overriding
            # at the moment we want this behaviour but that may change in the future
            # if we have a FORCE command added. It's just that this was the most
            # logical way to not lose the data
            self.modifyColumn()

        # add the index if appropriate
        if "index" in self.changes and self.index == True:
            self.addIndex()

        return None


    def removeForeignKey(self):
        sqlquery = """
            ALTER TABLE %s DROP FOREIGN KEY %s;
        """ % (self.table.name, self.previous_column.foreign_key_name)
        return self.database.query(sqlquery)


    def removeIndex(self):
        sqlquery = """
            DROP INDEX %s ON %s;
        """ % (self.column_name, self.table.name)
        return self.database.query(sqlquery)


    def modifyColumn(self):
        if "column_name" in self.changes:
            # change
            modify_string = "CHANGE %s %s" % (self.previous_column.column_name, self.sql())
        else:
            # modify
            modify_string = "MODIFY %s" % self.sql()
        sqlquery = """
            ALTER TABLE %s %s;
        """ % (
            self.table.name,
            modify_string
        )
        return self.database.query(sqlquery)


    def addIndex(self):
        sqlquery = """
            ADD INDEX %s ON %s;
        """ % (self.column_name, self.table.name)
        return self.database.query(sqlquery)


    def addForeignKey(self):
        return self.database.query(self.createForeignKeySql())


    def dropString(self):
        return "DROP COLUMN %s" % self.column_name


    def addString(self):
        return "ADD COLUMN %s" % self.sql()


    def updateLogicalId(self):
        sqlquery = """
            SELECT id FROM %s WHERE table_name = '%s' AND logical_id = '%s'
        """ % (LOGICAL_IDS_NAME, self.table.name, self.logical_id)
        rows = self.database.query(sqlquery)
        if len(rows) == 0:
            # create it from scratch
            insertquery = """
                INSERT INTO %s (table_name, column_name, logical_id) VALUES
                ('%s', '%s', '%s');
            """ % (
                LOGICAL_IDS_NAME,
                self.table.name,
                self.column_name,
                self.logical_id
            )
            print insertquery
            self.database.query(insertquery)
        else:
            # this needs to be an update
            updatequery = """
                UPDATE %s SET column_name = '%s' WHERE id = %s;
            """ % (
                LOGICAL_IDS_NAME,
                self.column_name,
                rows[0]['id']
            )
            print updatequery
            self.database.query(updatequery)
        return None


    def retrieveLogicalId(self):
        if self.logical_id is not None and self.logical_id != "":
            sqlquery = """
                SELECT id, column_name FROM %s WHERE table_name = '%s' AND logical_id = '%s';
            """ % (
                LOGICAL_IDS_NAME,
                self.table.name,
                self.logical_id
            )
            rows = self.database.query(sqlquery)
            if len(rows) > 0:
                return rows[0]

        # if it couldn't be found that way then try finding it via table and column
        backupquery = """
            SELECT id, column_name, logical_id FROM %s WHERE table_name = '%s' AND column_name = '%s';
        """ % (
            LOGICAL_IDS_NAME,
            self.table.name,
            self.column_name
        )
        rows = self.database.query(backupquery)
        if len(rows) > 0:
            # in here we should be setting the logical_id
            self.logical_id = rows[0]['logical_id']
            return rows[0]
        return None


    def exists(self):
        return None

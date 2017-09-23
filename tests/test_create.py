import pytest
import os
import sys
from random import randint


# TEST FILES
source_path = os.path.realpath(os.path.dirname(__file__)+"/../berye")
print "source path: %s" % str(source_path)
sys.path.append(source_path)

import Berye

class TestCreate(object):

    def test_createDatabase(self):
        assert 0

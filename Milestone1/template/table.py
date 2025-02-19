from page import *
from time import time
from index import *

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

class Record:
    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.tail_list = []
        self.base_list = []
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.index = Index()
        self.ridcounter = 0

    def __merge(self):
        pass

from table import Table
from __init__ import *
import os.path
from os import path
import json
import sys

class Database():

    def __init__(self):
        self.tables = []

    def __del__(self):
        print ("Closing database, saving %d table(s) to disk..."% len(self.tables))

    def open(self, file_name):
        file_name = file_name [2:] + ".json"
        self.file_name = file_name
        if (path.exists(self.file_name)):
            return

        else:
            print("Creating file for first time")
            with open(self.file_name, "w+") as write_file:
                return


    def close(self):
        for idi, i in enumerate(self.tables):
            self.tables[idi].close = True
            self.tables[idi].merge_thread.join()
            for idj, j in enumerate(i.buffer_pool.buffer):
                if (j):
                    print(j.bookindex)
                    self.tables[idi].dump_book_json(j)
        del self #Rip database


    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key):
        # CHECK IF TABLE EXISTS INSIDE THE DB FILE
        with open(self.file_name, "r") as read_file:
            try:
                data = json.load(read_file)
                if name in list(data.keys()):
                    print("Table exists in file, reconstructing meta data...")
                    self.tables.append(Table(name, num_columns, key, self.file_name))
                    self.tables[-1].construct_pd_and_index()
                    self.tables[-1].merge_thread.start()
                    set_last_table( self.tables[-1])
                    return self.tables[-1]
                else:
                    print("Table does not exist in data file")
                    self.tables.append(Table(name, num_columns, key, self.file_name))
                    with open(self.file_name, 'w+') as write_file:
                        data[name] = {}
                        json.dump(data, write_file)
                    self.tables[-1].merge_thread.start()


                    return self.tables[-1]
            except ValueError:
                print("Creating database file for first time")
                self.tables.append(Table(name, num_columns, key, self.file_name))
                self.tables[-1].merge_thread.start()
                set_last_table(self.tables[-1])
                return self.tables[-1]
            return table



    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        #remove table from disk
        self.table.close = True
        self.table.merge_thread.join()
        with open(self.file_name, 'r') as read_file:
            data = json.load(read_file)

        if name in data:
            print("name found in data, deleting table from disk")
            del data[name]
            if (len(data) != 0):
                with open(self.file_name, 'w') as write_file:
                    data = json.dump(data, write_file)
            else:
                os.remove(self.file_name)
                with open(self.file_name, 'w+') as write_file:
                    return

        for idi, i in enumerate(self.tables):
            if (i.name == name):
                self.tables.pop(idi)

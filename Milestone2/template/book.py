from page import *
from record import Record

class Book:

    def __init__(self, *param):
        if isinstance(param[0], int):
            self.bookindex = param[1]
            self.content = [Page(), Page(), Page(), Page(), Page()]
            self.book_indirection_flag = -1
            self.where_userData_starts = len(self.content)# so we can skip past the mettaData
            for i in range(param[0]):
                self.content.append(Page())
        else:
            self.bookindex = -1
            self.content = param
        self.dirty_bit = False
        self.tps = (2**64) - 1

    def book_insert(self, *columns):
        self.dirty_bit = True
        columns = columns[0]
        if(len(columns) > len(self.content)):
            print("ERROR: Trying to insert too many columns")
            exit()

        for idx, i in enumerate(columns):
            self.content[idx].write(i)

        return [self.bookindex, self.content[0].num_records - 1]

    def rid_to_zero(self, index):
        self.dirty_bit = True
        self.content[1].delete(index)

    def get_indirection(self, index):
        return self.content[0].read(index)

    # Returns value at page and index.
    # This read is book read calling the page read.
    # index = row# for each book
    def read(self, index, column):
        if index == 512:
            print("read index:" +str(index))
            print("read column:" +str(column))
        return self.content[column].read(index)

    def get_full_record(self, index):
        columns = []
        for i in range(len(self.content)):
            columns.append(self.read(index, i))
        return columns

    def record(self, index, keyindex, query_columns): #returns latest record (even if in tail)
        record = Record(self.read(index, 1), self.read(index, self.where_userData_starts + keyindex), [])
        columns = [None]*(len(self.content) - self.where_userData_starts)

        for i in range(self.where_userData_starts, len(self.content)):
            if query_columns[i - self.where_userData_starts]:
                columns[i - self.where_userData_starts] = self.read(index, i)

        record.columns = columns
        return record

    def set_flag(self, val):
        self.dirty_bit = True
        self.book_indirection_flag = val

    def set_meta_zero(self, val, row):
        self.dirty_bit = True
        self.content[0].update(val, row)

    #returns true if book is full.
    def is_full(self):
        if self.content[1].num_records < 512:
            return False
        else:
            return True

    #returns how many rows available in pages.
    def space_left(self):
        return 512 - self.content[3].num_records

    # Don't delete!! Will's and Draco's function
    # Return the number of record in a specific page in the books
    def page_num_record(self):
        return self.content[1].num_records

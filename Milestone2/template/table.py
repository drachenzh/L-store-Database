import json
import os.path
from os import path
import sys
import copy
from page import *
from time import time
from index import *
from buffer import *
from book import Book
import threading
import math

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
BASE_ID_COLUMN = 4


class Table:
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key, file_name = None):
        if (file_name):
            self.file_name = file_name
        else:
            print("No file name provided for table")
            self.file_name = ""
        self.name = name
        self.key = key
        self.buffer_pool = Buffer(self.key)
        self.num_columns = num_columns
        self.page_directory = {}
        self.ridcounter = 0
        self.tidcounter = (2**64) - 1
        self.index = [None] * num_columns
        self.index[key] = Index()
        self.last_written_book = [None, None, None] #[book index #, 0 book is not full or 1 for book is full, -1 book is on disk (any other number book is in buffer pool)]
        self.book_index = 0
        self.merge_queue = []
        self.close = False
        self.merge_thread = threading.Thread(target=self.merge)
        self.lock = threading.Lock()

    def create_index(self, col):
        if (col >= self.num_columns):
            print("No can do, pal. Column outta range.")
        elif (self.index[col] != None):
            print("No can do, pal. Index already created.")
        else:
            self.index[col] = Index()
            self.construct_index_by_col(col)

    def drop_index(self, col):
        if (col >= self.table.num_columns):
            print("No can do, pal. Column outta range.")
        elif (self.table.index[col] != None):
            self.table.index[col] = None

    def __del__(self):
        print ("%s: has been writen to file and deleted from buffer"%self.name)

    """
    set book uses book_in_bp and pull book an conbinds them together and returns
    the location of were the book is stored in the bp
    """
    def set_book(self,bookid):
        check = self.book_in_bp(bookid)
        if check != -1: #book is in dp just return its location in dp
            self.buffer_pool.pin(check)
            return check

        else: #book not in bp put it into bp
            return self.pull_book(bookid) #book is now in dp return its location in dp

    def push_book(self, ind):
        if self.buffer_pool.buffer[ind] != None and self.buffer_pool.buffer[ind].dirty_bit == True:
            self.dump_book_json(self.buffer_pool.buffer[ind])

    def check_basebook_in_buffer(self, basebook_index):
        for i in range(0, len(self.buffer_pool.buffer) - 1):
            if self.buffer_pool.buffer[i].bookindex == basebook_index:
                return [True, i]
        return [False, -1]


    def merge(self):

        while True:

            if self.close == True and len(self.merge_queue) == 0:
                return

            if len(self.merge_queue) != 0:
                index = self.merge_queue.pop(0)
                tailind = self.set_book(index)
                bid = self.buffer_pool.buffer[tailind].read(1,4)
                baseindex = self.set_book(self.page_directory[bid][0])
                self.merge_base_and_tail(baseindex, tailind)

    def merge_base_and_tail(self, base_bp, tail_bp):
        # Copy the selected base book and set the book
        # index to be -1.
        copybook = copy.deepcopy(self.buffer_pool.buffer[base_bp])

        # find the last row# of this full tail book
        tail_num_records = self.buffer_pool.buffer[tail_bp].page_num_record()

        # update the record in base book based on what we have in tail book
        for index in reversed(range(tail_num_records)):
            cur_bid = self.buffer_pool.buffer[tail_bp].read(index, 4)   # current row's bid
            cur_tid = self.buffer_pool.buffer[tail_bp].read(index, 1)   # current row's tid
            base_record_index = self.page_directory[cur_bid][1]   # get the base record index in its book
            base_ind = self.buffer_pool.buffer[base_bp].read(base_record_index, 0)   # get the inderection value for this base record

            # only replace the base record when indirection value = tid, which means this tail record store the newest information of this base record
            if base_ind == cur_tid:
                # get the full record of this line
                tail_record = self.buffer_pool.buffer[tail_bp].get_full_record(index)
                for m in range(5, len(copybook.content)):
                    # change the single line of copybook
                    copybook.content[m].update(tail_record[m], base_record_index)


        # assign the TPS to the copybook
        copybook.tps = self.buffer_pool.buffer[tail_bp].read(tail_num_records-1, 1)

        # Overwrite the indirection column from old book
        # to the copy book in case an update happended
        # during the merge process.
        copybook.content[0] = self.buffer_pool.buffer[base_bp].content[0]

        # Swap the book index change the copybook index to the old basebook
        self.lock.acquire()

        # push copy book to the buffer
        self.buffer_pool.buffer[base_bp] = copybook

        self.lock.release()

        # Unpin the books.
        self.buffer_pool.unpin(base_bp)
        self.buffer_pool.unpin(tail_bp)

    def pull_book(self, bookindex):
        slot = self.make_room()
        self.buffer_pool.buffer[slot] = self.pull_book_json(bookindex)
        return slot


    #Makes room for a new book to be inserted into bp
    def make_room(self):
        # Check if any empty slots
        slot = -1
        for idx, i in enumerate(self.buffer_pool.buffer):
            if i == None:
                slot = idx
                self.buffer_pool.pin(slot)
                return slot

        # if no empty slots
        if slot == -1:
            # replacement time
            slot = self.buffer_pool.find_LRU()

            # if the book is dirty
            self.push_book(slot)

        # Now slot is ready to be pulled to
            self.buffer_pool.pin(slot)
        return slot

    def pull_book_json(self, book_number):
        with open(self.file_name, "r") as read_file:
            data = json.load(read_file)
            data = data[self.name][str(book_number)]
            loaded_book = Book(len(data['page']) - 5, book_number)
            for idi, i in enumerate(data['page']):
                loaded_book.content[idi].data = eval(i)
            loaded_book.book_indirection_flag = data['i_flag']
            loaded_book.tps = data['tps']


            for i in range(512):
                if loaded_book.content[1].read_no_index_check(i) != 0:
                    for page in loaded_book.content:
                        page.num_records += 1

            return loaded_book

    def book_in_bp(self, bookid):
        for idx, i in enumerate(self.buffer_pool.buffer):
            if i != None:
                if (i.bookindex == bookid):
                    return idx
        return -1

    def dump_book_json(self, actualBook):
        book_number = actualBook.bookindex
        if (path.exists(self.file_name)):
            with open(self.file_name, "r") as read_file:
                try: #file exists and is not empty
                    data = json.load(read_file)

                    book_data = {str(book_number): []}
                    page_data = {'page': [], 'i_flag': actualBook.book_indirection_flag, 'tps': actualBook.tps}
                    for idj, j in enumerate(actualBook.content):
                        page_data['page'].append( str(j.data))
                    data[self.name][str(book_number)] = page_data
                    with open(self.file_name, "w") as write_file:
                        json.dump(data, write_file, indent=2)

                except ValueError:
                    book_data = {str(book_number): []}
                    data = {self.name: {str(book_number) :{'page': [], 'i_flag': actualBook.book_indirection_flag, 'tps': actualBook.tps}}}
                    for idj, j in enumerate(actualBook.content):
                        data[self.name][str(book_number)]['page'].append(str(j.data))
                    with open(self.file_name, "w") as write_file:
                         json.dump(data, write_file, indent=2)


    """
    reads all data in file and uses rid and key to reconstruct entire page page_directory
    and reconstructs primary index
    """
    def construct_pd_and_index(self):
        with open(self.file_name, "r") as read_file:
            data = json.load(read_file)
            data = data[self.name]

            for idx, x in enumerate(data):
                #print(int(x))
                book_number = idx
                rid_page = Page()
                sid_page = Page()
                bid_page = Page()
                ind_page = Page()
                tbd = Page()

                rid_page.data = eval(data[str(x)]['page'][RID_COLUMN])
                sid_page.data = eval(data[str(x)]['page'][self.key + 5])
                bid_page.data = eval(data[str(x)]['page'][BASE_ID_COLUMN])
                ind_page.data = eval(data[str(x)]['page'][INDIRECTION_COLUMN])
                for page_index in range(512):
                    rid = rid_page.read_no_index_check(page_index)
                    bid = bid_page.read_no_index_check(page_index)
                    sid = sid_page.read_no_index_check(page_index)
                    ind = ind_page.read_no_index_check(page_index)
                    if (rid != 0 and bid == rid):
                        if (ind != 0):
                            #tail_book_to_add = math.floor((2**64 - 2 - ind)/512) + 2
                            for i in data:
                                tbd.data = eval(data[str(i)]['page'][RID_COLUMN])
                                for page_index2 in range(512):
                                    #print("HERE %d" %ind)
                                    t = tbd.read_no_index_check(page_index2)
                                    if (t == ind):
                                        #print("HEREeeee %d" %ind)
                                        self.page_directory[ind] = [int(i), page_index2]
                                        break


                            #self.page_directory[ind] = [book_number, page_index]
                        self.page_directory[rid] = [int(x), page_index]
                        self.index[self.key].index[sid] = [rid]

    def construct_index_by_col(self, col):
        print(col)
        for i in range(len(self.buffer_pool.buffer)):
            self.push_book(i)

        with open(self.file_name, "r") as read_file:
            data = json.load(read_file)
            data = data[self.name]

            # Going through each book. x is bookid
            for idx, x in enumerate(data):
                rid_page = Page()
                col_page = Page()
                bid_page = Page()

                # Saving the data into the pages from database
                rid_page.data = eval(data[str(x)]['page'][RID_COLUMN])
                col_page.data = eval(data[str(x)]['page'][col + 5])
                bid_page.data = eval(data[str(x)]['page'][BASE_ID_COLUMN])
                for page_index in range(512):
                    rid = rid_page.read_no_index_check(page_index)
                    bid = bid_page.read_no_index_check(page_index)
                    cvl = col_page.read_no_index_check(page_index)
                    if (rid != 0 and bid == rid):
                        self.index[col].add_to_index(cvl, rid)

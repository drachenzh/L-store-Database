from table import *
from index import Index
from book import *
from record import Record
import sys
from  __init__ import writeLock


INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
BASE_ID_COLUMN = 4

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    """

    def __init__(self, table):
        self.table = table
        pass

    """
    # internal Method
    # Read a record with specified RID
    """
    #bring into buffer
    #set rid to rid_to_zero
    #remove from pd and primary indexing
    def delete(self, key):
        rid = self.table.index[0].locate(key)
        location = self.table.page_directory[rid[0]]

        buffer_index = self.table.set_book(location[0])
        self.table.buffer_pool.buffer[buffer_index].rid_to_zero(location[1])
        self.table.index[0].drop_index(key)
        self.table.buffer_pool.unpin(buffer_index)


    """
    # Insert a record with specified columns
    """

    def insert(self, *columns):
        #putting metta data into a list and adding user
        #data to the list
        data = list(columns)
        self.table.ridcounter = self.table.ridcounter + 1
        mettaData = [0,self.table.ridcounter,0,0,self.table.ridcounter]
        mettaData_and_data = mettaData + data
        location = []
        lastw = self.table.last_written_book

        # if no books or last base book full
        if (lastw[0] == None or lastw[1] == 1):
            idx = self.table.make_room()

            self.table.buffer_pool.buffer[idx] = Book(len(columns), self.table.book_index)

            lastw = [self.table.book_index, 0, idx]
            global writeLock
            with writeLock:
                self.table.book_index += 1

        # there is an available book.
        else:
            idx = self.table.set_book(lastw[0])

        idx = lastw[2]
        location = self.table.buffer_pool.buffer[idx].book_insert(mettaData_and_data)

        if(self.table.buffer_pool.buffer[idx].is_full()):
            lastw[1] = 1

        self.table.buffer_pool.unpin(idx)
        self.table.last_written_book = lastw

        #Setting RID key to book location value.
        self.table.page_directory[self.table.ridcounter] = location
        for i in range(len(self.table.index)):
            if(self.table.index[i] != None):
                self.table.index[i].add_to_index(data[i], mettaData[1])


    """
    # Read a record with specified key
    """


    def select(self, key, col, query_columns):

        # ###### latch a page in Book####################################################
        # #checking if book is locked or not
        # rid = self.table.index[self.table.key].locate(key)
        # book_index = self.table.page_directory[rid][0]
        # search_key = str(book_index) + "$" + str(query_columns)
        #
        # #not in the latch dictionary
        # if search_key not in self.table.latch_book:
        #     self.table.latch_book[search_key] = True
        # else:   # inside the dict
        #     while self.table.latch_book[search_key] == True :    # when it's locked wait until the lock be released
        #         continue
        #
        #     self.table.latch_book[search_key] = True    # set to true and release it when this select finish
        #
        # ########## finish latch checking process ######################################



        records = []
        if(self.table.index[col] == None):
            # do scan
            print("mc-scan")
            #self.table.latch_book[search_key] = False   # set the book to unlock
            return records

        RID_list = self.table.index[col].locate(key)
        # print(RID_list)

        #Taking RIDS->location and extracting records into record list.
        for i in RID_list:
            location = self.table.page_directory[i]
            ind = self.table.set_book(location[0])
            booky = self.table.buffer_pool.buffer[ind]


            ##We check the indirection and see that it exists and then
            ##we try and read it from the pd but its not there because merge?
            check_indirection = booky.get_indirection(location[1])
            if booky.read(location[1], 1) != 0: #checking to see if there is a delete
                if check_indirection == 0 or check_indirection >= booky.tps: #no indirection or old update
                    records.append(booky.record(location[1], self.table.key, query_columns))
                    self.table.buffer_pool.unpin(ind)
                else: #there is an indirection that is valid
                    self.table.buffer_pool.unpin(ind)
                    temp = self.table.page_directory[check_indirection]
                    tind = self.table.set_book(temp[0])
                    tbooky = self.table.buffer_pool.buffer[tind]

                    records.append(tbooky.record(temp[1], self.table.key, query_columns))
                    self.table.buffer_pool.unpin(tind)
            else:
                temp_rec = Record(location[1], self.table.key, ([0] * self.table.num_columns))
                records.append(temp_rec)

       # self.table.latch_book[search_key] = False  # set the book to unlock
        return records

    """
    # Update a record with specified key and columns
    """
    def update(self, key, *columns):
        #columns will be stored in weird tuples need to fix
        #UPDATE needs to change read in books to handle inderection
        #ONLY EDIT TAIL PAGES (tail_list)
        RID = self.table.index[self.table.key].locate(key)

        if RID == None:
            return False

        location = self.table.page_directory[RID[0]] # returns [book num, row]
        indirection_location = location

        data = list(columns)

        ######################## Latch process to ensure the tid counter only able to touch by one transaction at a time #####################################################
        waittime = 0
        while self.table.latch_tid == True:    # break the loop when not other thread using tid_counter
            waittime += 1
            #continue

        self.table.latch_tid = True                 # lock the tid_counter to prevent other transaction touch it
        self.table.tidcounter = self.table.tidcounter - 1
        tid_count = self.table.tidcounter           # assign current tid-counter to a temp variable so that we can release the tid_counter's lock right after that
        self.table.latch_tid = False                # finish using tid_counter for current transaction and release the lock

        if waittime > 0:
            print("Latching. Wait time:" + str(waittime))
        ######################## Latch process to ensure the tid counter only able to touch by one transaction at a time #####################################################

        pin_idx_list = []           #holds a list of idx that asosetate to  what has been pinned during update
        tail_location = [-1, -1]    #for later use
        tail_book_R_bp = -1         #for later use
        new_record =[]              #for later use

        """
        step 1) were is the book located eather on disk or in buffer_pool? do a search
        """
        base_book_bp = self.table.set_book(location[0]) #now holds the location of where book is stored in bp
        check_indirection =  self.table.buffer_pool.buffer[base_book_bp].get_indirection(location[1])
        pin_idx_list.append(base_book_bp)




        if check_indirection == 0:
        #constructing the full new record
            new_record = self.table.buffer_pool.buffer[base_book_bp].get_full_record(location[1])
            for idx, i in enumerate(data):
                if i != None:
                    new_record[idx + 5] = i
            new_record[1] = tid_count #note that the rid of the base record is already in the BASE_ID_COLUMN thanks to insert


        else: # there is indirection
            tail_location = self.table.page_directory[check_indirection] #[Book num, row num]
            tail_book_R_bp = self.table.set_book(tail_location[0])

            new_record = self.table.buffer_pool.buffer[tail_book_R_bp].get_full_record(tail_location[1])
            for idx, i in enumerate(data):
                if i != None:
                    new_record[idx + 5] = i
            new_record[INDIRECTION_COLUMN] = new_record[RID_COLUMN] # new record now points to the second newest record almost like a linked list
            new_record[RID_COLUMN] = tid_count #note that the rid of the base record is already in the BASE_ID_COLUMN thanks to insert
            self.table.buffer_pool.unpin(tail_book_R_bp)

        """
        NOW New_record holds the value that i wish to append to a tail book
        """
        indir_flag = self.table.buffer_pool.buffer[base_book_bp].book_indirection_flag
        if indir_flag == -1: #need a new tail book
            new_slot = self.table.make_room()   #make room
            self.table.buffer_pool.buffer[new_slot] = Book(len(columns), self.table.book_index) #add book
            location = self.table.buffer_pool.buffer[new_slot].book_insert(new_record)#add record to book
            self.table.buffer_pool.buffer[base_book_bp].set_flag(self.table.book_index) #set indirection flag in base book

            with writeLock:
                self.table.book_index += 1
            pin_idx_list.append(new_slot)

        else: #there is an availabe book to write to
            slot = self.table.set_book(indir_flag) #bring tail book onto the bp
            location = self.table.buffer_pool.buffer[slot].book_insert(new_record) #add record to book


            pin_idx_list.append(slot)
            if self.table.buffer_pool.buffer[slot].is_full(): # tail book is full set flag to -1
                self.table.buffer_pool.buffer[base_book_bp].set_flag(-1)

                #DOOOOO MERGE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                self.table.merge_queue.append(self.table.buffer_pool.buffer[slot].bookindex)


        self.table.page_directory[tid_count] = location
        #update base_book indirection with new TID
        # This is to avoid updating while books are being swapped.
        # Although this should rarely happen since merge waits for the book to be unused to swap books.
        self.table.buffer_pool.buffer[base_book_bp].set_meta_zero(tid_count, indirection_location[1])

        for i in pin_idx_list:
            self.table.buffer_pool.unpin(i)

        return True

    """
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int  # Index of desired column to aggregate
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        sum = 0

        # force start_range < end_range
        temp = start_range
        start_range = min(start_range, end_range)
        end_range = max(temp, end_range)

        current_key = start_range

        while current_key <= end_range:      # doing traversal from the start key_value to the end key_value
            if self.table.index[self.table.key].contains_key(current_key):
                query_column = []

                # initialize the column list with all 0 and mark the target column to 1
                for i in range(self.table.num_columns):
                    if i == aggregate_column_index:
                        query_column.append(1)
                    else:
                        query_column.append(0)

                # apply select function to find the corresponding value of given SID and column#, adding all found value to sum

                sum += self.select(current_key, 0, query_column)[0].columns[aggregate_column_index]
            current_key += 1

        return sum

    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r.columns[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False

    def change_link(self, key):
        #PINS AND UNPINS and Dirty bit
        #i am assuming that we get the primary key but if we don't we will have to make small changes to the code to handle it

        rid = self.table.index[0].locate(key) #get the rid of the key
        base_book_id, base_row = self.table.page_directory[rid] #get base book id and row of the base book record we wish to change the inderection
        base_book_bp = self.table.set_book(base_book_id)

        record_to_be_deleted_tid = self.table.buffer_pool.buffer[base_book_bp].get_indirection(base_row) #get the tid of the record we wish to get rid of
        tail_book_id, tail_row = self.table.page_directory(record_to_be_deleted_tid) #get tail book info
        tail_book_bp = self.table.set_book(tail_book_id)
        record = self.table.buffer_pool.buffer[tail_book_bp].get_full_record(tail_row)

        previous_updated_record = record[INDIRECTION_COLUMN]  #get the prevous updated record
        self.table.buffer_pool.buffer[tail_book_bp].rid_to_zero(tail_row)
        self.table.buffer_pool.buffer[base_book_bp].set_meta_zero(previous_updated_record, base_row)# change the inderection collumn of the base record

        self.table.buffer_pool.unpin(tail_book_bp)
        self.table.buffer_pool.unpin(base_book_bp)
        #############################################################

        # tail_book_id,row = self.table.page_directory[tid] #get book id and row of the record we wish to remove
        # tail_book_bp = self.table.set_book(tail_book_id) #make sure tail book is in bp
        #
        # record = self.table.buffer_pool.buffer[tail_book_bp].get_full_record(row) #get the full record
        # self.table.buffer_pool.buffer[tail_book_bp].rid_to_zero(row) #set the delete veriable in the record we wish to get rid of
        #
        # previous_updated_record = record[INDIRECTION_COLUMN] # this is the prevous updated record that will now become the most current updated record
        # base_record_rid = record[BASE_ID_COLUMN] # get the rid of the base record
        #
        # BRR_book_id, BRR_row = self.table.page_directory[base_record_rid] #get base book id and row of the base book record we wish to change the inderection
        # base_book_bp = self.table.set_book(BRR_book_id) # make sure the base book is in the bp
        #
        # self.table.buffer_pool.buffer[base_book_bp].set_meta_zero(previous_updated_record, BRR_row)# change the inderection collumn of the base record
        #
        # self.table.buffer_pool.unpin(tail_book_bp)
        # self.table.buffer_pool.unpin(base_book_bp)

from table import *
from index import Index
from book import *

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

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

    def delete(self, key):
        rid = self.table.index.locate(key)
        location = self.table.page_directory[rid[0]]

        book = self.table.base_list[location[0]]
        book.rid_to_zero(location[1])


    """
    # Insert a record with specified columns
    """

    def insert(self, *columns):
        #putting metta data into a list and adding user
        #data to the list
        data = list(columns)
        self.table.ridcounter = self.table.ridcounter + 1
        mettaData = [0,self.table.ridcounter,0,0]
        mettaData_and_data = mettaData + data

        #ONLY EDIT BASE PAGES (base_list)
        #Check if self.table.base_list is empty -> add new book
        location = []
        if len(self.table.base_list) == 0:
            self.table.base_list.append(Book(len(columns), 0))
            location = self.table.base_list[-1].book_insert(mettaData_and_data)

        #Check if self.table.base_list newest book is full-> add new book
        elif self.table.base_list[-1].is_full():
            bookindex = self.table.base_list[-1].bookindex + 1
            self.table.base_list.append(Book(len(columns), bookindex))
            location = self.table.base_list[-1].book_insert(mettaData_and_data)

        #Check if self.table.base_list newest book has room -> add to end of book
        else:
            # Add data to end of newest book
            location = self.table.base_list[-1].book_insert(mettaData_and_data)

        #Setting RID key to book location value.
        self.table.page_directory[self.table.ridcounter] = location
        self.table.index.create_index(data[self.table.key], mettaData[1])


    """
    # Read a record with specified key
    """

    def select(self, key, query_columns):
        RID_list = self.table.index.locate(key)
        records = []
        #Taking RIDS->location and extracting records into record list.
        for i in RID_list:
            #location[0] = book#, location[1] = row#
            location = self.table.page_directory[i]
            check_indirection =  self.table.base_list[location[0]].get_indirection(location[1])
            if self.table.base_list[location[0]].read(location[1], 1) != 0: #checking to see if there is a delete
                if check_indirection == 0: #no indirection
                    records.append(self.table.base_list[location[0]].record(location[1], self.table.key))
                else: #there is an indirection
                    temp = self.table.page_directory[check_indirection]
                    records.append(self.table.tail_list[temp[0]].record(temp[1], self.table.key))

        for idx in enumerate(query_columns):
            if query_columns[idx[0]] == 0:
                for i in records:
                    i.columns[idx[0]] = None

        return records


    """
    # Update a record with specified key and columns
    """

    def update(self, key, *columns):
        #columns will be stored in weird tuples need to fix
        #UPDATE needs to change read in books to handle inderection
        #ONLY EDIT TAIL PAGES (tail_list)
        #print(columns) #this gives me (none,#,none,none,none)
        RID = self.table.index.locate(key)
        location = self.table.page_directory[RID[0]] # returns [book num, row]
        indirection_location = location
        check_indirection =  self.table.base_list[location[0]].get_indirection(location[1])
        data = list(columns)
        self.table.ridcounter = self.table.ridcounter + 1

        #if no inderection
        if  check_indirection == 0:
            base_data = self.table.base_list[location[0]].get_full_record(location[1])

            #mettaData = [0,self.table.ridcounter,0,0]
            #mettaData_and_data = mettaData + data

            for idx, i in enumerate(data):
                if i != None:
                    base_data[idx + 4] = i

            base_data[1] = self.table.ridcounter

            if len(self.table.tail_list) == 0:
                self.table.tail_list.append(Book(len(columns), 0))
                location = self.table.tail_list[-1].book_insert(base_data)

            #Check if self.table.base_list newest book is full-> add new book
            elif self.table.tail_list[-1].is_full():
                bookindex = self.table.tail_list[-1].bookindex + 1
                self.table.tail_list.append(Book(len(columns), bookindex))
                location = self.table.tail_list[-1].book_insert(base_data)

            #Check if self.table.base_list newest book has room -> add to end of book
            else:
                # Add data to end of newest book
                location = self.table.tail_list[-1].book_insert(base_data)

        #if there is an inderection
        else:
            location = self.table.page_directory[check_indirection]

            tail_data = self.table.tail_list[location[0]].get_full_record(location[1])
            #mettaData = [0,self.table.ridcounter,0,0]
            #mettaData_and_data = mettaData + data

            for idx, i in enumerate(data):
                if i != None:
                    tail_data[idx + 4] = i

            tail_data[1] = self.table.ridcounter

            if len(self.table.tail_list) == 0:
                self.table.tail_list.append(Book(len(columns), 0))
                location = self.table.tail_list[-1].book_insert(tail_data)

            #Check if self.table.base_list newest book is full-> add new book
            elif self.table.tail_list[-1].is_full():
                bookindex = self.table.tail_list[-1].bookindex + 1
                self.table.tail_list.append(Book(len(columns), bookindex))
                location = self.table.tail_list[-1].book_insert(tail_data)

            #Check if self.table.base_list newest book has room -> add to end of book
            else:
                # Add data to end of newest book
                location = self.table.tail_list[-1].book_insert(tail_data)


        self.table.page_directory[self.table.ridcounter] = location
        #update base_book inderection with new RID
        self.table.base_list[indirection_location[0]].content[0].update(self.table.ridcounter, indirection_location[1])


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
            if self.table.index.contains_key(current_key):
                query_column = []

                # initialize the column list with all 0 and mark the target column to 1
                for i in range(self.table.num_columns):
                    if i == aggregate_column_index:
                        query_column.append(1)
                    else:
                        query_column.append(0)

                # apply select function to find the corresponding value of given SID and column#, adding all found value to sum
                sum += self.select(current_key, query_column)[0].columns[aggregate_column_index]

            current_key += 1

        return sum

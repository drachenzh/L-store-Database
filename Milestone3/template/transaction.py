from table import Table
from record import Record
from index import Index
from query import Query
import threading
from time import process_time
from __init__ import *


class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.locks = []    # list of locks for this transaction
        self.updates = []   # use to trace back the changed record and assit undo process when abortion
        self.queries = []   # list of query for this transaction
        self.pins = []
        self.transaction_id = inc_global_counter()
        self.table = last_table()


    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, *args):
        new_args = []
        # convert arguments to a list instead of tuple
        for x in args:
            new_args.append(x)
        new_args.append(self.transaction_id)     # append transcation_id to the list of args

        self.queries.append((query, new_args))

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort

    # This MUST return 0 if transaction is sucessful, else it must return 0
    def run(self):
        #print("~Transaction # " + str(self.transaction_id))

        for query, args in self.queries:
            result = query(*args)
            #print("Query Return Value: " + str(result))
            # If the query has failed the transaction should abort
            if result == False:
                #print("Query Return Value: " + str(result) + "    " + query.__name__)
                print("Aborting transaciton #" + str(self.transaction_id))
                return self.abort()
            else:
                if query.__name__ == "increment":     # when successful acquire exclusive lock
                    self.secure_lock(args[0], True)
                    pin_list = self.table.pull_base_and_tail(args[0])    # pin the corresponding base book and tail book, release them after commit or abortion
                    for pin in pin_list:
                        self.table.buffer_pool.pin(pin)
                        self.pins.append(pin)
                    self.updates.append(args[0])
                else:
                    self.secure_lock(args[0], False)

        return self.commit()


    # exclusive = lock_type    lock_type = false is shared   true = exlcusive
    def secure_lock(self, key, lock_type):
        locky = (key, lock_type)
        if self.locks.__contains__(locky) or self.locks.__contains__((key, True)):
            return
        else:
            self.locks.append(locky)



    def abort(self):
        #TODO: do roll-back and any other necessary operations
        query = Query(self.table)

        for key in self.updates:
            query.change_link(key)
        # print(self.locks)
        # print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
        self.release_locks()
        # print(self.locks)
        # print("################################")
        self.release_pins()
        return False

    def commit(self):
        # TODO: commit to database

        self.release_locks()
        self.release_pins()
        return True

    def release_pins(self):
        for pin in self.pins:
            self.table.buffer_pool.unpin(pin)

    def release_locks(self):
        #print("Checking release!!############################################")
        for locky in self.locks:
            self.table.release_lock(locky[0], self.transaction_id)
           # rid = self.table.index[self.table.key].locate(locky[0])[0]
           # self.table.page_directory[rid][2].printList()

       # print("Checking release!!********************************************")
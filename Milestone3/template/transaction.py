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
        self.locks = []
        self.updates = []
        self.queries = []
        self.commit_pins = []
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
        print("~Transaction # " + str(self.transaction_id))

        for query, args in self.queries:
            result = query(*args)
            # print("Query Return Value: " + str(result))
            # If the query has failed the transaction should abort
            if result == False:
                # print("Query Return Value: " + str(result) + "    " + query.__name__)
                print("Aborting transaciton #" + str(self.transaction_id))
                return self.abort()
            else:
                if query.__name__ == "increment":  # when successful acquire exclusive lock
                    self.secure_lock(args[0], True)

                    commit_list = self.table.pull_base_and_tail(args[0])   # get the buffer pool index of base book and tail book
                    for pin in commit_list:                        # mark this two position with uncommit
                        self.table.buffer_pool.commit_pin[pin] += 1
                        self.commit_pins.append(pin)

                    self.updates.append(args[0])
                else:
                    self.secure_lock(args[0], False)

        return self.commit()

    # exclusive = lock_type    lock_type = false is shared   true = exlcusive
    def secure_lock(self, key, exclusive):
        locky = (key, exclusive)
        if self.locks.__contains__(locky) or self.locks.__contains__((key, True)):
            return True
        self.locks.append(locky)


    def abort(self):
        #TODO: do roll-back and any other necessary operations
        query = Query(self.table)

        for key in self.updates:
            query.change_link(key)

        self.release_locks()
        self.uncommit_pins()
        return False

    def commit(self):
        # TODO: commit to database

        self.release_locks()
        self.uncommit_pins()
        return True

    def uncommit_pins(self):
        for pin in self.commit_pins:
            self.table.buffer_pool.commit_pin[pin] -= 1

    def release_locks(self):
        for locky in self.locks:
            self.table.release_lock(locky[0], self.transaction_id)

#from table import Table


class Index:

    def __init__(self):
        self.index = {}

    def contains_key(self, sid):
        return sid in self.index

    # returns the location of all records with the given value
    def locate(self, sid):
        if sid in self.index:
            return self.index[sid]
        else:
            print("The key does not exist.")

        # given a valid sid, return the rid so we
        # find the data associated with the sid.

    # optional: Create index on SID and RID.

    def create_index(self, sid, rid):
        if not sid in self.index:
            self.index[sid] = [rid]
        else:
            self.index[sid].append(rid)

        # make a dictionary that maps sid as key to rid as value.
        # rid is passed as a tuple of book ID and row_index.

    def drop_index(self, sid):
        if sid in self.index:
            del self.index[sid]
        else:
            print("The student ID doesn't exist.")

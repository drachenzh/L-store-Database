from lru import *


class Buffer:
    #hello
    def __init__(self, key):
        self.buffer_size = 40
        self.book_range = 1
        self.buffer = [None]*self.buffer_size
        self.lru_cache = LRU_cache(self.buffer_size)
        self.commit_pin = [0] * self.buffer_size
        self.pins = [0]*self.buffer_size
        self.latchs = [0]*self.buffer_size
        self.key = key


    def get_record(self, slot, row):
        return self.buffer[slot].record(row, self.key)


    def pin(self, index):
        self.pins[index] += 1
        self.lru_cache.add_front(index)

    def unpin(self, index):
        self.pins[index] -= 1

    # returns the postion of book that is the last least recently used
    def find_LRU(self):
        indx = self.lru_cache.pop_end()

        if self.pins[indx] == 0:
            return indx
        else:
            print("ALL PINS TAKEN --BUG")
            exit()
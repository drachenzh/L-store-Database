
class Buffer:
    #hello
    def __init__(self, key):
        self.buffer_size = 40
        self.book_range = 1
        self.buffer = [None]*self.buffer_size
        self.LRU_tracker = [None]*self.buffer_size  #least resently used makes it so we can keep track of old non used books with time stamps
        self.pins = [0]*self.buffer_size
        self.latchs = [0]*self.buffer_size
        self.key = key

        for i in range(self.buffer_size):
            self.LRU_tracker[i] = self.buffer_size - i  #NOTE  how old it is position

    def get_record(self, slot, row):
        return self.buffer[slot].record(row, self.key)


    def pin(self, index):
        self.pins[index] += 1
        self.touched(index)

    def unpin(self, index):
        self.pins[index] -= 1
        self.touched(index)

    def find_LRU(self):  #returns the postion of book that is the last least recently used
        result = -1

        for i in range(self.buffer_size, 0, -1):
            for j in range(self.buffer_size):
                if i == self.LRU_tracker[j] and self.pins[j] == 0:
                    return j

        if result == -1:
            print("ALL PINS TAKEN --BUG")
            exit()

        return result

    def touched(self,index):
        index_time = self.LRU_tracker[index]

        for i in range(self.buffer_size):

            if index_time == 1: #if we are looking at the most recently used don't do anything
                break

            elif i == index: #set the touched books time to 1 (the most resently touched)
                self.LRU_tracker[i] = 1

            elif self.LRU_tracker[i] > index_time: #time is greater than time of index passed don't increment
                pass

            else: #increment time
                self.LRU_tracker[i] = self.LRU_tracker[i] + 1


"""
# TESTING LRU_tracker
temp = Buffer()
print(temp.LRU_tracker)
print(temp.find_LRU())
temp.touched(0)
print(temp.LRU_tracker)
print(temp.find_LRU())
temp.touched(0)
print(temp.LRU_tracker)
print(temp.find_LRU())
temp.touched(0)
print(temp.LRU_tracker)
print(temp.find_LRU())
temp.touched(1)
print(temp.LRU_tracker)
print(temp.find_LRU())
temp.touched(1)
print(temp.LRU_tracker)
print(temp.find_LRU())
temp.touched(4)
print(temp.LRU_tracker)
print(temp.find_LRU())
temp.touched(2)
print(temp.LRU_tracker)
"""

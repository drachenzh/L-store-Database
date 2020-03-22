from config import *

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)


    # def has_capacity(self):
    #     return self.capacity

    def write(self, value):

        if self.has_space is False:
            print("ERROR: Page full.")
            exit()

        bytevalue = (value).to_bytes(8, byteorder='big')
        i = self.num_records * 8
        #print("checking i: " + str(i))
        self.data[i + 0] = bytevalue[0]
        self.data[i + 1] = bytevalue[1]
        self.data[i + 2] = bytevalue[2]
        self.data[i + 3] = bytevalue[3]
        self.data[i + 4] = bytevalue[4]
        self.data[i + 5] = bytevalue[5]
        self.data[i + 6] = bytevalue[6]
        self.data[i + 7] = bytevalue[7]

        self.num_records += 1

    def update(self, value, index):

        bytevalue = (value).to_bytes(8, byteorder='big')
        i = index * 8

        self.data[i + 0] = bytevalue[0]
        self.data[i + 1] = bytevalue[1]
        self.data[i + 2] = bytevalue[2]
        self.data[i + 3] = bytevalue[3]
        self.data[i + 4] = bytevalue[4]
        self.data[i + 5] = bytevalue[5]
        self.data[i + 6] = bytevalue[6]
        self.data[i + 7] = bytevalue[7]

    # index = row #
    def read(self, index):
        if(index >= self.num_records):
            # print("hello:" +str(index))
            # print(index)
            # print(self.num_records)
            print("index: %d num_records: %d" % (index, self.num_records))
            print("ERROR: Index out of range. READ")
            return 0
            #exit()

        rindex = index * 8
        bytevalue = bytearray(8)

        bytevalue[0] = self.data[rindex + 0]
        bytevalue[1] = self.data[rindex + 1]
        bytevalue[2] = self.data[rindex + 2]
        bytevalue[3] = self.data[rindex + 3]
        bytevalue[4] = self.data[rindex + 4]
        bytevalue[5] = self.data[rindex + 5]
        bytevalue[6] = self.data[rindex + 6]
        bytevalue[7] = self.data[rindex + 7]

        return int.from_bytes(bytevalue, byteorder='big')

    def read_no_index_check(self, index):
        rindex = index * 8
        bytevalue = bytearray(8)

        bytevalue[0] = self.data[rindex + 0]
        bytevalue[1] = self.data[rindex + 1]
        bytevalue[2] = self.data[rindex + 2]
        bytevalue[3] = self.data[rindex + 3]
        bytevalue[4] = self.data[rindex + 4]
        bytevalue[5] = self.data[rindex + 5]
        bytevalue[6] = self.data[rindex + 6]
        bytevalue[7] = self.data[rindex + 7]

        return int.from_bytes(bytevalue, byteorder='big')


    def delete(self, index):
        if(index >= self.num_records):
            print("ERROR: Index out of range.")
            return

        bytevalue = (0).to_bytes(8, byteorder='big')
        rindex = index * 8

        self.data[rindex + 0] = bytevalue[0]
        self.data[rindex + 1] = bytevalue[1]
        self.data[rindex + 2] = bytevalue[2]
        self.data[rindex + 3] = bytevalue[3]
        self.data[rindex + 4] = bytevalue[4]
        self.data[rindex + 5] = bytevalue[5]
        self.data[rindex + 6] = bytevalue[6]
        self.data[rindex + 7] = bytevalue[7]

    def has_space(self):
        if self.num_records < 512:
            return True
        else:
            return False

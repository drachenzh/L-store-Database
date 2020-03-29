
# node in the queue, store the value with buffer pool position
class Node:
    def __init__(self, bp):
        self.buffer_indx = bp
        self.next = None
        self.prev = None


class LRU_cache:

    def __init__(self, size):
        self.queue_size = 0
        self.hash_bp = {}
        self.buffer_size = size
        self.head = None
        self.tail = None

# add to front
    def add_front(self, buffer_pos):
        add_node = Node(buffer_pos)    # create a node

        if buffer_pos not in self.hash_bp or self.hash_bp[buffer_pos] == None:  # check if node already exist in the queue
            if self.queue_size != 0:
                self.head.prev = add_node
                add_node.next = self.head
                self.head = add_node
            else:     # add to the empty queue
                self.head = add_node
                self.tail = add_node
            self.queue_size += 1
            self.hash_bp[buffer_pos] = add_node   # update hash table to node address
        else:        # node already in the queue and hash table

            if self.queue_size == 1:
                self.head = add_node
                self.tail = add_node
                self.hash_bp[buffer_pos] = add_node  # update hash table to node address
            elif self.head.buffer_indx == buffer_pos:
                pass
            elif self.tail.buffer_indx == buffer_pos:   # when the delete node is at the end of queue
                delet_node = self.hash_bp[buffer_pos]
                self.tail = self.tail.prev
                self.tail.next = None
                del delet_node
                # add node to the front
                self.head.prev = add_node
                add_node.next = self.head
                self.head = add_node
                self.hash_bp[buffer_pos] = add_node  # update hash table to new node address
            else:
                delet_node = self.hash_bp[buffer_pos]     # delete the old node
                #print("cao!!!!!!!!!!!!! " + str(buffer_pos) + " " + str(self.head.buffer_indx) + " " + str(self.tail.buffer_indx) )
                delet_node.prev.next = delet_node.next
                delet_node.next.prev = delet_node.prev
                del delet_node
                # add node to the front
                self.head.prev = add_node
                add_node.next = self.head
                self.head = add_node
                self.hash_bp[buffer_pos] = add_node  # update hash table to new node address


# pop from the end, return the buffer position which can be removed
    def pop_end(self):
        delet_node = self.tail

        pop_node_value = self.tail.buffer_indx    # update the tail pointer
        self.tail = self.tail.prev
        self.tail.next = None
        self.hash_bp[pop_node_value] = None   # update the hash table value
        self.queue_size -= 1
        del delet_node

        return pop_node_value  # return the buffer position which can be removed





class Lock:
    def __init__(self, lock_type, tran_id):
        self.lock_type = lock_type
        self.tran_id = tran_id
        self.next = None


class Lock_List:
    def __init__(self):
        self.head = None
        self.tail = None
        self.num_reader = 0
        self.num_writer = 0

    def has_exlock(self):
        return self.tail.lock_type == 1

    def same_exlock_tranID(self, tran_id):
        return self.tail.tran_id == tran_id

    def has_lock(self, tran_id, type):
        head_lock = self.head
        while head_lock is not None:
            if head_lock.tran_id == tran_id and (head_lock.lock_type == type or head_lock.lock_type == 1):
                return True
            head_lock = head_lock.next

        return False

    def append_list(self, new_lock):
        if self.head is None:
            self.head = new_lock
            self.tail = new_lock
            return

        self.tail.next =new_lock
        self.tail = new_lock
        return

    def remove_lock(self, tran_id):
        head_lock = self.head

        if head_lock is not None:
            if head_lock.tran_id == tran_id:
                self.head = head_lock.next
                head_lock = None
                return

        while head_lock is not None:
            if head_lock.tran_id == tran_id:
                break
            prev = head_lock
            head_lock = head_lock.next

        if head_lock is None:
            return

        if head_lock.next is None:
            self.tail = prev

        prev.next = head_lock.next
        head_lock = None


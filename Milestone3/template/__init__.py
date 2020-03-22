import threading
threadLock = threading.Lock()
writeLock = threading.Lock()
global_counter = 0
last_table = None

def inc_global_counter():
    with threadLock:
        global global_counter
        global_counter += 1
        return global_counter

def set_last_table(table):
    global last_table
    last_table = table

def last_table():
    return last_table

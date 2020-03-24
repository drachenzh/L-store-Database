from db import Database
from query import Query
from transaction import Transaction
from transaction_worker import TransactionWorker

import threading
from random import choice, randint, sample, seed

db = Database()
db.open('db')
grades_table = db.create_table('Grades', 5, 0)

keys = []
records = {}
num_threads = 8
seed(8739878934)
num_records = 1000
num_transactions = 100
num_operations = 5

# Generate random records
for i in range(0, num_records):
    key = 92106429 + i
    keys.append(key)
    records[key] = [key, 0, 0, 0, 0]
    q = Query(grades_table)
    q.insert(*records[key])

# create TransactionWorkers
transaction_workers = []
for i in range(num_threads):
    transaction_workers.append(TransactionWorker([]))

# generates 10k random transactions
# each transaction will increment the first column of a record 5 times
<<<<<<< Updated upstream

for i in range(1000):
    k = randint(0, 2000 - 1)
    # k = 3
=======
for i in range(num_transactions):
    k = randint(0, num_records // num_operations - 1)
>>>>>>> Stashed changes
    transaction = Transaction()
    for j in range(num_operations):
        key = keys[k * num_operations + j]
        q = Query(grades_table)
        _k = 92106429 + randint(0, num_records - 1)
        transaction.add_query(q.select, _k, 0, [1, 1, 1, 1, 1])
        q = Query(grades_table)
        transaction.add_query(q.increment, key, 1)
    transaction_workers[i % num_threads].add_transaction(transaction)

threads = []
for transaction_worker in transaction_workers:
    threads.append(threading.Thread(target = transaction_worker.run, args = ()))

for i, thread in enumerate(threads):
    # print('Thread', i, 'started')
    thread.start()

for i, thread in enumerate(threads):
    thread.join()
    # print('Thread', i, 'finished')

num_committed_transactions = sum(t.result for t in transaction_workers)
# print(num_committed_transactions, 'transaction committed.')

query = Query(grades_table)
s = query.sum(keys[0], keys[-1], 1)

if s != num_committed_transactions * num_operations:
    print('Expected sum:', num_committed_transactions * num_operations, ', actual:', s, '. Failed.')
else:
    print('Pass.')

<<<<<<< Updated upstream
db.close()
=======
db.close()
>>>>>>> Stashed changes

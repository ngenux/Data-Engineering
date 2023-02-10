import pandas as pd
from pymongo import MongoClient, UpdateOne
import concurrent.futures
import time

df = pd.read_csv('data/bank_transactions.csv')
upsert_list = []

for i in df.index.unique().tolist()[:10]:
    upsert_list.append(
        {"CustomerID": df.iloc[i]['CustomerID'], "TransactionID": df.iloc[i]['TransactionID'], "CustGender": "Male"})
print(len(upsert_list))
print(upsert_list[0:10])

def upsert_documents(documents):
    client = MongoClient("mongodb://localhost:27017/", maxPoolSize=10, w=0)
    db = client["appdb"]
    collection = db["bank_data"]
    collection.create_index([("CustomerID", 1), ("TransactionID", 1)], unique=True)
    updates = [UpdateOne({'CustomerID': document['CustomerID'], 'TransactionID': document['TransactionID']},
                         {'$set': document}, upsert=True) for document in documents]
    result = collection.bulk_write(updates)
    return result


def process_chunk(chunk):
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        for documents in chunk:
            executor.submit(upsert_documents, [documents])


def parallel_upsert(documents, chunk_size=1000, num_threads=4):
    chunks = [documents[i:i + chunk_size] for i in range(0, len(documents), chunk_size)]
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        results = list(executor.map(process_chunk, chunks))
    return results


documents = upsert_list[0:10]
start_time = time.time()
parallel_upsert(documents, chunk_size=1000, num_threads=4)
end_time = time.time()
print("Time taken:", end_time - start_time, "seconds")
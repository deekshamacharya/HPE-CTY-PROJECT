import time
import csv
from arango import ArangoClient
client = ArangoClient()


db = client.db('_system', username='root', password='deek0912')

def insert_documents_from_csv( collection_name, csv_file_path):
    try:
        # Open the CSV file for reading
        with open(csv_file_path, 'r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            documents = list(csv_reader)
            
            # Start measuring time just before inserting
            start_time = time.time()
            
            # Insert documents in bulk into the collection
            collection = db.collection(collection_name)
            collection.insert_many(documents, sync=True)
        
        # End measuring time after insertion completes
        end_time = time.time()
        time_taken = end_time - start_time
        
        print(f"All documents from {csv_file_path} inserted successfully into ArangoDB in bulk. Time taken: {time_taken:.4f} seconds")
        return time_taken
        
    except Exception as e:
        print(f"An error occurred while inserting documents: {e}")
        return 0



# Example usage:
insert_csv_files = ['data_100_tuples.csv','data_1000_tuples.csv','data_10000_tuples.csv','data_100000_tuples.csv','data_1000000_tuples.csv']


collection_name = "mycoll"

# Insert documents from each CSV file
for csv_file in insert_csv_files:
    insert_documents_from_csv(collection_name, csv_file)

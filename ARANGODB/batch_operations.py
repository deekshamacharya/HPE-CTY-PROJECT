import csv
import time
import logging
import matplotlib.pyplot as plt

# Configure logging
logging.basicConfig(filename='arango_operations.log', level=logging.ERROR)

# Initialize ArangoDB client
from arango import ArangoClient
client = ArangoClient()

# Connect to the ArangoDB server
def connect_to_database(username, password):
    try:
        return client.db('_system', username=username, password=password)
    except Exception as e:
        logging.error(f"Error connecting to the database: {e}")
        print("An error occurred. Please check the log for details.")
        return None

# Function to insert documents in batches
def batch_insert_documents(db, collection_name, documents, batch_size=100000):
    try:
        num_batches_list = []
        insertion_time_taken_list = []
        start_time = time.time()
        for i in range(0, len(documents), batch_size):
            batch = documents[i:i+batch_size]
            num_batches = i // batch_size + 1
            num_batches_list.append(num_batches)
            batch_start_time = time.time()
            db.collection(collection_name).insert_many(batch)
            batch_end_time = time.time()
            batch_time_taken = batch_end_time - batch_start_time
            insertion_time_taken_list.append(batch_time_taken)
            print(f"Batch {num_batches} inserted into collection '{collection_name}' in {batch_time_taken:.4f} seconds")
        total_time_taken = time.time() - start_time
        print(f"Total time taken for insertion: {total_time_taken:.4f} seconds")
        plot_graph(num_batches_list, insertion_time_taken_list, 'Insertion')
    except Exception as e:
        logging.error(f"An error occurred during batch insertion: {e}")
        print("An error occurred. Please check the log for details.")

# Function to plot graph
def plot_graph(x_data, y_data, operation_name):
    plt.plot(x_data, y_data, marker='o')
    plt.xlabel('Number of Batches' if operation_name != 'Update' else 'Number of Entries')
    plt.ylabel('Time Taken (seconds)')
    plt.title(f'{operation_name} Time vs. Number of Batches' if operation_name != 'Update' else f'{operation_name} Time vs. Number of Entries')
    plt.grid(True)
    plt.show()

def batch_delete_documents_from_csv(db, collection_name, csv_file_path, batch_size=100000, max_batches=None):
    try:
        start_time = time.time()
        num_batches_list = []
        deletion_time_taken_list = []
        batch_counter = 0
        batch_document_ids = []

        with open(csv_file_path, 'r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            for row in csv_reader:
                filters = ' && '.join([f"doc.`{key}` == '{value}'" for key, value in row.items()])
                query = f"""
                    FOR doc IN {collection_name}
                    FILTER {filters}
                    RETURN doc._id
                """
                cursor = db.aql.execute(query)
                document_ids = [doc_id for doc_id in cursor]

                if document_ids:
                    batch_document_ids.extend(document_ids)

                    if len(batch_document_ids) >= batch_size:
                        batch_counter += 1
                        start_delete_time = time.time()
                        db.collection(collection_name).delete_many(batch_document_ids)
                        end_delete_time = time.time()
                        batch_time_taken = end_delete_time - start_delete_time
                        num_batches_list.append(batch_counter)
                        deletion_time_taken_list.append(batch_time_taken)
                        print(f"Batch {batch_counter} deletion completed. Time taken: {batch_time_taken:.4f} seconds")
                        batch_document_ids = []

                        if max_batches is not None and batch_counter == max_batches:
                            break

            if batch_document_ids:
                batch_counter += 1
                start_delete_time = time.time()
                db.collection(collection_name).delete_many(batch_document_ids)
                end_delete_time = time.time()
                batch_time_taken = end_delete_time - start_delete_time
                num_batches_list.append(batch_counter)
                deletion_time_taken_list.append(batch_time_taken)
                print(f"Batch {batch_counter} deletion completed. Time taken: {batch_time_taken:.4f} seconds")

        total_time_taken = sum(deletion_time_taken_list)
        print(f"Total time taken to delete {batch_counter} batches: {total_time_taken:.4f} seconds")

    except Exception as e:
        print(f"An error occurred during batch deletion: {e}")



def update_documents(db, collection_name, attributes):
    try:
        query = f"""
            FOR doc IN {collection_name}
            FILTER doc.`Source IP` == "{attributes['Source IP']}"
            UPDATE doc WITH {{ 'Source Port': '0' }} IN {collection_name}
            RETURN NEW
        """
        cursor = db.aql.execute(query)
        updated_documents = list(cursor)
        updated_count = len(updated_documents)
        if updated_count == 0:
            print("No documents found for update.")
        return updated_count
    except Exception as e:
        logging.error(f"An error occurred while updating documents in ArangoDB: {e}")
        print("An error occurred. Please check the log for details.")
        return 0

def update_documents_from_csv(db, collection_name, csv_file_path, batch_size=10000):
    try:
        total_entries_processed = 0
        time_taken_list = []
        updated_count_batch = 0
        
        with open(csv_file_path, 'r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            start_time = time.time()  # Start overall update time measurement
            for row in csv_reader:
                updated_count = update_documents(db, collection_name, row)
                updated_count_batch += updated_count
                total_entries_processed += updated_count
                if total_entries_processed % batch_size == 0:
                    end_time = time.time()
                    batch_time_taken = end_time - start_time
                    if updated_count_batch > 0:
                        print(f"Time taken to update batch {total_entries_processed // batch_size}: {batch_time_taken:.4f} seconds")
                        time_taken_list.append(batch_time_taken)
                    updated_count_batch = 0  # Reset the count for the next batch
                    start_time = time.time()  # Start time for the next batch
            
            # Check if there are remaining entries to update
            if total_entries_processed % batch_size != 0:
                end_time = time.time()
                batch_time_taken = end_time - start_time
                if updated_count_batch > 0:
                    print(f"Time taken to update batch {total_entries_processed // batch_size + 1}: {batch_time_taken:.4f} seconds")
                    time_taken_list.append(batch_time_taken)
        
        total_time_taken = sum(time_taken_list)  # Total time taken for updates
        print(f"Total time taken to update {total_entries_processed} entries: {total_time_taken:.4f} seconds")
    
    except Exception as e:
        logging.error(f"An error occurred while updating documents: {e}")
        print("An error occurred. Please check the log for details.")
        return 0, []


# Main function
if __name__ == "__main__":
    # Define the collection name
    collection_name = 'mycoll'

    # Define the CSV file paths
    insertion_csv_file_path = 'data_1000000_tuples.csv'
    update_delete_csv_file_path = 'data_1000000_tuples.csv'

    # Define the batch size
    batch_size = 100000

    # Connect to the ArangoDB server
    db = connect_to_database('root', 'deek0912')
    if db:
        # Menu for selecting operation
        print("Select operation:")
        print("1. Insert from data.csv")
        print("2. Delete from shuffleddata.csv")
        print("3. Update from shuffleddata.csv")

        operation_choice = int(input("Enter your choice: "))

# Perform operation based on the user choice
if operation_choice == 1:
    # Read documents from the CSV file for insertion
    documents = []
    with open(insertion_csv_file_path, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            documents.append(row)
    # Batch insert documents into the collection
    batch_insert_documents(db, collection_name, documents, batch_size)
elif operation_choice == 2:
    # Batch delete documents from the collection based on shuffleddata.csv
    batch_delete_documents_from_csv(db, collection_name, update_delete_csv_file_path, batch_size)
elif operation_choice == 3:
    # Batch update documents in the collection based on shuffleddata.csv
    update_documents_from_csv(db, collection_name, update_delete_csv_file_path, batch_size)
else:
    print("Invalid choice. Please select a valid operation.")
    update_documents_from_csv(db, collection_name, update_delete_csv_file_path, batch_size)

import time
import matplotlib.pyplot as plt
import csv
from arango import ArangoClient
client = ArangoClient()



# Connect to the ArangoDB server
db = client.db('_system', username='root', password='deek0912')


# Function to insert documents from a CSV file into a collection and return the time taken
# Function to insert documents from a CSV file into a collection and return the time taken
def insert_documents_from_csv(collection_name, csv_file_path):
    try:
        # Open the CSV file for reading
        with open(csv_file_path, 'r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            
            # Start measuring time just before inserting
            start_time = time.time()
            
            # Insert documents one by one into the collection
            collection = db.collection(collection_name)
            for row in csv_reader:
                collection.insert(row, sync=True)
        
        # End measuring time after insertion completes
        end_time = time.time()
        time_taken = end_time - start_time
        
        print(f"All documents from {csv_file_path} inserted successfully into ArangoDB one by one. Time taken: {time_taken:.4f} seconds")
        return time_taken
        
    except Exception as e:
        print(f"An error occurred while inserting documents: {e}")
        return 0


# Function to delete documents from the collection based on the attributes in a CSV row
def delete_documents(collection_name, attributes):
    try:
        # Construct the AQL query
        query = f"FOR doc IN {collection_name} FILTER "
        conditions = []
        for key, value in attributes.items():
            # Enclose attribute names containing spaces in backticks
            key = f"`{key}`"
            # Enclose string attribute values in double quotes
            if isinstance(value, str):
                value = f'"{value}"'
            conditions.append(f"doc.{key} == {value}")
        query += " AND ".join(conditions)
        query += f" REMOVE doc IN {collection_name}"

        # Delete documents from the collection based on the query
        cursor = db.aql.execute(query)
        deleted_count = len(list(cursor))

        return deleted_count
    except Exception as e:
        print(f"An error occurred while deleting documents: {e}")
        return 0

# Function to delete documents from the collection based on the number of entries in a CSV file
def delete_documents_from_csv(collection_name, csv_file_paths):
    try:
        deletion_time_taken_list = []
        for csv_file_path in csv_file_paths:
            # Count the number of entries in the CSV file
            num_entries = sum(1 for line in open(csv_file_path))

            # Open the CSV file for reading
            with open(csv_file_path, 'r') as csv_file:
                csv_reader = csv.DictReader(csv_file)
                # Initialize time variables
                start_time = time.time()
                # Iterate over each row in the CSV file
                for row in csv_reader:
                    # Delete documents based on the attributes in the row
                    delete_documents(collection_name, row)
                end_time = time.time()
                time_taken = end_time - start_time
                deletion_time_taken_list.append(time_taken)
                print(f"Time taken to delete {num_entries} documents from CSV file '{csv_file_path}': {time_taken:.4f} seconds")
        return deletion_time_taken_list
    except Exception as e:
        print(f"An error occurred while deleting documents: {e}")
        return []

# Function to update documents in the collection based on the attributes in a CSV row
def update_documents(collection_name, attributes):
    try:
        # Construct the AQL query to update documents with the matching attributes
        query = f"""
            FOR doc IN {collection_name}
            FILTER doc.`Source IP` == "{attributes['Source IP']}"
            UPDATE doc WITH {{ 'Source IP': '100' }} IN {collection_name}
        """

        # Execute the AQL query
        cursor = db.aql.execute(query)
        updated_count = len(list(cursor))

        return updated_count
    except Exception as e:
        print(f"An error occurred while updating documents in ArangoDB: {e}")
        return 0

# Function to update documents in the collection from a CSV file
def update_documents_from_csv(collection_name, csv_file_path):
    try:
        update_time_taken_list = []
        # Count the number of entries in the CSV file
        num_entries = sum(1 for line in open(csv_file_path))

        # Open the CSV file for reading
        with open(csv_file_path, 'r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            # Initialize time variables
            start_time = time.time()
            # Iterate over each row in the CSV file
            for row in csv_reader:
                # Update documents based on the attributes in the row in ArangoDB
                update_documents(collection_name, row)
            end_time = time.time()
            time_taken = end_time - start_time
            update_time_taken_list.append(time_taken)
            print(f"Time taken to update {num_entries} documents from CSV file '{csv_file_path}': {time_taken:.4f} seconds")
        return update_time_taken_list
    except Exception as e:
        print(f"An error occurred while updating documents: {e}")
        return []

# Function to plot graph for time taken vs. number of entries
def plot_graph(num_entries_list, time_taken_list, operation_name):
    plt.plot(num_entries_list, time_taken_list, marker='o')
    plt.xlabel('Number of Entries in CSV File')
    plt.ylabel('Time Taken (seconds)')
    plt.title(f'Time Taken for {operation_name}')
    plt.grid(True)
    if operation_name == 'Insertion':
        plt.xticks(num_entries_list, ['100', '1000', '10000', '100000', '1000000'])
    else:
        plt.xticks(num_entries_list[:4], ['100', '1000', '10000', '100000'])
    plt.savefig(f'{operation_name}_graph.png')
    plt.show()

# Main function
if __name__ == "__main__":
    # Define the collection name
    collection_name = 'mycoll'
    
    # Define the CSV file paths for insertion, deletion, and update
    insertion_csv_file_paths = ['data_100_tuples.csv','data_1000_tuples.csv','data_10000_tuples.csv','data_100000_tuples.csv','data_1000000_tuples.csv']
    deletion_csv_file_paths = ['data_100_tuples.csv','data_1000_tuples.csv','data_10000_tuples.csv','data_100000_tuples.csv','data_1000000_tuples.csv']
    update_csv_file_paths = ['data_100_tuples.csv','data_1000_tuples.csv','data_10000_tuples.csv','data_100000_tuples.csv','data_1000000_tuples.csv']

    # Lists to store time taken and number of entries for each operation
    num_entries_list = ['100', '1000', '10000', '100000','1000000']

    # Ask the user for the operation choice
    print("Choose the operation:\n1. Insert\n2. Delete\n3. Update\n")
    operation_choice = int(input("Enter your choice: "))

    # Switch case for different operations
    if operation_choice == 1:  # Insert
        insertion_time_taken_list = []
        for csv_file_path in insertion_csv_file_paths:
            time_taken = insert_documents_from_csv(collection_name, csv_file_path)
            insertion_time_taken_list.append(time_taken)
        plot_graph(num_entries_list, insertion_time_taken_list, 'Insertion')
    elif operation_choice == 2:  # Delete
        deletion_time_taken_list = delete_documents_from_csv(collection_name, deletion_csv_file_paths)
        plot_graph(num_entries_list[:4], deletion_time_taken_list, 'Deletion')
    elif operation_choice == 3:  # Update
        update_time_taken_list = []
        for csv_file_path in update_csv_file_paths:
            time_taken = update_documents_from_csv(collection_name, csv_file_path)
            update_time_taken_list.append(time_taken)
        plot_graph(num_entries_list[:4], update_time_taken_list, 'Update')
    else:
        print("Invalid choice")

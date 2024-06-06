from arango import ArangoClient

# Initialize ArangoDB client
client = ArangoClient()

# Connect to the ArangoDB server
db = client.db('_system', username='root', password='deek0912')

def search_by_source_and_destination_ports(collection_name, source_port, destination_port):
    try:
        print("Source port:", source_port)  # Debugging output
        print("Destination port:", destination_port)  # Debugging output
        
        # Construct the AQL query
        query = f"""
            FOR doc IN {collection_name}
            FILTER doc.`Source Port` == @source_port && doc.`Destination Port` == @destination_port
            RETURN doc
        """
        
        print("AQL Query:", query)  # Debugging output
        
        # Execute the AQL query with the provided source and destination ports
        cursor = db.aql.execute(query, bind_vars={'source_port': source_port, 'destination_port': destination_port})
        
        # Retrieve the matching documents
        matching_documents = [doc for doc in cursor]

        # Print the count of matching documents
        print(f"Number of matching documents: {len(matching_documents)}")
        
        # Return the matching documents
        return matching_documents

    except Exception as e:
        print(f"An error occurred during search: {e}")
        return []

# Main function
if __name__ == "__main__":
    # Define the collection name
    collection_name = 'mycoll'
    
    # Define the source port and destination port to search
    source_port = '100'  # Source port value to search
    destination_port = '13504'  # Destination port value to search
    
    # Perform the search by source and destination ports
    matching_documents = search_by_source_and_destination_ports(collection_name, source_port, destination_port)
    
    # Print the matching documents
    for doc in matching_documents:
        print(doc)  # Or process/display the document as needed

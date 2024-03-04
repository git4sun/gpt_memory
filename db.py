import sqlite3
from datetime import datetime
import pickle  # For serialization and deserialization

class DB:
    def __init__(self, db_path='memory.db'):
        self.db_path = db_path
        self.conn = self.connect_db()
        self.setup_database()

    def connect_db(self):
        """Establish a connection to the SQLite database."""
        conn = sqlite3.connect(self.db_path)
        return conn

    def setup_database(self):
        """Create the 'mem' table if it doesn't already exist."""
        query = '''CREATE TABLE IF NOT EXISTS mem
                   (id INTEGER PRIMARY KEY, text TEXT, role TEXT, ts TEXT, categories TEXT, labels TEXT,
                   embedding BLOB, continued INTEGER, level1 TEXT, level2 TEXT, level3 TEXT, user_id TEXT)'''
        cur = self.conn.cursor()
        cur.execute(query)
        self.conn.commit()
        query = '''CREATE TABLE IF NOT EXISTS log
                   (id INTEGER PRIMARY KEY, ts TEXT, decay_weights Blob, feedback Text, inputs Blob, outputs blob)'''
        cur = self.conn.cursor()
        cur.execute(query)
        self.conn.commit()

    def insert_mem(self, data):
        """Insert a new record into the 'mem' table."""
        # Serialize the embedding before inserting
        if 'embedding' in data and data['embedding'] is not None:
            data['embedding'] = pickle.dumps(data['embedding'])
        cur = self.conn.cursor()
        cur.execute('''INSERT INTO mem (text, role, ts, categories, labels, embedding, continued, level1, level2, level3, user_id)
                       VALUES (:text, :role, :ts, :categories, :labels, :embedding, :continued, :level1, :level2, :level3, :user_id)''', data)
        self.conn.commit()
        return cur.lastrowid

    def read_mems(self, user_id=0, limit=100):
        """Retrieve message history for a given user, returned as a dictionary keyed by message ID."""
        cur = self.conn.cursor()
        query = "SELECT * FROM mem WHERE user_id = ? ORDER BY ts DESC LIMIT ?"
        cur.execute(query, (user_id, limit))
        rows = cur.fetchall()

        # Initialize an empty dictionary to store the message data, keyed by message ID
        messages_dict = {}

        # Iterate through each row, deserializing the embedding and storing the data in the dictionary
        for row in rows:
            message_id = row[0]
            # Check and deserialize the embedding if not None
            if row[6] is not None:
                embedding = pickle.loads(row[6])
            else:
                embedding = None

            # Create a dictionary for the current row, replacing the embedding with its deserialized version
            row_dict = {
                'id': row[0],
                'text': row[1],
                'role': row[2],
                'ts': row[3],
                'categories': row[4],
                'labels': row[5],
                'embedding': embedding,  # Use the deserialized embedding
                'continued': row[7],
                'level1': row[8],
                'level2': row[9],
                'level3': row[10],
                'user_id': row[11]
            }

            # Add the current row's data to the messages_dict, keyed by message ID
            messages_dict[message_id] = row_dict

        return messages_dict
    
    def show_mems(self, user_id=0, limit=100):
        """Retrieve message history for a given user."""
        cur = self.conn.cursor()
        query = "SELECT id, text, role, ts, categories, labels, continued, level1, level2, level3, user_id FROM mem WHERE user_id = ? ORDER BY ts DESC LIMIT ?"
        cur.execute(query, (user_id, limit))
        rows = cur.fetchall()
        # Deserialize the embedding for each row
        return rows
        
    def delete_mem_by_ids(self, ids):
        """Delete records from the 'mem' table given a list of IDs.

        Parameters:
        - ids: List of integers representing the IDs of the records to delete.
        """
        # Convert the list of IDs into a tuple, which is required for the SQL query placeholder format.
        ids_tuple = tuple(ids)
        
        # Prepare the SQL DELETE statement. Using `?` placeholders for parameters.
        # The number of placeholders must match the number of IDs we want to delete.
        # This is why we format the placeholders string based on the length of the ids list.
        query = f"DELETE FROM mem WHERE id IN ({','.join('?' * len(ids))})"
        
        cur = self.conn.cursor()
        cur.execute(query, ids_tuple)
        self.conn.commit()

        print(f"Deleted {cur.rowcount} records from the 'mem' table.")
    def insert_log(self, ts, decay_weights, feedback, inputs, outputs):
        """Insert a new log record into the 'log' table."""
        cur = self.conn.cursor()
        # Serialize complex objects to BLOB before insertion
        serialized_decay_weights = pickle.dumps(decay_weights)
        serialized_inputs = pickle.dumps(inputs)
        serialized_outputs = pickle.dumps(outputs)
        
        query = '''INSERT INTO log (ts, decay_weights, feedback, inputs, outputs)
                   VALUES (?, ?, ?, ?, ?)'''
        cur.execute(query, (ts, serialized_decay_weights, feedback, serialized_inputs, serialized_outputs))
        self.conn.commit()

    def update_log(self, log_id, **kwargs):
        """Update information for a given log entry in the 'log' table."""
        cur = self.conn.cursor()
        
        # Serialize complex objects to BLOB if present in kwargs
        if 'decay_weights' in kwargs:
            kwargs['decay_weights'] = pickle.dumps(kwargs['decay_weights'])
        if 'inputs' in kwargs:
            kwargs['inputs'] = pickle.dumps(kwargs['inputs'])
        if 'outputs' in kwargs:
            kwargs['outputs'] = pickle.dumps(kwargs['outputs'])
        
        # Construct the SET part of the SQL update query dynamically based on kwargs
        set_parts = [f"{key} = ?" for key in kwargs.keys()]
        set_clause = ", ".join(set_parts)
        
        query = f"UPDATE log SET {set_clause} WHERE id = ?"
        cur.execute(query, list(kwargs.values()) + [log_id])
        self.conn.commit()

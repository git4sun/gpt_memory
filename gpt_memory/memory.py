from datetime import datetime
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from openai import OpenAI
from rank_bm25 import BM25Okapi
from sklearn.feature_extraction.text import TfidfVectorizer
from .db import DB
from .gpt import GPT
import re
from rank_bm25 import BM25Okapi
import tiktoken as tt
import json

class Memory:
    def __init__(self, model='gpt-4o-mini', conf='db_config.json'):
        self.db = DB() 
        
        try:
            with open(conf) as config_file:
                config = json.load(config_file)
            db_url = config.get('db_url', 'sqlite:///memory.db')
        except FileNotFoundError:
            db_url = 'sqlite:///memory.db'

        self.db = DB(db_url)
        self.model = model
        self.gpt = GPT(model=self.model)
        self.enc = tt.get_encoding('o200k_base')

    def label_followup_messages(self, history, current_message_text):
        continuation_scores = {}
        for message_id, message_details in history.items():
            previous_text = message_details['text']
            
            prompt = f"Given the previous message: '{previous_text}', how likely is the following message a continuation? '{current_message_text}' Please provide a score from 0 to 1, where 1 is very likely and 0 is not likely. Give a direct answer with a float value."
            
            continuation_score = self.get_continuation_score(prompt)
            
            continuation_scores[message_id] = continuation_score

        sorted_scores = sorted(continuation_scores.items(), key=lambda item: item[1], reverse=True)
        return sorted_scores

    def get_continuation_score(self, prompt):
        """
        Ask GPT to determine the likelihood of the current message being a followup to the previous one.
        TODO: change to LangChain and pydantic parsing output results
        """
        try:
            response = self.gpt.query(prompt)
            # Parse the response to extract the score
            # This parsing depends on the format of GPT's response. Adjust the parsing logic accordingly.
            score_text = response.choices[0].message.content.strip().lower()
            # print('gpt output > ', score_text)
            match = re.search(r"([-+]?\d*\.\d+|\d+)", score_text)
            score = float(match.group(1)) if match else 0.0
            # Convert score text to float, ensuring it falls within 0 to 1
            return score
        except Exception as e:
            print(f"Error calling OpenAI API: {e}")
            return 0  # Consider a default score or error handling

    def get_message_embedding(self, message):
        """
        Request GPT embedding API to get embeddings for the input message.
        """
        try:
            return self.gpt.embeddings(message)
        except Exception as e:
            print(f"Error calling OpenAI Embedding API: {e}")
            return None  # Consider how you want to handle errors gracefully

    def calculate_embedding_distance(self, current_embedding, history):
        embedding_distances = {}
        for message_id, message_details in history.items():
            h_embedding = message_details['embedding']
            
            if h_embedding is not None:
                # Convert embeddings to numpy arrays and ensure they are 2D
                emb1 = np.array(current_embedding).reshape(1, -1)
                emb2 = np.array(h_embedding).reshape(1, -1)
                # Calculate dot product
                dot_product = np.dot(emb1, emb2.T)[0][0]
                # Calculate magnitudes
                emb1_norm = np.linalg.norm(emb1)
                emb2_norm = np.linalg.norm(emb2)
                # Compute cosine similarity
                distance = dot_product / (emb1_norm * emb2_norm)
                embedding_distances[message_id] = distance
        
        return embedding_distances

    def calculate_bm25_and_sort(self, history, current_message_text):
        historical_texts = [details['text'] for details in history.values()]
        historical_ids = list(history.keys())

        tokenized_corpus = [self.enc.encode(doc) for doc in historical_texts]
        bm25 = BM25Okapi(tokenized_corpus)
        query_tokens = self.enc.encode(current_message_text)
        bm25_scores = bm25.get_scores(query_tokens)
        
        id_score_pairs = dict(zip(historical_ids, bm25_scores))
        return id_score_pairs

    def prepare_context(self, history, followups, embedding_distances, bm25_scores):
        """
        Prepare context by calculating a final score for each message in history based on
        time lapse, relevance decay, and embedding weighting.
        """
        final_scores = {}

        # Current time for time lapse calculation
        now = datetime.now()
        t_lambda = 0.01
        for message_id, message_details in history.items():
            # print(message_id, message_details['ts'])
            # 1. Time lapse to now and calculate exponential decay factor as function of recency
            message_time = message_details['ts']
            time_lapse = (now - message_time).total_seconds()/3600   # Seconds
            time_decay = np.exp(-time_lapse*t_lambda)

            # 2. Relevance decay factor based on BM25 scores
            bm25_score = bm25_scores[message_id]
            relevance_decay = 1/np.exp(-bm25_score)

            # 3. Embedding weighting factor
            embedding_distance = embedding_distances[message_id]
            # embedding_weight = 1 - embedding_distance  # Assuming distance is [0, 1], convert to similarity

            # 4. Combine the above 3 to get a final score
            final_score = time_decay * relevance_decay * embedding_distance
            abstraction_levels = 1 if final_score > 0.66 else 2 if final_score > 0.33 else 3

            final_scores[message_id] = [final_score, abstraction_levels]
            # print(message_id, time_lapse, time_decay, relevance_decay, embedding_distance, final_score)

        # Display debugging information
        # print(f"{'MID':<5} {'cont':<5} {'followup':<8} {'embedding':<10} {'bm25':<5} {'abs':<4} {'score':<5} {'Text':<20}")
        inputs = []
        for mid in history:
            cont = history[mid]['continued']
            followup = followups[mid] if mid in followups else 'no'
            embedding = round(embedding_distances[mid], 4) if mid in embedding_distances else 0
            bm25 = round(bm25_scores[mid], 4) if mid in bm25_scores else 0
            abs_level = final_scores[mid][1] if mid in final_scores else 0
            score = round(final_scores[mid][0], 4) if mid in final_scores else 0
            text = history[mid]['text'].replace('\n', ' ')
            # print(f"{mid:<5} {cont:<5} {followup:<8} {embedding:<10} {bm25:<5} {abs_level:<4} {score:<5} {text:<20}")
            inputs.append([mid,cont,followup,embedding,bm25,abs_level,score])

        ts = datetime.now()
        decay_weights = {'time_decay': t_lambda}  # Example structure
        feedback = ''
        self.db.insert_log(ts, decay_weights, feedback, inputs, final_scores)
        
        return final_scores

    def check_and_generate_abstraction(self, message_id, level):
        """
        Check if the abstraction level text exists for the given message ID. If not,
        generate an abstraction using GPT and store the response in the database.

        Parameters:
        - message_id: ID of the message to check and generate abstraction for.
        - level: abstract level
        """
        n_tokens = (4-level) * 50
        # First, check if the abstraction text already exists
        result = self.db.get_abstract(level, message_id)

        if result and result[0]:
            # print(f"Abstraction already exists for message ID {message_id}.")
            return result[0]
        else:
            # If abstraction doesn't exist, generate it using GPT
            message_text = self.db.get_message_text_by_id(message_id)
            abstraction_text = self.generate_abstraction_with_gpt(message_text, n_tokens)

            self.db.update_abstract(message_id, abstraction_text, level)

            # print(f"Generated and stored abstraction for message ID {message_id}.")
            return abstraction_text
    
    def generate_abstraction_with_gpt(self, text, n_tokens):
        """
        Generate an abstraction of the given text using GPT within a specified token limit.

        Parameters:
        - text: The text to abstract.
        - n_tokens: The maximum number of tokens for the abstraction.
        """
        # Placeholder for GPT call
        # You would use openai.Completion.create() or similar here
        prompt = (
            f"Please create an abstract for the following text within {n_tokens} tokens:\n\n"
            f"\"{text}\"\n\n"
            "Abstract:"
        )
        return self.gpt.gpt_text(prompt)
    
    def prepare_prompt(self, history, scores):
        """
        Prepare a prompt for GPT using the history and scores, incorporating the abstraction level for each message.
        """
        prompt_lines = []

        for message_id, details in scores.items():
            # Retrieve message details from history
            context = history[message_id]['text']
            if len(context.split(' ')) > 5:
                context = self.check_and_generate_abstraction(message_id, details[1])
            # Format the message with its abstraction level and text
            prompt_lines.append(context)

        # Combine all lines into a single prompt string, separating messages with newlines
        prompt = "\n".join(prompt_lines) + "\n"

        return prompt
    
    def relevance_module(self, message_data, limit=100):
        history = self.db.read_mems(message_data['user_id'], limit=limit)

        if len(history) < 1:
            prompt = ''
        else:
            recent_keys = sorted(history.keys(), reverse=True)
            recent_history = {key: history[key] for key in recent_keys}
            followups = self.label_followup_messages(recent_history, message_data['text']) 
            if followups[0][1] > 0.8: message_data['continued'] = followups[0][0]
            # Step 2.2: Calculate embedding similarity score
            top_100_embeddings = self.calculate_embedding_distance(message_data['embedding'], history)

            # Step 2.3: Calculate text relevance score using BM25
            top_100_bm25 = self.calculate_bm25_and_sort(history, message_data['text'])
            scores = self.prepare_context(history, {f[0]:f[1] for f in followups}, top_100_embeddings, top_100_bm25)
            prompt = self.prepare_prompt(history, scores)
        return prompt
    
    def process_message(self, message:str, user_id:int=0)->tuple[int, str]:
        """
        Processes the input message and generates a response based on historical interactions.
        """

        current_ts = datetime.now()  # Format timestamp
        current_embedding = self.get_message_embedding(message)
        message_data = {
            'text': message,
            'role': 'user',  # Assuming role is 'user' for incoming messages
            'ts': current_ts,
            'categories': '',  # Adjust according to your schema
            'labels': '',  # Adjust according to your schema
            'embedding': current_embedding,  # Placeholder, adjust as needed
            'continued': 0,  # Placeholder, adjust as needed
            'level1': '',  # Adjust according to your schema
            'level2': '',  # Adjust according to your schema
            'level3': '',  # Adjust according to your schema
            'user_id': str(user_id)  # Ensure user_id is a string if your schema expects it
        }

        response = ''
        # Step 2: Call Relevance module to get the most relevant messages
        prompt = self.relevance_module(message_data)
        # If history is empty, directly interact with GPT ChatCompletion
        if len(prompt) < 1:
            # Extract and return the GPT-generated response
            response = self.gpt.gpt_text(message)
        else:
            response = self.gpt.gpt_text(prompt + message)
            # Step 3: Record all the data to the database
            # Step 4: get Feedback from the user
            # Step 5: Placeholder for getting abstraction level of each history message
            # Assuming the function is called get_abstraction_level, which is not implemented here
            # abstraction_levels = [get_abstraction_level(msg, followups, embedding_distances, bm25_scores) for msg in history]
                
        new_mid = self.db.insert_mem(message_data) 
        current_ts = datetime.now()
        response_data =  {
            'text': response,
            'role': 'ai',  # Assuming role is 'user' for incoming messages
            'ts': current_ts,
            'categories': '',  # Adjust according to your schema
            'labels': '',  # Adjust according to your schema
            'embedding': current_embedding,  # Placeholder, adjust as needed
            'continued': new_mid,  # Placeholder, adjust as needed
            'level1': '',  # Adjust according to your schema
            'level2': '',  # Adjust according to your schema
            'level3': '',  # Adjust according to your schema
            'user_id': str(user_id)  # Ensure user_id is a string if your schema expects it
        }
        self.db.insert_mem(response_data) 
        status = 2 if "feedback" in response else 1  # Normal response
        return (status, response)
    
    def record_feedback(self, feedback):
        # Logic to record the feedback in the database
        feedback_data = {
            'text': feedback,
            # Populate other fields as necessary
        }
        self.db.insert_mem(feedback_data)  # Assuming insert_mem can be used for feedback
        print("Feedback recorded. Thank you!")


    def show_mem(self):
        h = self.db.read_mems()
        for mid in h:
            if 'embedding' in h[mid]:
                del h[mid]['embedding']
        return h
    
    def delete_mem(self, ids):
        self.db.delete_mem_by_ids(ids)
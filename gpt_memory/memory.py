from datetime import datetime
import numpy as np
from .db import DB
from .gpt import GPT
import tiktoken as tt
import json
from sentence_transformers import SentenceTransformer

class Memory:
    def __init__(self, model='deepseek-chat', conf='db_config.json', base_url='https://api.deepseek.com'):
        self.db = DB() 
        self.embd = SentenceTransformer('intfloat/multilingual-e5-large-instruct')
        try:
            with open(conf) as config_file:
                config = json.load(config_file)
            db_url = config.get('db_url', 'sqlite:///memory.db')
        except FileNotFoundError:
            db_url = 'sqlite:///memory.db'

        self.db = DB(db_url)
        self.gpt = GPT(model=model, base_url=base_url)
        self.enc = tt.get_encoding('o200k_base')
        self.sim_threshold = 0.86
        self.same_threshold = 0.98

    def get_time_probs(self, history:dict)->dict[int, float]:
        current_time = datetime.now()
        time_diffs = {}
        for msg_id, msg_details in history.items():
            time_diff = (current_time - msg_details['ts']).total_seconds()
            time_diffs[msg_id] = time_diff
        # Convert time differences to probabilities
        one_minute = 60
        one_day = 86400  # 24 * 60 * 60 seconds
        probs = {}
        
        for msg_id, time_diff in time_diffs.items():
            if time_diff <= one_minute:
                # Within 1 minute, probability is 1
                probs[msg_id] = 1.0
            elif time_diff > one_day:
                # After 1 day, very low but slowly decaying probability
                days_passed = time_diff / one_day
                probs[msg_id] = max(0.1, 0.2 * np.exp(-0.1 * (days_passed - 1)))
            else:
                # Between 1 minute and 1 day, exponential decay
                normalized_time = (time_diff - one_minute) / (one_day - one_minute)
                probs[msg_id] = np.exp(-5 * normalized_time)
        return probs

    def get_sim_probs(self, sim_scores:dict, history:dict)->dict[int, float]:
        ### TODO: use similarity scores to get probabilities, the calculation should be a ML module
        ### currently just use similarity scores as probabilities
        return sim_scores  
    
    def label_followup_messages(self, history:dict[int, dict], current_message:dict, similarity_scores:dict[int, float], debug=False)->list[tuple]:
        # history: dict(message_id, message_object as dict)
        # current_message: message object for current msg
        # similarity_score: for each historical msg id, similarity to current msg 
        # return: sorted list of items with (msg id:int , continuation score: float)
        continuation_scores = {}

        # Calculate time differences in seconds for each message
        time_probs = self.get_time_probs(history)
        sim_probs = self.get_sim_probs(similarity_scores, history)     

        # Multiply time probabilities and similarity probabilities for each message ID
        overall_probs = [(msg_id, time_probs[msg_id] * sim_probs[msg_id]) for msg_id in similarity_scores]
        # Sort message IDs by overall probability score from highest to lowest
        sorted_scores = sorted(overall_probs, key=lambda x: x[1], reverse=True)
        filtered_scores = [(msg_id, score) for msg_id, score in sorted_scores if score > self.sim_threshold][:3]
        if debug: print(f'[label_followup_messages] message ids for continuation test: {filtered_scores}')
        for message_id, score in filtered_scores:
            message_details = history[message_id]
            continuation_score = self.get_continuation_score(message_details['text'], current_message['text'])
            continuation_scores[message_id] = continuation_score
            if debug: print(f'[label_followup_messages] message {message_id} continuation from LLM: {continuation_score}')

        sorted_scores = sorted(continuation_scores.items(), key=lambda item: item[1], reverse=True)
        return sorted_scores

    def get_continuation_score(self, previous_text:str, current_message_text:str)->float:
        """
        Ask GPT to determine the likelihood of the current message being a followup to the previous one.
        """
        try:
            response = self.gpt.followup(previous_text, current_message_text)
            return response['score']
        except Exception as e:
            print(f"Error calling LLM API: {e}")
            return 0  # Consider a default score or error handling

    def get_message_embedding(self, message:str)->list[float]:
        try:
            tokens = message.split(' ')
            return self.embd.encode(' '.join(tokens[:480]))
        except Exception as e:
            print(f"Error calling Embedding API: {e}")
            return []

    def calculate_embedding_similarity(self, current_embedding:list[float], history:dict[int, dict])->dict[int, float]:
        embedding_similarity = {}
        for message_id, message_details in history.items():
            h_embedding = message_details['embedding']
            
            if h_embedding is not None:
                similarity = np.dot(current_embedding, h_embedding)
                if similarity > self.sim_threshold:
                    embedding_similarity[message_id] = similarity
        
        return embedding_similarity

    def prepare_context(self, history:dict, followups:dict, embedding_similarity:dict[int, list[float]], debug:bool=False)->dict:
        """
        Prepare context by calculating a final score for each message in history based on
        time lapse, relevance decay, and embedding weighting.
        """
        final_scores = {}

        # Current time for time lapse calculation
        now = datetime.now()
        t_lambda = 6
        for message_id in embedding_similarity:
            message_details = history[message_id]
            # 1. Time lapse to now and calculate exponential decay factor as function of recency
            message_time = message_details['ts']
            time_lapse = (now - message_time).total_seconds()/60   # Seconds
            time_decay = 1/4*np.exp(-time_lapse*6)+0.75 if message_id not in followups else followups[message_id]

            # 3. Embedding weighting factor
            embedding_weight = embedding_similarity[message_id]
            # embedding_weight = 1 - embedding_similarity  # Assuming similarity is [0, 1], convert to similarity

            # 4. Combine the above 3 to get a final score
            final_score = time_decay * embedding_weight
            if debug: print(f'[prepare_context] relevance score {message_id}: {time_decay} X {embedding_weight} = {final_score}')
            abstraction_levels = 1 if final_score > 0.66 else 2 if final_score > 0.33 else 3
            final_scores[message_id] = [final_score, abstraction_levels]
            # print(message_id, time_lapse, time_decay, relevance_decay, embedding_similarity, final_score)
        if debug: 
            print(f'---------------------[prepare_context]---------------------')
            print(f"{'MID':<5} {'cont':<5} {'followup':<8} {'embedding':<10} {'abs':<4} {'score':<5} {'Text':<20}")
        inputs = []
        for mid in history:
            cont = history[mid]['continued']
            followup = followups[mid] if mid in followups else -1
            embedding = round(embedding_similarity[mid], 4) if mid in embedding_similarity else 0
            abs_level = final_scores[mid][1] if mid in final_scores else 0
            score = round(final_scores[mid][0], 4) if mid in final_scores else 0
            text = history[mid]['text'].replace('\n', ' ')
            if debug: print(f"{mid:<5} {cont:<5} {followup:<8} {embedding:<10} {abs_level:<4} {score:<5} {text:<20}")
            inputs.append([mid,cont,followup,embedding,abs_level,score])
        
        if debug: 
            print(f'---------------------[end prepare_context]---------------------')

        ts = datetime.now()
        decay_weights = {'time_decay': t_lambda}  # Example structure
        feedback = ''
        lid = self.db.insert_log(ts, decay_weights, feedback, inputs, final_scores)
        if debug: print(f'[prepare_context] insert into log {lid}')
        return final_scores

    def check_and_generate_abstraction(self, message:dict, level:int, min_tokens=5):
        """
        Check if the abstraction level text exists for the given message ID. If not,
        generate an abstraction using GPT and store the response in the database.
        """
        n_tokens = int((4-level) * 0.25 * len(message['text'].split(' ')))
        if n_tokens <= min_tokens: return message['text'] 

        abstract = message[f'level{level}']
        if len(abstract) > 2:
            return abstract
        else:
            prompt = (
                f"Generate an abstract for the following text within {n_tokens} tokens:\n"
                f"\"{message['text']}\"\n\n"
                "Abstract:"
            )
            abstraction_text = self.gpt.gpt_text(prompt)
            self.db.update_abstract(message['id'], abstraction_text, level)

            return abstraction_text
    
    def prepare_prompt(self, history:dict[int, dict], scores:dict[int, tuple[float, int]], followups:dict, debug=False)->str:
        """
        Prepare a prompt for GPT using the history and scores, incorporating the abstraction level for each message.
        """
        prompt_lines = []
        for message_id, details in scores.items():
            context = history[message_id]['text']
            if len(context.split(' ')) > 10:
                context = self.check_and_generate_abstraction(history[message_id], details[1])
            if debug: print(f'[prepare_prompt] Msg[{message_id}] Abstract level{details[1]}: {context}')
            prompt_lines.append(f'{history[message_id]["role"]} response: "'+context+'" .\n')

        for message_id, score in followups.items():
            context = history[message_id]['text']
            prompt_lines.append(f'{history[message_id]["role"]} response: "'+context+'" .\n')

        prompt = " ".join(prompt_lines)
        return prompt
    
    def eval_mood(self, message_data:dict, history:dict[int, dict], followups:list[tuple]):
        one_minute = 60
        candidates = []
        current_time = datetime.now()
        time_diffs = {}
        for msg_id, msg_details in history.items():
            time_diff = (current_time - msg_details['ts']).total_seconds()
            if time_diff <= one_minute:
                candidates.append(msg_id)
            elif time_diff <= 5*one_minute:
                time_diffs[msg_id] = time_diff        
        
        for fid, score in followups:
            if fid in time_diffs and score>=self.sim_threshold:
                candidates.append[fid]

        messages_to_sort = [history[msg_id] for msg_id in candidates]
        sorted_messages = sorted(messages_to_sort, key=lambda msg: msg['ts'])
        conversation = ''
        for item in sorted_messages:
            if item['role'] == 'ai':
                conversation += 'ai response: '+self.check_and_generate_abstraction(item, 3) + '\n'
            else:
                conversation += 'user input: '+item['text'] + '\n'

        message_data['labels'] = self.gpt.get_mood(conversation, message_data['text'])
        return message_data

    def relevance_module(self, message_data:dict, limit:int=3, debug=False)->tuple[str, dict, dict]:
        history = self.db.read_mems(message_data['user_id'], limit=limit)
        if debug: print(f'Loaded history: {len(history)}')
        if len(history) < 1:
            prompt = ''
        else:
            # Step 2.2: Calculate embedding similarity score
            history_similarity = self.calculate_embedding_similarity(message_data['embedding'], history)
            if debug: print(f'[relevance_module] History similarities: {history_similarity}')

            sorted_history = sorted(history_similarity.items(), key=lambda x: x[1], reverse=True)
            for mid, score in sorted_history:
                if score >= self.same_threshold and history[mid]['role'] == 'user':
                    response = self.db.get_message_by_id('continued', mid)
                    return '', None, response

            followups = self.label_followup_messages(history, message_data, history_similarity, debug) 
            if debug: print(f'[relevance_module] Follow ups in history: {followups}')

            message_data = self.eval_mood(message_data, history, followups)
            if len(followups)>0 and followups[0][1] >= self.sim_threshold: message_data['continued'] = followups[0][0]
            if debug: print(f'[relevance_module] Current Mood: ', message_data['labels'])

            followups = {f[0]:f[1] for f in followups}
            scores = self.prepare_context(history, followups, history_similarity, debug)
            prompt = self.prepare_prompt(history, scores, followups, debug)
        return prompt, message_data, None
    
    def process_message(self, message:str, user_id:int=0, debug=False)->tuple[int, str]:
        """
        Processes the input message and generates a response based on historical interactions.
        """

        current_ts = datetime.now()  # Format timestamp
        current_embedding = self.get_message_embedding(message)
        if debug: print(f'[process_message] Getting Embedding Success. Embedding Dim: {len(current_embedding)}')
        message_data = {
            'text': message,
            'role': 'user',  # AI or user, Assuming role is 'user' for incoming messages
            'ts': current_ts,
            'categories': '',  # Adjust according to your schema
            'labels': '',  # List of Emotional status 
            'embedding': current_embedding,  # Placeholder, adjust as needed
            'continued': 0,  # Placeholder, adjust as needed
            'level1': '',  # Adjust according to your schema
            'level2': '',  # Adjust according to your schema
            'level3': '',  # Adjust according to your schema
            'user_id': str(user_id)  # Ensure user_id is a string if your schema expects it
        }

        response = ''
        # Step 2: Call Relevance module to get the most relevant messages
        prompt, message_data, response = self.relevance_module(message_data, limit=10, debug=debug)
        if debug: print(f'[process_message] Process relevance Success. Aditional Prompt: [{prompt}]')

        if message_data == None and len(response) >0:
            if debug: print(f'[process_message] Already asked question: Message ID: {response["id"]}')
            return response['text']
        
        if len(prompt) < 1:
            # Extract and return the GPT-generated response
            response = self.gpt.gpt_text(message)
        else:
            response = self.gpt.gpt_text('History context:\n'+prompt + message)
        
        if debug: print(f'[process_message] Call LLM Success. Response size: {len(response)}')

        new_mid = self.db.insert_mem(message_data) 
        if debug: print(f'[process_message] Insert user input to DB Success. Message ID: {new_mid}')

        current_ts = datetime.now()
        response_data =  {
            'text': response,
            'role': 'ai',  # Assuming role is 'user' for incoming messages
            'ts': current_ts,
            'categories': '',  # Adjust according to your schema
            'labels': '',  # Adjust according to your schema
            'embedding': self.get_message_embedding(response),  # Placeholder, adjust as needed
            'continued': new_mid,  # response from AI is by default a continued message of the user input message
            'level1': '',  # Adjust according to your schema
            'level2': '',  # Adjust according to your schema
            'level3': '',  # Adjust according to your schema
            'user_id': str(user_id)  # Ensure user_id is a string if your schema expects it
        }
        nmid = self.db.insert_mem(response_data) 
        if debug: print(f'[process_message] Insert ai response to DB Success. Message ID: {nmid}')
        return response
    
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
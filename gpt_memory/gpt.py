from openai import OpenAI
import json

class GPT:

    def __init__(self, model='gpt-4o-mini'):
        self.client = OpenAI()
        self.model = model

    def get_client(self):
        return self.client
    
    def query(self, text, model=None):
        if model == None:
            model = self.model

        return self.client.chat.completions.create(
                messages=[
                    {
                        "role": "user",
                        "content": text,
                    }
                ],
                model=model,
            )
    def gpt_function(self, text, functions, model=None):
        if model == None:
            model = self.model
        try:
            return self.client.chat.completions.create(
                messages=[
                    {
                        "role": "user",
                        "content": text,
                    }
                ],
                model=model,
                functions=functions
            )
        except Exception as e:
            return None
        
    def gpt_text(self, text, model=None):
        if model == None:
            model = self.model
        try:
            response = self.client.chat.completions.create(
                messages=[
                    {
                        "role": "user",
                        "content": text,
                    }
                ],
                model=model
            )
            return response.choices[0].message.content
        except Exception as e:
            return None
        
    def gpt_msgs(self, msgs, model=None):
        if model == None:
            model = self.model
        try:
            response = self.client.chat.completions.create(
                messages=msgs,
                model=model
            )
            return response.choices[0].message.content
        except Exception as e:
            return None
    
    def embeddings(self, text, model="text-embedding-3-large"):
        try:
            response = self.client.embeddings.create(
                input=text,
                model=model
            )
            return response.data[0].embedding
        except Exception as e:
            return None
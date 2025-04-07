import json
from pydantic import BaseModel, Field
from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import PromptTemplate
from sentence_transformers import SentenceTransformer
from getpass import getpass
import os

class GPT:
    def __init__(self, model='deepseek-chat'):
        self.model = model
        self.base_url = 'https://api.deepseek.com'
        self.api_key = os.environ["OPENAI_API_KEY"] if os.environ.get("OPENAI_API_KEY") else getpass.getpass("Enter your LLM API key: ")
        self.client = ChatOpenAI(api_key=self.api_key, model=self.model, temperature=0, base_url=self.base_url)

        
    def gpt_text(self, text):
        messages = [
            ("human", text),
        ]
        ai_msg = self.client.invoke(messages)
        return ai_msg.content
    
    def followup(self, previous_text:str, current_text:str)->any:
        class Score(BaseModel):
            score: float = Field(description="The score of the continuation")

        score_parser = JsonOutputParser(pydantic_object=Score)
        score_prompt = PromptTemplate(
            template="""Given the previous message: '{previous_text}', how likely is the following message '{current_text}' a follow up of the previous message?
            Provide a score from 0 to 1, where 1 is very likely and 0 is not likely. Give a direct answer with a float value."
            {format_instructions}
            """,
            input_variables=["prompt"],
            partial_variables={"format_instructions": score_parser.get_format_instructions()}
        )
        chain = score_prompt | self.client | score_parser
        response = chain.invoke({"previous_text": previous_text, "current_text": current_text})
        return response
    
    def get_mood(self, conversation:str, input:str)->list[str]:
        class Mood(BaseModel):
            mood: list[str] = Field(description="The list of emotional status categories.")

        score_parser = JsonOutputParser(pydantic_object=Mood)
        score_prompt = PromptTemplate(
            template="""You are a Psychiatrist evaluating user's emotional status. Given the following conversation between AI and users: \n\n'{conversation}'.\n\n 
            Now user input the following message: '{input}'. What is the user's emotional status? Label the emotional status with the following categories: 
            Joy, Sadness, Anger, Fear, Surprise, Disgust, Trust and Anticipation. User could be in multiple categories at the same time. 
            For example, 'I am so excited to learn that.' includes Joy and Surprise.
            {format_instructions}
            """,
            input_variables=["prompt"],
            partial_variables={"format_instructions": score_parser.get_format_instructions()}
        )
        chain = score_prompt | self.client | score_parser
        response = chain.invoke({"conversation": conversation, "input": input})
        return response['mood']
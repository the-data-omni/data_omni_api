import openai
import os
from ..utils.logging_config import logger

# Set your OpenAI API key
openai.api_key = os.getenv("OPENAI_API_KEY")

def generate_natural_language_question(query):
    """Generates a natural language question using gpt-3.5-turbo."""
    try:
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a helpful assistant that translates BigQuery SQL queries into natural language questions."},
                {"role": "user", "content": f"Translate the following BigQuery SQL query into a natural language question:\n```sql\n{query}\n```"}
            ],
            max_tokens=150,
            n=1,
            stop=None,
            temperature=0.7,
        )
        question = response.choices[0].message['content'].strip()
        return question
    except openai.error.OpenAIError as e:
        logger.error(f"OpenAI API error: {e}")
        return f"Error generating question for: {query}"

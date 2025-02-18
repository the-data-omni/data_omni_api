"""service to use llm (openai) to translate bigquery queries to questions"""
import os
import openai
from openai import OpenAI

# Set your OpenAI API key
openai.api_key = os.getenv("OPENAI_API_KEY")
client = OpenAI(
    api_key=os.environ.get("OPENAI_API_KEY"),  # This is the default and can be omitted
)


def generate_natural_language_question(query):
    """Generates a natural language question using gpt-3.5-turbo."""
    try:
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a helpful assistant that translates BigQuery SQL queries into natural language questions.\n\n"
                        "Here is an example of the style we want:\n\n"
                        "EXAMPLE SQL:\n"
                        "  SELECT name\n"
                        "  FROM employees\n"
                        "  WHERE department = 'Sales';\n\n"
                        "EXAMPLE NATURAL LANGUAGE QUESTION:\n"
                        "  'Which employees work in the Sales department?'\n\n"
                        "Notice that the question focuses on what a user might *ask* in plain English.\n\n"
                        "Now, please produce a similar natural language question for the following SQL query:"
                    )
                },
                {
                    "role": "user",
                    "content": f"```sql\n{query}\n```"
                }
            ],
            max_tokens=150,
            n=1,
            stop=None,
            temperature=0.7,
        )
        question = response.choices[0].message.content.strip()
        return question
    except openai.APIConnectionError as e:
        print("The server could not be reached")
        print(e.__cause__)
        return f"Error generating question for: {query}"
    except openai.RateLimitError as e:
        print("A 429 status code was received; we should back off a bit. %s", {e})
        return f"Error generating question for: {query}"
    except openai.APIStatusError as e:
        print("Another non-200-range status code was received")
        print(e.status_code)
        print(e.response)
        return f"Error generating question for: {query}"



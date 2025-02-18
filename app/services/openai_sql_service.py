# services/openai_sql_service.py

import os
import openai
from openai import OpenAI

# Set your OpenAI API key
openai.api_key = os.getenv("OPENAI_API_KEY")

# Create a client instance
client = OpenAI(
    api_key=os.environ.get("OPENAI_API_KEY"),  # same as openai.api_key, but explicit
)

def generate_sql_from_question(user_question: str, existing_sql: str = "") -> str:
    """
    Generates or refines a SQL query using gpt-3.5-turbo, following the older
    openai library style with client.chat.completions.create().

    :param user_question: Natural language question or request about the data.
    :param existing_sql: Optional existing SQL to refine (or empty to create new).
    :return: A string containing the generated/refined SQL (or an error message).
    """
    try:
        # System instructions: only output valid BigQuery SQL, no extra text
        system_prompt = (
            "You are a helpful AI that generates or refines SQL queries.\n"
            "Only output valid SQL, with no extra commentary or explanation.\n"
            "Use BigQuery dialect by default.\n"
            "If the user provides existing SQL, refine or modify it.\n"
            "If no existing SQL is provided, create a new valid SQL statement.\n"
        )

        # We'll pass both the existing SQL and the user question in the conversation
        messages = [
            {
                "role": "system",
                "content": system_prompt
            },
            {
                "role": "user",
                "content": (
                    f"Existing SQL:\n```sql\n{existing_sql}\n```\n\n"
                    f"User question:\n{user_question}"
                ),
            }
        ]

        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=messages,
            temperature=0.2,   # keep it lower for more precise SQL
            max_tokens=512,
            n=1,
            stop=None,
        )

        # Extract the SQL from the response
        new_sql = response.choices[0].message.content.strip()
        return new_sql

    except openai.APIConnectionError as e:
        print("The server could not be reached:", e.__cause__)
        return "ERROR: Connection to OpenAI failed."
    except openai.RateLimitError as e:
        print("A 429 status code was received; we should back off a bit.", e)
        return "ERROR: Rate limit reached."
    except openai.APIStatusError as e:
        print("Another non-200-range status code was received", e.status_code)
        return f"ERROR: OpenAI returned status {e.status_code}"
    except Exception as e:
        print("Unknown error in generate_sql_from_question:", e)
        return f"ERROR: {str(e)}"

import os

class Config:
    # Add any configuration variables you might need
    OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
    # You can add more configurations like DEBUG, DATABASE_URI, etc.

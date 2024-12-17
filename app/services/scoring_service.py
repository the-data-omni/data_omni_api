# import logging
# import os
# from flask import jsonify
# from google.cloud import bigquery
# from ..utils.logging_config import logger
# import json
# import re
# from collections import defaultdict
# from itertools import combinations
# from difflib import SequenceMatcher

# def score_gen_ai(schema):
#     # Weights
#     weights = {
#         'field_names': 35,
#         'field_descriptions': 25,
#         'field_name_similarity': 20,
#         'field_types': 10,
#         'keys_presence': 10
#     }
    
#     # Initialize counts
#     total_fields = len(schema)
#     total_tables = set(entry['table_name'] for entry in schema)
#     num_tables = len(total_tables)
    
#     # Initialize metrics
#     field_names_score = 0
#     field_descriptions_score = 0
#     field_name_similarity_score = 0
#     field_types_score = 0
#     primary_key_score = 0
#     foreign_key_score = 0
    
#     # Field Names Metric
#     # Check for naming convention consistency (e.g., snake_case)
#     snake_case_pattern = re.compile(r'^[a-z0-9_]+$')
#     camel_case_pattern = re.compile(r'^[a-z]+(?:[A-Z][a-z]+)*$')
#     pascal_case_pattern = re.compile(r'^[A-Z][a-z]+(?:[A-Z][a-z]+)*$')
    
#     # Determine which naming convention is predominantly used
#     snake_case_count = 0
#     camel_case_count = 0
#     pascal_case_count = 0
#     for entry in schema:
#         field_name = entry['column_name']
#         if snake_case_pattern.match(field_name):
#             snake_case_count += 1
#         if camel_case_pattern.match(field_name):
#             camel_case_count += 1  
#         if pascal_case_pattern.match(field_name):
#             pascal_case_count += 1
    
#     # Determine the dominant naming convention
#     naming_convention_counts = {
#         'snake_case': snake_case_count,
#         'camel_case': camel_case_count,
#         'pascal_case': pascal_case_count
#     }
#     dominant_convention = max(naming_convention_counts, key=naming_convention_counts.get)
#     total_convention_fields = naming_convention_counts[dominant_convention]
    
#     # Field Names Score: Proportion of field names following the dominant convention
#     field_names_score = (total_convention_fields / total_fields) * weights['field_names']
    
#     # Field Descriptions Metric
#     fields_with_descriptions = sum(1 for entry in schema if entry.get('description'))
#     field_descriptions_score = (fields_with_descriptions / total_fields) * weights['field_descriptions']
    
#     # Field Name Similarity Metric
#     # Compute similarity between field names
#     field_names = [entry['column_name'] for entry in schema]
#     similarity_scores = []
#     for name1, name2 in combinations(field_names, 2):
#         similarity = SequenceMatcher(None, name1, name2).ratio()
#         similarity_scores.append(similarity)
#     if similarity_scores:
#         average_similarity = sum(similarity_scores) / len(similarity_scores)
#     else:
#         average_similarity = 0  # Only one field, no similarity to compute
    
#     # Field Name Similarity Score: Inversely proportional to average similarity
#     # Higher similarity reduces the score
#     field_name_similarity_score = (1 - average_similarity) * weights['field_name_similarity']
    
#     # Field Types Metric
#     fields_with_types = sum(1 for entry in schema if entry.get('data_type'))
#     field_types_score = (fields_with_types / total_fields) * weights['field_types']
    
#     # Primary/Foreign Key Presence Metric
#     # Group fields by table
#     tables = defaultdict(list)
#     for entry in schema:
#         tables[entry['table_name']].append(entry)
    
#     tables_with_primary_key = 0
#     tables_with_foreign_key = 0
#     for table_fields in tables.values():
#         has_primary_key = any(field.get('primary_key') for field in table_fields)
#         has_foreign_key = any(field.get('foreign_key') for field in table_fields)
#         if has_primary_key:
#             tables_with_primary_key += 1
#         if has_foreign_key:
#             tables_with_foreign_key += 1
    
#     # Each contributes 5% to the total score
#     primary_key_score = (tables_with_primary_key / num_tables) * (weights['keys_presence'] / 2)
#     foreign_key_score = (tables_with_foreign_key / num_tables) * (weights['keys_presence'] / 2)
#     keys_presence_score = primary_key_score + foreign_key_score
    
#     # Total Score
#     total_score = (
#         field_names_score +
#         field_descriptions_score +
#         field_name_similarity_score +
#         field_types_score +
#         keys_presence_score
#     )
    
#     # Prepare detailed scores
#     detailed_scores = {
#         'Field Names Score': field_names_score,
#         'Field Descriptions Score': field_descriptions_score,
#         'Field Name Similarity Score': field_name_similarity_score,
#         'Field Types Score': field_types_score,
#         'Keys Presence Score': keys_presence_score,
#         'Total Score': total_score
#     }
    
#     return detailed_scores

import logging
import os
from flask import jsonify
from google.cloud import bigquery
from ..utils.logging_config import logger
import json
import re
from collections import defaultdict
from itertools import combinations
from difflib import SequenceMatcher
import openai

openai.api_key = os.getenv("OPENAI_API_KEY")
if not openai.api_key:
    logger.error("OpenAI API key not found. Please set OPENAI_API_KEY environment variable.")

def score_gen_ai(schema):
    # Validate schema
    if not isinstance(schema, list) or len(schema) == 0:
        logger.error("Schema provided to score_gen_ai is invalid or empty.")
        return {
            'error': 'Invalid schema input',
            'message': 'Schema must be a non-empty list of field dictionaries.'
        }

    required_keys = {'table_name', 'column_name'}
    for entry in schema:
        if not required_keys.issubset(entry.keys()):
            logger.error(f"Schema entry missing required keys: {entry}")
            return {
                'error': 'Invalid schema entry',
                'message': f'Each schema entry must contain at least {required_keys}'
            }

    # Weights
    weights = {
        'field_names': 35,
        'field_descriptions': 25,
        'field_name_similarity': 20,
        'field_types': 10,
        'keys_presence': 10
    }

    total_fields = len(schema)
    total_tables = set(entry['table_name'] for entry in schema)
    num_tables = len(total_tables)

    # Initialize metrics
    field_names_score = 0
    field_descriptions_score = 0
    field_name_similarity_score = 0
    field_types_score = 0
    primary_key_score = 0
    foreign_key_score = 0

    non_meaningful_penalized_fields = set()
    similarity_penalized_fields = set()

    def is_field_name_meaningful_openai(field_name):
        if not openai.api_key:
            logger.error("OpenAI API key not set. Cannot call OpenAI API.")
            # Fallback to True to avoid penalizing due to missing API key
            return True

        prompt = f"""
The following is a database field name: "{field_name}"

Decide if this field name clearly indicates its purpose to a user. 
A meaningful name suggests the type of data it holds or its role. 
If it's too vague, generic, or abstract and does not convey its meaning, it is not meaningful.

Return "meaningful" if the name itself is clear and meaningful, or "not meaningful" if it is too vague.
"""
        try:
            response = openai.Completion.create(
                model="text-davinci-003",
                prompt=prompt,
                max_tokens=50,
                temperature=0
            )
            answer = response.choices[0].text.strip().lower()
            return "meaningful" in answer
        except Exception as e:
            logger.error(f"Error calling OpenAI API for field name '{field_name}': {e}")
            # If there's an error in the API call, default to True (no penalty)
            return True

    # Compute how many fields are meaningful or described
    fields_meaningful_or_described = 0
    for entry in schema:
        field_name = entry['column_name']
        desc_val = entry.get('description')
        desc = desc_val.strip() if desc_val and isinstance(desc_val, str) else ''

        if is_field_name_meaningful_openai(field_name):
            fields_meaningful_or_described += 1
        else:
            if desc:
                fields_meaningful_or_described += 1
            else:
                non_meaningful_penalized_fields.add(field_name)

    if total_fields == 0:
        logger.error("No fields found in schema, cannot compute scores.")
        return {
            'error': 'No fields in schema',
            'message': 'Schema had zero fields.'
        }

    field_names_score = (fields_meaningful_or_described / total_fields) * weights['field_names']

    # Field Descriptions Metric
    fields_with_descriptions = sum(1 for entry in schema if entry.get('description') and isinstance(entry.get('description'), str) and entry['description'].strip())
    field_descriptions_score = (fields_with_descriptions / total_fields) * weights['field_descriptions']

    # Field Types Metric
    fields_with_types = sum(1 for entry in schema if entry.get('data_type'))
    field_types_score = (fields_with_types / total_fields) * weights['field_types']

    # Keys Presence Metric
    tables_dict = defaultdict(list)
    for entry in schema:
        tables_dict[entry['table_name']].append(entry)

    if num_tables == 0:
        logger.error("No tables found in schema.")
        primary_key_score = 0
        foreign_key_score = 0
        keys_presence_score = 0
    else:
        tables_with_primary_key = 0
        tables_with_foreign_key = 0
        for table_fields in tables_dict.values():
            has_primary_key = any(field.get('primary_key') for field in table_fields)
            has_foreign_key = any(field.get('foreign_key') for field in table_fields)
            if has_primary_key:
                tables_with_primary_key += 1
            if has_foreign_key:
                tables_with_foreign_key += 1

        primary_key_score = (tables_with_primary_key / num_tables) * (weights['keys_presence'] / 2)
        foreign_key_score = (tables_with_foreign_key / num_tables) * (weights['keys_presence'] / 2)
        keys_presence_score = primary_key_score + foreign_key_score

    # Field Name Similarity Metric (Adjusted)
    field_names = [entry['column_name'] for entry in schema]
    field_descriptions_map = {}
    field_table_map = {}

    for entry in schema:
        desc_val = entry.get('description')
        desc = desc_val.strip() if desc_val and isinstance(desc_val, str) else ''
        field_descriptions_map[entry['column_name']] = desc
        field_table_map[entry['column_name']] = entry['table_name']

    similarity_threshold = 0.8
    similar_pairs = []
    if len(field_names) > 1:
        for name1, name2 in combinations(field_names, 2):
            table1 = field_table_map[name1]
            table2 = field_table_map[name2]
            if table1 != table2:
                # Different tables, no penalty
                continue

            similarity = SequenceMatcher(None, name1, name2).ratio()
            if similarity >= similarity_threshold:
                desc1 = field_descriptions_map[name1]
                desc2 = field_descriptions_map[name2]
                if desc1 and desc2 and desc1 != desc2:
                    # Descriptions differ, no penalty
                    continue
                else:
                    similar_pairs.append((name1, name2))
                    # Penalize these fields
                    similarity_penalized_fields.add(name1)
                    similarity_penalized_fields.add(name2)
        total_pairs = (total_fields * (total_fields - 1)) / 2
    else:
        total_pairs = 1  # Only one field, no pairs

    confusion_rate = len(similar_pairs) / total_pairs if total_pairs > 0 else 0
    field_name_similarity_score = (1 - confusion_rate) * weights['field_name_similarity']

    # Total Score
    total_score = (
        field_names_score +
        field_descriptions_score +
        field_name_similarity_score +
        field_types_score +
        keys_presence_score
    )

    penalized_fields = {
        'NonMeaningful_NoDescription': list(non_meaningful_penalized_fields),
        'Similar_Undifferentiated': list(similarity_penalized_fields)
    }

    detailed_scores = {
        'Field Names Score': field_names_score,
        'Field Descriptions Score': field_descriptions_score,
        'Field Name Similarity Score': field_name_similarity_score,
        'Field Types Score': field_types_score,
        'Keys Presence Score': keys_presence_score,
        'Total Score': total_score,
        'Penalized Fields': penalized_fields
    }

    return detailed_scores


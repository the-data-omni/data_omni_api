# import logging
# from collections import defaultdict
# from itertools import combinations

# import spacy
# from sentence_transformers import SentenceTransformer, util

# # ------------------------------------------
# # Configure Logging
# # ------------------------------------------
# logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)
# ch = logging.StreamHandler()
# ch.setLevel(logging.DEBUG)
# logger.addHandler(ch)

# # ------------------------------------------
# # Load spaCy model with word vectors
# # ------------------------------------------
# nlp = spacy.load("en_core_web_md")

# # Reference docs for optional similarity checks in spaCy
# doc_meaningful_ref = nlp("meaningful field name")
# doc_placeholder_ref = nlp("placeholder unknown generic dummy test")


# def is_field_name_meaningful_spacy_advanced(
#     field_name: str,
#     doc_similarity_meaningful_min: float = 0.05,
#     doc_similarity_placeholder_max: float = 0.80
# ) -> bool:
#     """
#     Check if the field name is likely user-friendly (spaCy-based).
    
#     Parameters:
#     -----------
#     field_name : str
#         The raw column name from the schema.
#     doc_similarity_meaningful_min : float
#         The minimum similarity to "meaningful field name" doc. If below, we call it not meaningful.
#     doc_similarity_placeholder_max : float
#         The maximum similarity to "placeholder unknown generic dummy test." If above, not meaningful.

#     Heuristics:
#       1. Adequate length (> 3 characters) after strip.
#       2. Replace underscores with spaces so that e.g. "event_name" -> "event name".
#       3. Contains at least one token that is not punctuation or numeric.
#       4. Contains at least one noun/proper noun/adjective.
#       5. doc similarity to "meaningful field name" >= doc_similarity_meaningful_min
#          doc similarity to "placeholder unknown ..." <= doc_similarity_placeholder_max
#     """
#     # Preprocess underscores => spaces
#     field_name_proc = field_name.strip().replace("_", " ")

#     # Basic length check
#     if len(field_name_proc) < 4:
#         return False

#     doc = nlp(field_name_proc)

#     # Filter out punctuation, numeric tokens, or empty strings
#     valid_tokens = [
#         token for token in doc
#         if not token.is_punct
#         and not token.like_num
#         and token.text.strip()
#     ]
#     if not valid_tokens:
#         return False

#     # Check for at least one NOUN, PROPN, or ADJ
#     has_informative_pos = any(token.pos_ in ("NOUN", "PROPN", "ADJ") for token in valid_tokens)
#     if not has_informative_pos:
#         return False

#     # (Optional) doc-similarity checks
#     doc_similarity_meaningful = doc.similarity(doc_meaningful_ref)
#     doc_similarity_placeholder = doc.similarity(doc_placeholder_ref)

#     # If doc similarity to "meaningful field name" is too low, fail
#     if doc_similarity_meaningful < doc_similarity_meaningful_min:
#         return False

#     # If doc similarity to "placeholder unknown..." is too high, fail
#     if doc_similarity_placeholder > doc_similarity_placeholder_max:
#         return False

#     return True

# # ------------------------------------------
# # Initialize local SentenceTransformer model
# # ------------------------------------------
# model = SentenceTransformer('all-MiniLM-L6-v2')

# def score_gen_ai(
#     schema,
#     similarity_threshold: float = 0.8,
#     doc_similarity_meaningful_min: float = 0.05,
#     doc_similarity_placeholder_max: float = 0.80,
#     weights_override: dict = None
# ):
#     """
#     Main function to score how 'good' a database schema is, based on:
#       - meaningful field names (via spaCy-based advanced check),
#       - presence of descriptions,
#       - distinct/unique field names (via Sentence-Transformers),
#       - presence of data types,
#       - presence of primary/foreign keys.

#     Parameters:
#     -----------
#     schema : List[dict]
#         A list of dictionaries, each with at least:
#         {
#           'table_name': str,
#           'column_name': str,
#           'description': Optional[str],
#           'data_type': Optional[str],
#           'primary_key': Optional[bool],
#           'foreign_key': Optional[bool]
#         }

#     similarity_threshold : float
#         If the cosine similarity between two field names >= this value (and they're in the same table,
#         with identical or no descriptions), we flag them as "too similar."

#     doc_similarity_meaningful_min : float
#         The minimum spaCy similarity to "meaningful field name" required to consider a name meaningful.

#     doc_similarity_placeholder_max : float
#         The maximum spaCy similarity to "placeholder unknown..." before we consider it meaningless.

#     weights_override : dict
#         Allows you to override the default weight distribution. For example:
#             {
#               'field_names': 30,
#               'field_descriptions': 25,
#               'field_name_similarity': 25,
#               'field_types': 10,
#               'keys_presence': 10
#             }
#         Any weights not specified will fall back to the defaults.

#     Returns:
#     --------
#     dict
#         A structure containing detailed scoring and lists of penalized fields.
#     """

#     # Default weights
#     default_weights = {
#         'field_names': 35,
#         'field_descriptions': 25,
#         'field_name_similarity': 20,
#         'field_types': 10,
#         'keys_presence': 10
#     }

#     # If user passed an override dictionary, merge it with defaults
#     if not weights_override:
#         weights = default_weights
#     else:
#         weights = default_weights.copy()
#         for k, v in weights_override.items():
#             # Only update valid keys
#             if k in weights:
#                 weights[k] = v

#     # Validate schema
#     if not isinstance(schema, list) or len(schema) == 0:
#         logger.error("Schema provided to score_gen_ai is invalid or empty.")
#         return {
#             'error': 'Invalid schema input',
#             'message': 'Schema must be a non-empty list of field dictionaries.'
#         }

#     required_keys = {'table_name', 'column_name'}
#     for entry in schema:
#         if not required_keys.issubset(entry.keys()):
#             logger.error(f"Schema entry missing required keys: {entry}")
#             return {
#                 'error': 'Invalid schema entry',
#                 'message': f'Each schema entry must contain at least {required_keys}'
#             }

#     total_fields = len(schema)
#     total_tables = set(entry['table_name'] for entry in schema)
#     num_tables = len(total_tables)

#     if total_fields == 0:
#         logger.error("No fields found in schema, cannot compute scores.")
#         return {
#             'error': 'No fields in schema',
#             'message': 'Schema had zero fields.'
#         }

#     # Initialize scoring variables
#     field_names_score = 0
#     field_descriptions_score = 0
#     field_name_similarity_score = 0
#     field_types_score = 0
#     keys_presence_score = 0

#     # Sets to track penalized fields
#     non_meaningful_fields = set()
#     non_meaningful_no_description = set()
#     similarity_penalized_fields = set()

#     # For numeric scoring: how many fields are "meaningful or described"
#     fields_meaningful_or_described = 0

#     # ------------------------------------------
#     # 1) Evaluate field names for meaningfulness
#     # ------------------------------------------
#     for entry in schema:
#         field_name = entry['column_name']
#         desc_val = entry.get('description')
#         desc = desc_val.strip() if desc_val and isinstance(desc_val, str) else ''

#         # Use adjustable spaCy check
#         if is_field_name_meaningful_spacy_advanced(
#             field_name,
#             doc_similarity_meaningful_min=doc_similarity_meaningful_min,
#             doc_similarity_placeholder_max=doc_similarity_placeholder_max
#         ):
#             fields_meaningful_or_described += 1
#         else:
#             non_meaningful_fields.add(field_name)
#             if desc:
#                 # If there's a description, we don't penalize in numeric score
#                 fields_meaningful_or_described += 1
#             else:
#                 # No description => numeric penalty
#                 non_meaningful_no_description.add(field_name)

#     # (a) Field Names Score
#     field_names_score = (fields_meaningful_or_described / total_fields) * weights['field_names']

#     # ------------------------------------------
#     # 2) Field Descriptions Metric
#     # ------------------------------------------
#     fields_with_descriptions = sum(
#         1 for entry in schema
#         if entry.get('description')
#         and isinstance(entry.get('description'), str)
#         and entry['description'].strip()
#     )
#     field_descriptions_score = (fields_with_descriptions / total_fields) * weights['field_descriptions']

#     # ------------------------------------------
#     # 3) Field Types Metric
#     # ------------------------------------------
#     fields_with_types = sum(1 for entry in schema if entry.get('data_type'))
#     field_types_score = (fields_with_types / total_fields) * weights['field_types']

#     # ------------------------------------------
#     # 4) Keys Presence Metric
#     # ------------------------------------------
#     if num_tables > 0:
#         tables_dict = defaultdict(list)
#         for entry in schema:
#             tables_dict[entry['table_name']].append(entry)

#         tables_with_primary_key = 0
#         tables_with_foreign_key = 0
#         for table_fields in tables_dict.values():
#             has_primary_key = any(field.get('primary_key') for field in table_fields)
#             has_foreign_key = any(field.get('foreign_key') for field in table_fields)
#             if has_primary_key:
#                 tables_with_primary_key += 1
#             if has_foreign_key:
#                 tables_with_foreign_key += 1

#         primary_key_score = (tables_with_primary_key / num_tables) * (weights['keys_presence'] / 2)
#         foreign_key_score = (tables_with_foreign_key / num_tables) * (weights['keys_presence'] / 2)
#         keys_presence_score = primary_key_score + foreign_key_score
#     else:
#         logger.error("No tables found in schema.")
#         keys_presence_score = 0

#     # ------------------------------------------
#     # 5) Field Name Similarity Metric
#     # ------------------------------------------
#     field_names = [entry['column_name'] for entry in schema]
#     field_descriptions_map = {}
#     field_table_map = {}

#     for entry in schema:
#         desc_val = entry.get('description')
#         desc_str = desc_val.strip() if desc_val and isinstance(desc_val, str) else ''
#         field_descriptions_map[entry['column_name']] = desc_str
#         field_table_map[entry['column_name']] = entry['table_name']

#     similar_pairs = []

#     if len(field_names) > 1:
#         # Encode each field name once
#         name_to_embedding = {
#             name: model.encode(name, convert_to_tensor=True) for name in field_names
#         }

#         for name1, name2 in combinations(field_names, 2):
#             # Only check fields in the same table
#             if field_table_map[name1] != field_table_map[name2]:
#                 continue

#             sim_score = util.pytorch_cos_sim(
#                 name_to_embedding[name1],
#                 name_to_embedding[name2]
#             ).item()

#             if sim_score >= similarity_threshold:
#                 desc1 = field_descriptions_map[name1]
#                 desc2 = field_descriptions_map[name2]
#                 if desc1 and desc2 and desc1 != desc2:
#                     # Different descriptions => user can differentiate => no penalty
#                     continue
#                 else:
#                     similar_pairs.append((name1, name2))
#                     similarity_penalized_fields.add(name1)
#                     similarity_penalized_fields.add(name2)

#         total_pairs = (total_fields * (total_fields - 1)) / 2
#         confusion_rate = len(similar_pairs) / total_pairs if total_pairs > 0 else 0
#     else:
#         # Only one field => no pairs
#         total_pairs = 1
#         confusion_rate = 0

#     field_name_similarity_score = (1 - confusion_rate) * weights['field_name_similarity']

#     # -------------------
#     # Compute Total Score
#     # -------------------
#     total_score = (
#         field_names_score +
#         field_descriptions_score +
#         field_name_similarity_score +
#         field_types_score +
#         keys_presence_score
#     )

#     # -------------------
#     # Return Detailed Info
#     # -------------------
#     penalized_fields = {
#         'NonMeaningful': list(non_meaningful_fields),
#         'NonMeaningful_NoDescription': list(non_meaningful_no_description),
#         'Similar_Undifferentiated': list(similarity_penalized_fields),
#     }

#     detailed_scores = {
#         'Field Names Score': field_names_score,
#         'Field Descriptions Score': field_descriptions_score,
#         'Field Name Similarity Score': field_name_similarity_score,
#         'Field Types Score': field_types_score,
#         'Keys Presence Score': keys_presence_score,
#         'Total Score': total_score,
#         'Penalized Fields': penalized_fields
#     }

#     return detailed_scores
import logging
from collections import defaultdict
from itertools import combinations

import spacy
from sentence_transformers import SentenceTransformer, util

# ------------------------------------------
# Configure Logging
# ------------------------------------------
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

# ------------------------------------------
# Load spaCy model with word vectors
# ------------------------------------------
nlp = spacy.load("en_core_web_md")

# Reference docs for optional similarity checks in spaCy
doc_meaningful_ref = nlp("meaningful field name")
doc_placeholder_ref = nlp("placeholder unknown generic dummy test")


def is_field_name_meaningful_spacy_advanced(
    field_name: str,
    doc_similarity_meaningful_min: float = 0.05,
    doc_similarity_placeholder_max: float = 0.80
) -> bool:
    """
    Check if the field name is likely user-friendly (spaCy-based).
    """
    # Preprocess underscores => spaces
    field_name_proc = field_name.strip().replace("_", " ")

    # Basic length check
    if len(field_name_proc) < 4:
        return False

    doc = nlp(field_name_proc)

    # Filter out punctuation, numeric tokens, or empty strings
    valid_tokens = [
        token for token in doc
        if not token.is_punct
        and not token.like_num
        and token.text.strip()
    ]
    if not valid_tokens:
        return False

    # Check for at least one NOUN, PROPN, or ADJ
    has_informative_pos = any(token.pos_ in ("NOUN", "PROPN", "ADJ") for token in valid_tokens)
    if not has_informative_pos:
        return False

    # (Optional) doc-similarity checks
    doc_similarity_meaningful = doc.similarity(doc_meaningful_ref)
    doc_similarity_placeholder = doc.similarity(doc_placeholder_ref)

    # If doc similarity to "meaningful field name" is too low, fail
    if doc_similarity_meaningful < doc_similarity_meaningful_min:
        return False

    # If doc similarity to "placeholder unknown..." is too high, fail
    if doc_similarity_placeholder > doc_similarity_placeholder_max:
        return False

    return True

# ------------------------------------------
# Initialize local SentenceTransformer model
# ------------------------------------------
model = SentenceTransformer('all-MiniLM-L6-v2')


def score_gen_ai(
    schema,
    similarity_threshold: float = 0.8,
    doc_similarity_meaningful_min: float = 0.05,
    doc_similarity_placeholder_max: float = 0.80,
    weights_override: dict = None
):
    """
    Main function to score how 'good' a database schema is, returning both
    numeric scores and percentage-based scores for each metric.
    """

    # Default weights
    default_weights = {
        'field_names': 35,
        'field_descriptions': 25,
        'field_name_similarity': 20,
        'field_types': 10,
        'keys_presence': 10
    }

    # If user passed an override dictionary, merge it with defaults
    if not weights_override:
        weights = default_weights
    else:
        weights = default_weights.copy()
        for k, v in weights_override.items():
            # Only update valid keys
            if k in weights:
                weights[k] = v

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

    total_fields = len(schema)
    total_tables = set(entry['table_name'] for entry in schema)
    num_tables = len(total_tables)

    if total_fields == 0:
        logger.error("No fields found in schema, cannot compute scores.")
        return {
            'error': 'No fields in schema',
            'message': 'Schema had zero fields.'
        }

    # Initialize scoring variables
    field_names_score = 0.0
    field_descriptions_score = 0.0
    field_name_similarity_score = 0.0
    field_types_score = 0.0
    keys_presence_score = 0.0

    # Sets to track penalized fields
    non_meaningful_fields = set()
    non_meaningful_no_description = set()
    similarity_penalized_fields = set()

    # For numeric scoring: how many fields are "meaningful or described"
    fields_meaningful_or_described = 0

    # ------------------------------------------
    # 1) Evaluate field names for meaningfulness
    # ------------------------------------------
    for entry in schema:
        field_name = entry['column_name']
        desc_val = entry.get('description')
        desc = desc_val.strip() if desc_val and isinstance(desc_val, str) else ''

        # Use adjustable spaCy check
        if is_field_name_meaningful_spacy_advanced(
            field_name,
            doc_similarity_meaningful_min=doc_similarity_meaningful_min,
            doc_similarity_placeholder_max=doc_similarity_placeholder_max
        ):
            fields_meaningful_or_described += 1
        else:
            non_meaningful_fields.add(field_name)
            if desc:
                # If there's a description, we don't penalize in numeric score
                fields_meaningful_or_described += 1
            else:
                # No description => numeric penalty
                non_meaningful_no_description.add(field_name)

    # (a) Field Names Score
    field_names_score = (fields_meaningful_or_described / total_fields) * weights['field_names']

    # ------------------------------------------
    # 2) Field Descriptions Metric
    # ------------------------------------------
    fields_with_descriptions = sum(
        1 for entry in schema
        if entry.get('description')
        and isinstance(entry.get('description'), str)
        and entry['description'].strip()
    )
    field_descriptions_score = (fields_with_descriptions / total_fields) * weights['field_descriptions']

    # ------------------------------------------
    # 3) Field Types Metric
    # ------------------------------------------
    fields_with_types = sum(1 for entry in schema if entry.get('data_type'))
    field_types_score = (fields_with_types / total_fields) * weights['field_types']

    # ------------------------------------------
    # 4) Keys Presence Metric
    # ------------------------------------------
    if num_tables > 0:
        tables_dict = defaultdict(list)
        for entry in schema:
            tables_dict[entry['table_name']].append(entry)

        tables_with_primary_key = 0
        tables_with_foreign_key = 0
        for table_fields in tables_dict.values():
            has_primary_key = any(field.get('primary_key') for field in table_fields)
            has_foreign_key = any(field.get('foreign_key') for field in table_fields)
            if has_primary_key:
                tables_with_primary_key += 1
            if has_foreign_key:
                tables_with_foreign_key += 1

        # Split the 'keys_presence' weight between primary key and foreign key
        primary_key_score = (tables_with_primary_key / num_tables) * (weights['keys_presence'] / 2)
        foreign_key_score = (tables_with_foreign_key / num_tables) * (weights['keys_presence'] / 2)
        keys_presence_score = primary_key_score + foreign_key_score
    else:
        logger.error("No tables found in schema.")
        keys_presence_score = 0

    # ------------------------------------------
    # 5) Field Name Similarity Metric
    # ------------------------------------------
    field_names = [entry['column_name'] for entry in schema]
    field_descriptions_map = {}
    field_table_map = {}

    for entry in schema:
        desc_val = entry.get('description')
        desc_str = desc_val.strip() if desc_val and isinstance(desc_val, str) else ''
        field_descriptions_map[entry['column_name']] = desc_str
        field_table_map[entry['column_name']] = entry['table_name']

    similar_pairs = []

    if len(field_names) > 1:
        # Encode each field name once
        name_to_embedding = {
            name: model.encode(name, convert_to_tensor=True) for name in field_names
        }

        for name1, name2 in combinations(field_names, 2):
            # Only check fields in the same table
            if field_table_map[name1] != field_table_map[name2]:
                continue

            sim_score = util.pytorch_cos_sim(
                name_to_embedding[name1],
                name_to_embedding[name2]
            ).item()

            if sim_score >= similarity_threshold:
                desc1 = field_descriptions_map[name1]
                desc2 = field_descriptions_map[name2]
                if desc1 and desc2 and desc1 != desc2:
                    # Different descriptions => user can differentiate => no penalty
                    continue
                else:
                    similar_pairs.append((name1, name2))
                    similarity_penalized_fields.add(name1)
                    similarity_penalized_fields.add(name2)

        total_pairs = (total_fields * (total_fields - 1)) / 2
        confusion_rate = len(similar_pairs) / total_pairs if total_pairs > 0 else 0
    else:
        # Only one field => no pairs
        total_pairs = 1
        confusion_rate = 0

    field_name_similarity_score = (1 - confusion_rate) * weights['field_name_similarity']

    # -------------------
    # Compute Total Score
    # -------------------
    total_score = (
        field_names_score +
        field_descriptions_score +
        field_name_similarity_score +
        field_types_score +
        keys_presence_score
    )

    # -------------------
    # Percentages
    # -------------------
    # If you assume sum(weights) is your "max possible score"
    # then total_score / sum(weights) * 100 is your overall %
    sum_weights = sum(weights.values())

    field_names_score_pct = (field_names_score / weights['field_names']) * 100
    field_descriptions_score_pct = (field_descriptions_score / weights['field_descriptions']) * 100
    field_name_similarity_score_pct = (field_name_similarity_score / weights['field_name_similarity']) * 100
    field_types_score_pct = (field_types_score / weights['field_types']) * 100
    keys_presence_score_pct = (keys_presence_score / weights['keys_presence']) * 100
    total_score_pct = (total_score / sum_weights) * 100

    # -------------------
    # Return Detailed Info
    # -------------------
    penalized_fields = {
        'NonMeaningful': list(non_meaningful_fields),
        'NonMeaningful_NoDescription': list(non_meaningful_no_description),
        'Similar_Undifferentiated': list(similarity_penalized_fields),
    }

    detailed_scores = {
        # Raw numeric scores
        'Field Names Score': field_names_score,
        'Field Descriptions Score': field_descriptions_score,
        'Field Name Similarity Score': field_name_similarity_score,
        'Field Types Score': field_types_score,
        'Keys Presence Score': keys_presence_score,
        'Total Score': total_score,

        # Percentage-based scores
        'Field Names Score (%)': field_names_score_pct,
        'Field Descriptions Score (%)': field_descriptions_score_pct,
        'Field Name Similarity Score (%)': field_name_similarity_score_pct,
        'Field Types Score (%)': field_types_score_pct,
        'Keys Presence Score (%)': keys_presence_score_pct,
        'Total Score (%)': total_score_pct,

        'Penalized Fields': penalized_fields
    }

    return detailed_scores

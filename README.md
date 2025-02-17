# Data Omni APIs

A lightweight Flask API designed to compute and return a score for your database schema, with optional override weights for various scoring criteria. This repository can help you evaluate how closely your database schema will be understood by your users and how easily it will work with Generative AI models.

---

<video width="600" controls>
  <source src="https://github.com/the-data-omni/data_omni_api/blob/demo.mp4" type="video/mp4">
  Your browser does not support the video tag.
</video>

[![Data Omni Scoring](https://raw.githubusercontent.com/the-data-omni/data_omni_api/main/.github/assets/data_omni_proto.gif)](https://youtu.be/iby_mqXOcbU)

## Features

- **Schema scoring**: Evaluates tables and columns based on:
  - Field names
  - Field descriptions
  - Field name similarity
  - Field types
  - Presence of primary/foreign keys
- **Customizable weights**: Override default weights for each criterion via the `weights_override` field.

---

## Repository Structure

```bash
schema_scoring_api/
├── app/
│   ├── __init__.py
│   ├── routes/
│   │   ├── biquery_routes.py
│   │   ├── queries_routes.py
│   │   ├── schema_scoring_routes.py
│   ├── services/
│   │   ├── biquery_service.py
│   │   ├── openai_service.py
│   │   ├── scoring_service.py
│   ├── utils/
│   │   └── logging_config.py
├── config,py
├── README
├── requirements.txt
└── run.py
```

- **`app/routes/schema_scoring_routes.py`**: Contains the Flask route(s) for handling requests.
- **`app/services/scoring_service.py`**: Implements the core logic of the scoring process.
- **`app/utils/logging_config.py`**: Provides a logging configuration.
- **`app/__init__.py`**: Initializes the Flask application.
- **`run.py`**: Entry point for running the Flask development server.
- **`requirements.txt`**: Lists all the Python dependencies needed.

---

## Installation

1. **Clone the repository**:

   ```bash
   git clone https://github.com/the-data-omni/data_omni_api.git
   cd data_omni_api
   ```

2. **(Recommended) Create a virtual environment**:

   ```bash
   # For Windows
   python -m venv venv
   venv\Scripts\activate

   # For macOS/Linux
   python3 -m venv venv
   source venv/bin/activate
   ```

3. **Install dependencies**:

   ```bash
   pip install -r requirements.txt
   ```

---

## Usage

1. **Run the Flask application**:

   ```bash
   python run.py
   ```
   
   By default, this will start the application on `http://127.0.0.1:5000`.

2. **To get schema - Bigquery**, upload your service account file with Read access to metadata and save it to the root folder. 

   If you do not want to connect to your bigquery instance, you can skip this step and get the schema manually in the format described in step 4

3. **Send a GET request -Bigquery** to the /bigquery_info endpoint. This will return the schema in the format required for the next step. 

    ```http://127.0.0.1:5000/bigquery_info```

4. **Send a POST request** with the following JSON structure to the /score_schema endpoint.  

   ```bash
   curl -X POST -H "Content-Type: application/json" \
   -d '{
         "schema": [
           {
            "table_catalog": "project_name",
            "table_schema": "dataset_name",
            "table_name": "table_name",
            "column_name": "column_name",
            "field_path": "field_path",
            "data_type": "STRING",
            "description": "example description",
            "collation_name": "NULL",
            "rounding_mode": null,
            "primary_key": boolean,
            "foreign_key": boolean
           },
           {
            "table_catalog": "project_name",
            "table_schema": "dataset_name",
            "table_name": "table_name",
            "column_name": "column_name",
            "field_path": "field_path",
            "data_type": "STRING",
            "description": "example description",
            "collation_name": "NULL",
            "rounding_mode": null,
            "primary_key": boolean,
            "foreign_key": boolean  
           }
         ],
         "weights_override": {
           "field_names": 10,
           "field_descriptions": 40,
           "field_name_similarity": 10,
           "field_types": 90,
           "keys_presence": 10
         }
       }' http://127.0.0.1:5000/score_schema
   ```

5. **Review the response**.

---

## Example JSON Payload

Below is the expected JSON payload format for scoring:

```json
{
    "Field Descriptions Score": 0.0,
    "Field Descriptions Score (%)": 0.0,
    "Field Name Similarity Score": 0.0,
    "Field Name Similarity Score (%)": 0.0,
    "Field Names Score": 0.0,
    "Field Names Score (%)": 0.0,
    "Field Types Score": 90.0,
    "Field Types Score (%)": 100.0,
    "Keys Presence Score": 2.5,
    "Keys Presence Score (%)": 25.0,
    "Penalized Fields": {
        "NonMeaningful": [
            "column_name"
        ],
        "NonMeaningful_NoDescription": [
            "column_name"
        ],
        "Similar_Undifferentiated": [
            "column_name"
        ]
    },
    "Total Score": 92.5,
    "Total Score (%)": 57.8125
}
```

- **`schema`** (required): A list of objects, each describing a table’s column.
- **`weights_override`** (optional): Provides custom weights to adjust scoring criteria weights are percentage weights adding up to 100.

---

## Contributing

Contributions are welcome! To contribute:

1. Fork this repository.
2. Create a new branch for your feature or bugfix.
3. Make your changes, then commit and push.
4. Submit a pull request describing your changes.

---

## Contact

For questions, suggestions, or feedback, please open an issue in the repository or reach out to the project maintainer(s).

---

**Happy Coding!**  
Feel free to file an issue for any bugs or feature requests. If you find this project useful, consider giving it a star on GitHub!

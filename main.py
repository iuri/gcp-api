from flask import Flask, request, jsonify, session, redirect, url_for
import os, logging
from dotenv import load_dotenv
from google.cloud import storage, bigquery
import json 
import time
import threading
from werkzeug.utils import secure_filename


import base64
from email.mime.text import MIMEText
from google.oauth2 import service_account
from googleapiclient.discovery import build




# from google.cloud import storage
app = Flask(__name__)

app.secret_key = '!@BRaC@DaBR@!'  # <- must be a strong string, not random every run


load_dotenv()

# Set environment variables
# GCS bucket configuration
GCS_BUCKET = os.environ.get("GCS_BUCKET_NAME")
PROJECT_ID = "lunavisionlabs" # Replace with your GCP project ID
BQ_DATASET = "computervision"  # Replace with your BigQuery dataset
BQ_TABLE = "faces"  # Replace with your BigQuery table


# Path to the credentials.json you downloaded
SERVICE_ACCOUNT_FILE = './lunavisionlabs-48f46e544921.json'
SCOPES = ['https://www.googleapis.com/auth/gmail.send']
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)


service = build('gmail', 'v1', credentials=credentials)


# Allowed Extensions
ALLOWED_EXTENSIONS = {'json'}

# Logging setup
logging.basicConfig(level=logging.INFO)

# Initialize Google Cloud Storage client
storage_client = storage.Client()


INPUT_FOLDER = "json"
OUTPUT_FOLDER = "processed"

def gcs_to_bigquery(filename):
    """Triggered by a new JSON file upload in GCS. Loads JSON into BigQuery."""
    
    print(f"Processing file: {filename} from bucket: {GCS_BUCKET}")

    # Read JSON file from GCS
    bucket = storage_client.bucket(GCS_BUCKET)
    blob_path = f"{os.path.join(INPUT_FOLDER,filename)}"
    blob = bucket.blob(blob_path)

    # Wait until file is available (max 10s)
    max_retries = 10
    for i in range(max_retries):
        if blob.exists():
            break
        print(f"File not found: {blob_path}, retrying... ({i+1}/{max_retries})")
        time.sleep(1)
    else:
        print(f"Error: File {blob_path} not found in bucket {bucket}")
        return
    
   
    bigquery_client = bigquery.Client()

    # Define BigQuery table reference
    table_ref = bigquery_client.dataset(BQ_DATASET).table(BQ_TABLE)

    # List files in the specified folder
    blobs = bucket.list_blobs(prefix='json')

    # Process each JSON file in the folder
    for blob in blobs:
        # print('blob', blob)

        if not blob.name.endswith(".json"):
            continue  # Skip non-JSON files
    
        # Read JSON file content
        json_data = json.loads(blob.download_as_text()) 

        # Extract face IDs from the JSON file
        face_ids = [face["id"] for face in json_data.get("faces", [])]

        if face_ids:
        
            # Query existing IDs in BigQuery
            query = f"""
                SELECT face.id FROM `{bigquery_client.project}.{BQ_DATASET}.{BQ_TABLE}`, UNNEST(faces) AS face
                WHERE face.id IN ({','.join([f"'{face_id}'" for face_id in face_ids])})
            """
            query_job = bigquery_client.query(query)
            existing_ids = {row["id"] for row in query_job.result()}  # Convert to set

            print('IDS', existing_ids)

            # Transform JSON into BigQuery-compatible format
            rows_to_insert = []
            # print("LEN", len(json_data.get("faces", [])))
            # print('faces', json_data.get("faces", []))
            for face in json_data.get("faces", []):
                if face["id"] not in existing_ids:
                
                    row = {
                        "creation_date": json_data["creation_date"],
                        "host": json_data["host"],
                        "filename": json_data["filename"],
                        "faces": [{
                            "id": face["id"],
                            "parent_face_id": json_data["parent_face_id"],
                            "person_id": json_data["person_id"],
                            "score": face["score"],
                            "attributes": {
                                "age": face["attributes"]["age"],
                                "eyeglasses": face["attributes"]["eyeglasses"],
                                "gender": face["attributes"]["gender"],
                                "emotions": {
                                    "estimations": {
                                        "anger": face["attributes"]["emotions"]["estimations"]["anger"],
                                        "disgust": face["attributes"]["emotions"]["estimations"]["disgust"],
                                        "fear": face["attributes"]["emotions"]["estimations"]["fear"],
                                        "happiness": face["attributes"]["emotions"]["estimations"]["happiness"],
                                        "neutral": face["attributes"]["emotions"]["estimations"]["neutral"],
                                        "sadness": face["attributes"]["emotions"]["estimations"]["sadness"],
                                        "surprise": face["attributes"]["emotions"]["estimations"]["surprise"]
                                    },
                                    "predominant_emotion": face["attributes"]["emotions"]["predominant_emotion"]
                                }
                            },
                            "rect": {
                                "height": face["rect"]["height"],
                                "width": face["rect"]["width"],
                                "x": face["rect"]["x"],
                                "y": face["rect"]["y"]
                            },
                            "rectISO": {
                                "height": face["rectISO"]["height"],
                                "width": face["rectISO"]["width"],
                                "x": face["rectISO"]["x"],
                                "y": face["rectISO"]["y"]
                            }
                        }]
                    }
                    rows_to_insert.append(row)
                else:
                    print(f"Deleting existing record: {face['id']}")
                    blob.delete()
            

            # Insert data into BigQuery
            if rows_to_insert:
                errors = bigquery_client.insert_rows_json(table_ref, rows_to_insert)
                if errors:
                    print(f"Errors inserting data from {blob.name}: {errors}")
                else:
                    print(f"Successfully inserted data from {blob.name}")
                    # Move file to processed folder
                    output_blob = OUTPUT_FOLDER + blob.name[len(INPUT_FOLDER):]
                    output_blob = bucket.rename_blob(blob, output_blob)
                    print(f"Moved {blob.name} to {output_blob.name}")


        else:
            print(f"No faces found in {blob.name}. Skipping...")












def save_to_gcs(json_data, filename):
    """Save JSON data to Google Cloud Storage."""
    bucket = storage_client.bucket(GCS_BUCKET)
    blob = bucket.blob(f"json/{filename}")

    # Convert JSON to string
    json_str = json.dumps(json_data, indent=4)

    # Upload JSON as a file
    blob.upload_from_string(json_str, content_type="application/json")

    return f"gs://{GCS_BUCKET}/json/{filename}"

# Create a helper function to check file extension
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

    
@app.route('/upload', methods=['POST'])
def upload():
    """Handles JSON file upload from HTTP POST request."""
   
    if request.method == 'POST':
        # Check if the post request has the file part
        if 'file' not in request.files:
            return jsonify({"error": "No file part"}), 400        

        file = request.files['file']
        # If no file is selected
        if file.filename == '':
            return jsonify({"error": "No selected file"}), 400

        if not allowed_file(file.filename):
            return jsonify({"error": "Please, upload JSON files only!"}), 400

        # If file is valid and has allowed extension
        # if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        
        print('filename', filename)
        # Read and validate JSON
        json_data = json.load(file)

        # Upload to GCS
        gcs_url = save_to_gcs(json_data, filename)

        # Trigger BigQuery ingestion in a separate thread
        thread = threading.Thread(target=gcs_to_bigquery, args=(filename,))
        thread.start()


        # gcs_to_bigquery(filename)
        return jsonify({"message": "File uploaded successfully", "gcs_url": gcs_url}), 201



def send_email1(json_data, filename):
    """Send email with JSON data."""
    print('filename', filename)
    print('json_data', json_data)

    return 

@app.route('/email', methods=['POST'])
def email():
    """Handles JSON file upload from HTTP POST request."""
   
    if request.method == 'POST':
        # Parse incoming JSON
        data = request.get_json(force=True)
        print("DATA", data)
        # Upload to GCS
        person_email = 'iuri.sampaio@omeletecompany.com'
        send_email(
            sender='lunavisionlabs@appspot.gserviceaccount.com', 
            to='iuri.sampaio@gmail.com', 
            subject="O&CO Vision: A person has been framed! ", 
            body="body"
        )

        return jsonify({"status": "Email sent to {}".format(person_email)}), 200






## Begin send Emails 

def create_message(sender, to, subject, message_text):
    message = MIMEText(message_text)
    message['to'] = to
    message['from'] = sender
    message['subject'] = subject
    return {'raw': base64.urlsafe_b64encode(message.as_bytes()).decode()}

def send_email(sender, to, subject, body):
    message = create_message(sender, to, subject, body)
    send_message = service.users().messages().send(userId="me", body=message).execute()
    print('Message Id: {}'.format(send_message['id']))
    return send_message


## END Sending Emails

















@app.route('/')
def home():
    return "ok", 200

if __name__ == '__main__':
    # app.run(host='0.0.0.0', port=5000, ssl_context=('cert.pem', 'key.pem'))
    # app.run(host='0.0.0.0', port=5000)
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 8080)))


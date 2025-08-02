import os
import pickle
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

SCOPES = ['https://www.googleapis.com/auth/drive.file']

def autenticar_drive():
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    return build('drive', 'v3', credentials=creds)

def subir_a_drive(nombre_archivo_local, nombre_en_drive):
    servicio = autenticar_drive()
    file_metadata = {'name': nombre_en_drive}
    media = MediaFileUpload(nombre_archivo_local, resumable=True)
    archivo = servicio.files().create(body=file_metadata, media_body=media, fields='id').execute()
    print(f"Archivo subido a Google Drive con ID: {archivo.get('id')}")

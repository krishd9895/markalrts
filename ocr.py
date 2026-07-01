
import io
import os
import json
import logging
import asyncio
from functools import partial

from google.oauth2.credentials import Credentials
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload, MediaIoBaseUpload

# The scope needed to upload, read, and delete files for OCR
SCOPES = ['https://www.googleapis.com/auth/drive']

# ── OCR-specific logger (file only, no terminal output) ──────────────────────
# The actual handler is attached by logs.py via setup_ocr_logger().
# We grab the named logger here; if logs.py hasn't run yet, messages
# will still be captured once the handler is attached.
ocr_logger = logging.getLogger("ocr_activity")


# ─────────────────────────────────────────────────────────────────────────────

async def get_credentials_from_db(db):
    """Retrieve Google Drive credentials and token from MongoDB.
    Handles both OAuth 2.0 installed app credentials and service account credentials.
    """
    creds_doc = await db["config"].find_one({"_id": "google_drive_creds"})
    if not creds_doc:
        return None, None
    
    # Check if it's a service account
    service_account_data = creds_doc.get("service_account")
    if service_account_data:
        creds = ServiceAccountCredentials.from_service_account_info(service_account_data, scopes=SCOPES)
        return creds, None
    
    # Otherwise, it's OAuth 2.0
    token_data = creds_doc.get("token")
    credentials_data = creds_doc.get("credentials")
    if not token_data:
        return None, credentials_data
    creds = Credentials.from_authorized_user_info(token_data, SCOPES)
    return creds, credentials_data


async def save_credentials_to_db(db, token_data, credentials_data=None):
    """Save Google Drive token and optionally credentials to MongoDB."""
    update_data = {"token": token_data, "already_notified_auth_issue": False}
    if credentials_data is not None:
        update_data["credentials"] = credentials_data
    await db["config"].update_one(
        {"_id": "google_drive_creds"},
        {"$set": update_data},
        upsert=True
    )


async def save_service_account_to_db(db, service_account_data):
    """Save Google Drive service account credentials to MongoDB."""
    await db["config"].update_one(
        {"_id": "google_drive_creds"},
        {"$set": {
            "service_account": service_account_data,
            "already_notified_auth_issue": False
        }},
        upsert=True
    )


def _sync_get_drive_service(creds):
    """Synchronous function to build Drive service."""
    return build('drive', 'v3', credentials=creds)


def _sync_image_to_text(service, image_data, image_name="Temp_OCR_File.jpg"):
    """Synchronous function to perform OCR (takes BytesIO or bytes)."""
    # Force Google Drive to convert the image into a Google Doc (triggers OCR)
    file_metadata = {
        'name': 'Temp_OCR_File',
        'mimeType': 'application/vnd.google-apps.document'
    }

    # Normalise to BytesIO
    if isinstance(image_data, (bytes, bytearray)):
        image_data = io.BytesIO(image_data)

    media = MediaIoBaseUpload(image_data, mimetype='image/jpeg', resumable=True)

    uploaded_file = service.files().create(
        body=file_metadata,
        media_body=media,
        fields='id'
    ).execute()

    file_id = uploaded_file.get('id')

    # Export as plain text from the generated Google Doc
    request = service.files().export_media(fileId=file_id, mimeType='text/plain')

    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()

    extracted_text = fh.getvalue().decode('utf-8')

    # Clean up the temporary Drive file
    service.files().delete(fileId=file_id).execute()

    return extracted_text


async def image_to_text(db, image_data, bot=None, owners=None, image_name="Temp_OCR_File.jpg"):
    """
    Async wrapper for OCR with DB-based credentials storage.
    Accepts BytesIO or bytes only — no disk reads or writes.
    Optionally sends messages to owners if auth fails (without spamming).
    """
    # Reject file paths — bytes only to avoid disk usage
    if isinstance(image_data, str):
        raise ValueError(
            "image_to_text() no longer accepts file paths. "
            "Pass image bytes or BytesIO instead."
        )
    if isinstance(image_data, (bytes, bytearray)):
        image_data = io.BytesIO(image_data)

    loop = asyncio.get_event_loop()

    creds, credentials_data = await get_credentials_from_db(db)

    # Check if we've already notified about auth issues
    google_creds_doc = await db["config"].find_one({"_id": "google_drive_creds"})
    already_notified = google_creds_doc.get("already_notified_auth_issue", False) if google_creds_doc else False

    # If we have valid credentials, reset the notification flag
    if creds and google_creds_doc and google_creds_doc.get("already_notified_auth_issue"):
        await db["config"].update_one(
            {"_id": "google_drive_creds"},
            {"$set": {"already_notified_auth_issue": False}}
        )
        already_notified = False

    if not creds:
        ocr_logger.warning("No Google Drive credentials found in DB. Authentication required.")
        if bot and owners and not already_notified:
            for owner in owners:
                await bot.send_message(
                    owner,
                    f"🔐 Google Drive authentication required.\n\n"
                    f"✅ **Recommended (No token refresh needed)**: Use a Service Account!\n"
                    f"1. Create a service account in Google Cloud Console\n"
                    f"2. Download its JSON key file\n"
                    f"3. Go to /settings → 📄 Google Drive Credentials → 📤 Upload Service Account Key (Recommended)\n"
                    f"4. Upload the JSON key file\n"
                    f"5. Share your Google Drive with the service account's email address (will be shown after upload)\n\n"
                    f"Or, for OAuth 2.0 (requires token refreshes):\n"
                    f"1. Upload your credentials.json via /settings → 📄 Google Drive Credentials → 📤 Upload OAuth credentials.json\n"
                    f"2. Generate a new token.json file locally by running the authentication flow\n"
                    f"3. Go to /settings → 📄 Google Drive Credentials → 📤 Upload OAuth token.json\n"
                    f"4. Upload your new token.json file"
                )
            # Mark that we've notified the user
            await db["config"].update_one(
                {"_id": "google_drive_creds"},
                {"$set": {"already_notified_auth_issue": True}},
                upsert=True
            )
        # If we have credentials.json locally, we can still try the local flow as a fallback
        if os.path.exists('credentials.json'):
            with open('credentials.json', 'r') as f:
                credentials_data = json.load(f)
            flow = InstalledAppFlow.from_client_config(credentials_data, SCOPES)
            creds = flow.run_local_server(port=0)
            token_data = json.loads(creds.to_json())
            await save_credentials_to_db(db, token_data, credentials_data)
            # Reset notification flag since we've fixed the issue
            await db["config"].update_one(
                {"_id": "google_drive_creds"},
                {"$set": {"already_notified_auth_issue": False}}
            )
            ocr_logger.info("New credentials obtained and saved to DB.")
        elif credentials_data is None:
            raise Exception("No Google Drive credentials found. Please upload a service account key or OAuth credentials via the bot's /settings menu.")

    elif not creds.valid:
        if creds.expired and creds.refresh_token:
            try:
                ocr_logger.info("Access token expired. Refreshing...")
                creds.refresh(Request())
                token_data = json.loads(creds.to_json())
                await save_credentials_to_db(db, token_data)
                # Reset notification flag since we've fixed the issue
                await db["config"].update_one(
                    {"_id": "google_drive_creds"},
                    {"$set": {"already_notified_auth_issue": False}}
                )
                ocr_logger.info("Token refreshed successfully.")
            except Exception as e:
                ocr_logger.error(f"Token refresh failed: {e}")
                if bot and owners and not already_notified:
                    for owner in owners:
                        await bot.send_message(
                            owner,
                            f"🔐 Google Drive token refresh failed. Error: {e}\n\n"
                            f"✅ **Recommended fix (No more token refreshes!)**: Switch to a Service Account!\n"
                            f"1. Create a service account in Google Cloud Console\n"
                            f"2. Download its JSON key file\n"
                            f"3. Go to /settings → 📄 Google Drive Credentials → 📤 Upload Service Account Key (Recommended)\n"
                            f"4. Upload the JSON key file\n"
                            f"5. Share your Google Drive with the service account's email address (will be shown after upload)\n\n"
                            f"Or, to continue using OAuth 2.0:\n"
                            f"1. Generate a new token.json file locally by running the authentication flow\n"
                            f"2. Go to /settings → 📄 Google Drive Credentials → 📤 Upload OAuth token.json\n"
                            f"3. Upload your new token.json file"
                        )
                    # Mark that we've notified the user
                    await db["config"].update_one(
                        {"_id": "google_drive_creds"},
                        {"$set": {"already_notified_auth_issue": True}},
                        upsert=True
                    )
                raise e
        else:
            ocr_logger.warning("Credentials invalid and no refresh token. Re-authenticating...")
            if bot and owners and not already_notified:
                for owner in owners:
                    await bot.send_message(
                        owner,
                        f"🔐 Google Drive authentication required.\n\n"
                        f"✅ **Recommended (No token refresh needed)**: Use a Service Account!\n"
                        f"1. Create a service account in Google Cloud Console\n"
                        f"2. Download its JSON key file\n"
                        f"3. Go to /settings → 📄 Google Drive Credentials → 📤 Upload Service Account Key (Recommended)\n"
                        f"4. Upload the JSON key file\n"
                        f"5. Share your Google Drive with the service account's email address (will be shown after upload)\n\n"
                        f"Or, for OAuth 2.0:\n"
                        f"1. Make sure you have uploaded credentials.json via /settings → 📄 Google Drive Credentials → 📤 Upload OAuth credentials.json\n"
                        f"2. Generate a new token.json file locally by running the authentication flow\n"
                        f"3. Go to /settings → 📄 Google Drive Credentials → 📤 Upload OAuth token.json\n"
                        f"4. Upload your new token.json file"
                    )
                # Mark that we've notified the user
                await db["config"].update_one(
                    {"_id": "google_drive_creds"},
                    {"$set": {"already_notified_auth_issue": True}},
                    upsert=True
                )
            # If we have credentials.json locally, we can still try the local flow as a fallback
            if os.path.exists('credentials.json'):
                with open('credentials.json', 'r') as f:
                    credentials_data = json.load(f)
                flow = InstalledAppFlow.from_client_config(credentials_data, SCOPES)
                creds = flow.run_local_server(port=0)
                token_data = json.loads(creds.to_json())
                await save_credentials_to_db(db, token_data, credentials_data)
                # Reset notification flag since we've fixed the issue
                await db["config"].update_one(
                    {"_id": "google_drive_creds"},
                    {"$set": {"already_notified_auth_issue": False}}
                )
                ocr_logger.info("Re-authentication complete. New credentials saved.")
            else:
                raise Exception("No credentials.json found locally and no token available. Please upload credentials.json and token.json via the bot's /settings menu.")

    service = await loop.run_in_executor(None, _sync_get_drive_service, creds)
    extracted_text = await loop.run_in_executor(
        None, partial(_sync_image_to_text, service, image_data, image_name)
    )
    return extracted_text


# ── Legacy synchronous helpers (kept for backwards compatibility) ─────────────

def get_drive_service():
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    return build('drive', 'v3', credentials=creds)


def image_to_text_sync(image_path):
    if not os.path.exists(image_path):
        raise FileNotFoundError(f"Could not find the image file: {image_path}")

    service = get_drive_service()
    ocr_logger.info(f"[sync] Uploading '{image_path}' to Google Drive...")

    file_metadata = {
        'name': 'Temp_OCR_File',
        'mimeType': 'application/vnd.google-apps.document'
    }

    media = MediaFileUpload(image_path, mimetype='image/jpeg', resumable=True)

    uploaded_file = service.files().create(
        body=file_metadata,
        media_body=media,
        fields='id'
    ).execute()

    file_id = uploaded_file.get('id')
    ocr_logger.info(f"[sync] Processing OCR (file_id={file_id})...")

    request = service.files().export_media(fileId=file_id, mimeType='text/plain')

    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()

    extracted_text = fh.getvalue().decode('utf-8')

    service.files().delete(fileId=file_id).execute()
    ocr_logger.info("[sync] OCR complete. Temporary Drive file deleted.")

    return extracted_text


if __name__ == '__main__':
    TARGET_IMAGE = 'receipt.jpg'
    try:
        text = image_to_text_sync(TARGET_IMAGE)
        ocr_logger.info("\n--- EXTRACTED TEXT ---")
        ocr_logger.info(text)
        ocr_logger.info("----------------------")
    except Exception as e:
        ocr_logger.error(f"Error: {e}")

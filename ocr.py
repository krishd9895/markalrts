import io
import os
import json
import logging
import asyncio
from functools import partial
from PIL import Image

from google.oauth2.credentials import Credentials
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload, MediaIoBaseUpload

# Try to import EasyOCR for fallback
try:
    import easyocr
    EASYOCR_AVAILABLE = True
    # Initialize reader once at module level for efficiency
    EASYOCR_READER = None
except ImportError:
    EASYOCR_AVAILABLE = False
    EASYOCR_READER = None

# The scope needed to upload, read, and delete files for OCR
SCOPES = ['https://www.googleapis.com/auth/drive']

# ── Loggers ──────────────────────
ocr_logger = logging.getLogger("ocr_activity")
bot_activity_logger = logging.getLogger("bot_activity")


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
    return build('drive', 'v3', credentials=creds, cache_discovery=False)


def _sync_test_credentials(creds):
    """Synchronous function to test Google Drive credentials by listing files."""
    # Silence file_cache warning by disabling cache
    service = build('drive', 'v3', credentials=creds, cache_discovery=False)
    try:
        # Try to list first 5 files to verify connection works
        results = service.files().list(pageSize=5, fields="files(id, name)").execute()
        return True, results.get('files', [])
    except Exception as e:
        raise e


def _sync_image_to_text(service, image_data, image_name="Temp_OCR_File.jpg"):
    """Synchronous function to perform OCR (takes BytesIO or bytes)."""
    # Force Google Drive to convert the image to a Google Doc (triggers OCR)
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

    try:
        # Export as plain text from the generated Google Doc
        request = service.files().export_media(fileId=file_id, mimeType='text/plain')

        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()

        extracted_text = fh.getvalue().decode('utf-8')
    finally:
        # Always clean up the temporary Drive file, even if OCR fails
        try:
            service.files().delete(fileId=file_id).execute()
        except Exception as e:
            # Don't fail the whole OCR just because cleanup failed
            pass

    return extracted_text


def _sync_cleanup_old_temp_files(service):
    """Synchronous function to delete old Temp_OCR_File files from Drive."""
    try:
        # List all files named Temp_OCR_File
        results = service.files().list(
            q="name='Temp_OCR_File'",
            fields="files(id, name)",
            pageSize=100
        ).execute()
        
        files = results.get('files', [])
        deleted_count = 0
        
        for file in files:
            try:
                service.files().delete(fileId=file['id']).execute()
                deleted_count += 1
            except Exception as e:
                pass  # Skip files that can't be deleted
                
        return deleted_count
    except Exception as e:
        return 0


def _sync_easyocr_image_to_text(image_data):
    """Synchronous function to perform OCR using EasyOCR as fallback."""
    global EASYOCR_READER
    
    if not EASYOCR_AVAILABLE:
        raise ImportError("EasyOCR is not installed")
    
    # Initialize reader if not already initialized
    if EASYOCR_READER is None:
        # Use English by default, can be expanded to other languages
        EASYOCR_READER = easyocr.Reader(['en'], gpu=False)  # GPU=False for Docker compatibility
    
    # Load image from BytesIO
    image = Image.open(image_data)
    
    # Perform OCR
    results = EASYOCR_READER.readtext(image, detail=0)
    
    # Join results into a single string
    extracted_text = '\n'.join(results)
    
    return extracted_text


async def test_google_credentials(db):
    """Test Google Drive credentials (service account first, then OAuth). Returns (success: bool, message: str, details: dict)"""
    from functools import partial
    import asyncio
    loop = asyncio.get_event_loop()
    
    # Get what's stored in DB
    creds_doc = await db["config"].find_one({"_id": "google_drive_creds"})
    
    if not creds_doc:
        details = {
            "service_account": None,
            "oauth_credentials": None,
            "oauth_token": None
        }
        return False, "❌ No Google Drive credentials stored in the database.", details
    
    details = {
        "service_account": creds_doc.get("service_account") is not None,
        "oauth_credentials": creds_doc.get("credentials") is not None,
        "oauth_token": creds_doc.get("token") is not None
    }
    
    creds, _ = await get_credentials_from_db(db)
    
    if not creds:
        if details["service_account"]:
            return False, "❌ Service account found but failed to load.", details
        elif details["oauth_credentials"] and not details["oauth_token"]:
            return False, "❌ OAuth credentials found but no token present.", details
        elif details["oauth_token"]:
            return False, "❌ OAuth token found but failed to load.", details
        else:
            return False, "❌ No valid Google Drive credentials found in the database.", details
    
    try:
        # First, build the service
        service = await loop.run_in_executor(None, _sync_get_drive_service, creds)
        
        # Clean up old temp files
        deleted_count = await loop.run_in_executor(None, _sync_cleanup_old_temp_files, service)
        
        # Test credentials
        success, files = await loop.run_in_executor(None, _sync_test_credentials, creds)
        if success:
            cred_type = "Service Account" if details["service_account"] else "OAuth 2.0"
            cleanup_msg = f" Cleaned up {deleted_count} old temp file(s)." if deleted_count > 0 else ""
            return True, f"✅ Google Drive {cred_type} credentials are working correctly! Successfully listed {len(files)} file(s) from your Drive.{cleanup_msg}", details
    except Exception as e:
        return False, f"❌ Google Drive credentials test failed: {str(e)}", details
    
    return False, "❌ Failed to verify credentials.", details


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
    
    # Make a copy of image data for possible fallback (since BytesIO is read-once)
    image_bytes = None
    if isinstance(image_data, (bytes, bytearray)):
        image_bytes = image_data
        image_data = io.BytesIO(image_data)
    else:
        # If it's already a BytesIO, reset it and make a copy
        image_data.seek(0)
        image_bytes = image_data.read()
        image_data = io.BytesIO(image_bytes)

    loop = asyncio.get_event_loop()

    creds, credentials_data = await get_credentials_from_db(db)
    
    # Check what type of credentials we have
    google_creds_doc = await db["config"].find_one({"_id": "google_drive_creds"})
    is_service_account = google_creds_doc and google_creds_doc.get("service_account") is not None
    
    # Check if we've already notified about auth issues
    already_notified = google_creds_doc.get("already_notified_auth_issue", False) if google_creds_doc else False

    # If we have valid credentials, reset the notification flag
    if creds and google_creds_doc and google_creds_doc.get("already_notified_auth_issue"):
        await db["config"].update_one(
            {"_id": "google_drive_creds"},
            {"$set": {"already_notified_auth_issue": False}}
        )
        already_notified = False

    use_drive_api = True
    drive_error = None
    
    if not creds:
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
                    f"5. Share your Google Drive with the service account email (found in the JSON file)\n\n"
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
        else:
            use_drive_api = False

    elif not is_service_account and not creds.valid:
        # Only check validity and refresh for OAuth credentials, not service accounts
        if creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                token_data = json.loads(creds.to_json())
                await save_credentials_to_db(db, token_data)
                # Reset notification flag since we've fixed the issue
                await db["config"].update_one(
                    {"_id": "google_drive_creds"},
                    {"$set": {"already_notified_auth_issue": False}}
                )
            except Exception as e:
                bot_activity_logger.error(f"Google Drive token refresh failed: {e}")
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
                            f"5. Share your Google Drive with the service account email (found in the JSON file)\n\n"
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
                use_drive_api = False
        else:
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
                        f"5. Share your Google Drive with the service account email (found in the JSON file)\n\n"
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
            else:
                use_drive_api = False

    # Try Drive API first
    extracted_text = None
    if use_drive_api:
        try:
            # Reset image data pointer
            image_data.seek(0)
            service = await loop.run_in_executor(None, _sync_get_drive_service, creds)
            extracted_text = await loop.run_in_executor(
                None, partial(_sync_image_to_text, service, image_data, image_name)
            )
            return extracted_text
        except Exception as e:
            drive_error = str(e)
            bot_activity_logger.warning(f"Google Drive API failed: {e}")
            # Fall through to EasyOCR
    
    # Try EasyOCR fallback
    if EASYOCR_AVAILABLE:
        try:
            image_data_fallback = io.BytesIO(image_bytes)
            extracted_text = await loop.run_in_executor(
                None, _sync_easyocr_image_to_text, image_data_fallback
            )
            return extracted_text
        except Exception as e:
            bot_activity_logger.error(f"EasyOCR fallback failed: {e}")
            raise Exception(f"Both Google Drive API and EasyOCR failed. Drive error: {drive_error if use_drive_api else 'No credentials'}, EasyOCR error: {str(e)}")
    else:
        raise Exception("Google Drive API failed and EasyOCR is not installed as fallback.")


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

    request = service.files().export_media(fileId=file_id, mimeType='text/plain')

    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()

    extracted_text = fh.getvalue().decode('utf-8')

    service.files().delete(fileId=file_id).execute()

    return extracted_text


if __name__ == '__main__':
    TARGET_IMAGE = 'receipt.jpg'
    try:
        text = image_to_text_sync(TARGET_IMAGE)
        print("\n--- EXTRACTED TEXT ---")
        print(text)
        print("----------------------")
    except Exception as e:
        print(f"Error: {e}")

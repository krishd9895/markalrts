import io
import os
import logging
import asyncio
from functools import partial
import httpx

# Import config
from config import EXTERNAL_OCR_SERVICE_URL, DEFAULT_OCR_METHOD, INSTALL_TESSERACT

# ── OPTIONAL LOCAL OCR STACK ────────────────────────────────────────────────
# Pillow and Tesseract (pytesseract) are optional. On low-resource machines,
# or when the system package tesseract-ocr is not installed, these imports
# simply fail and we fall back to an external OCR service instead of crashing
# the whole bot.

# Both Pillow and Tesseract are only imported when INSTALL_TESSERACT is enabled.
# There is no point loading Pillow if Tesseract will never be used — it just
# wastes memory and storage on resource-constrained machines.
PIL_AVAILABLE = False
TESSERACT_AVAILABLE = False
Image = None  # keep the name available so the rest of the module can reference it safely

if INSTALL_TESSERACT:
    # Try to import Pillow first -- needed to open images for local OCR
    try:
        from PIL import Image
        PIL_AVAILABLE = True
    except ImportError:
        PIL_AVAILABLE = False

    # Try to import Tesseract -- only useful when Pillow is also available
    if PIL_AVAILABLE:
        try:
            import pytesseract
            TESSERACT_AVAILABLE = True
        except ImportError:
            TESSERACT_AVAILABLE = False

# ── Loggers ──────────────────────
ocr_logger = logging.getLogger("ocr_activity")
bot_activity_logger = logging.getLogger("bot_activity")

if not TESSERACT_AVAILABLE:
    if INSTALL_TESSERACT:
        ocr_logger.warning(
            "Tesseract is not available (pytesseract or Pillow failed to import, "
            "or the system package tesseract-ocr is not installed). "
            "Falling back to an external OCR service (if configured)."
        )
    else:
        ocr_logger.info(
            "Tesseract is disabled via INSTALL_TESSERACT=false. "
            "Using external OCR service."
        )


# ─────────────────────────────────────────────────────────────────────────────

def _sync_tesseract_image_to_text(image_data):
    """Synchronous function to perform OCR using Tesseract (lightweight default)."""
    if not TESSERACT_AVAILABLE:
        raise ImportError("Tesseract (pytesseract) is not installed")
    
    # Load image from BytesIO
    image = Image.open(image_data)
    
    # Perform OCR
    extracted_text = pytesseract.image_to_string(image)
    
    return extracted_text


def clean_ocr_text(text):
    """Clean OCR text specifically for stock market alerts with aggressive noise removal"""
    import re
    
    if not text:
        return ""
    
    cleaned = text
    
    # Step 1: Normalize all whitespace
    cleaned = re.sub(r'\r\n', '\n', cleaned)  # Normalize newlines
    cleaned = re.sub(r'\r', '\n', cleaned)
    cleaned = re.sub(r'[ \t]+', ' ', cleaned)  # Replace multiple spaces/tabs with single
    cleaned = re.sub(r'\n{3,}', '\n\n', cleaned)  # Replace 3+ newlines with 2
    
    # Step 2: Remove common OCR garbage characters
    garbage_chars = r'[^\x20-\x7E]'  # Remove non-ASCII characters
    cleaned = re.sub(garbage_chars, '', cleaned)
    
    # Step 3: Remove lines that are mostly garbage (less than 30% alphanumeric)
    lines = cleaned.split('\n')
    cleaned_lines = []
    for line in lines:
        line = line.strip()
        if line:
            alnum_count = sum(1 for c in line if c.isalnum())
            if alnum_count >= 0.3 * len(line):  # Keep line if at least 30% alphanumeric
                cleaned_lines.append(line)
    
    cleaned = '\n'.join(cleaned_lines)
    
    # Step 4: Final character whitelist for stock market
    # Keep: letters, numbers, common symbols ($,%,.,,, -, (), [], {}, :, ;, +, =, @, #, &, *, /, \, |)
    cleaned = re.sub(r'[^a-zA-Z0-9\s\n\-\$\%\.\,\(\)\[\]\{\}\:\;\+\=\@\#\&\*\/\\\|]', '', cleaned)
    
    # Step 5: Trim final whitespace
    cleaned = re.sub(r'^\s+|\s+$', '', cleaned)
    cleaned = re.sub(r'[ \t]+', ' ', cleaned)
    cleaned = re.sub(r'\n{2,}', '\n\n', cleaned)
    
    return cleaned


async def _sync_external_service_image_to_text(image_bytes):
    """Async function to perform OCR using external service (REST API)."""
    if not EXTERNAL_OCR_SERVICE_URL:
        raise ValueError("External OCR service URL is not configured")
    
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            files = {'file': ('image.jpg', image_bytes)}  # Use 'file' key as expected by most external OCR services
            data = {'lang': 'eng'}  # Add language parameter
            response = await client.post(
                EXTERNAL_OCR_SERVICE_URL,
                files=files,
                data=data
            )
            response.raise_for_status()
            
            # First try to parse as JSON
            try:
                result = response.json()
                if isinstance(result, dict):
                    if 'text' in result:
                        return result['text']
                    if 'data' in result and isinstance(result['data'], dict) and 'text' in result['data']:
                        return result['data']['text']
            except:
                pass  # If JSON parsing fails, treat as raw text
            
            # Fallback: return raw text response (your service returns raw text)
            return response.text
    except Exception as e:
        raise RuntimeError(f"External OCR service failed: {str(e)}")


async def image_to_text(db, image_data, bot=None, owners=None, image_name="Temp_OCR_File.jpg"):
    """Async wrapper for OCR with multiple fallbacks.

    The method attempted first is controlled by DEFAULT_OCR_METHOD (env var).
    The remaining methods are tried as fallbacks in this fixed order:
      external → tesseract
    """
    # Reject file paths — bytes only to avoid disk usage
    if isinstance(image_data, str):
        raise ValueError("image_to_text() no longer accepts file paths. Pass image bytes or BytesIO instead.")
    
    # Make a copy of image data for fallbacks (since BytesIO is read-once)
    image_bytes = None
    if isinstance(image_data, (bytes, bytearray)):
        image_bytes = image_data
        image_data = io.BytesIO(image_data)
    else:
        image_data.seek(0)
        image_bytes = image_data.read()
        image_data = io.BytesIO(image_bytes)
    
    loop = asyncio.get_event_loop()

    # ── Build the ordered fallback list from DEFAULT_OCR_METHOD ──────────────
    # The preferred method goes first; remaining methods follow in their fixed
    # order: external → tesseract.
    _ALL_METHODS = ["external", "tesseract"]
    _preferred = DEFAULT_OCR_METHOD.strip().lower()
    if _preferred not in _ALL_METHODS:
        bot_activity_logger.warning(
            f"Unknown DEFAULT_OCR_METHOD '{_preferred}'. "
            f"Falling back to default order: {_ALL_METHODS}"
        )
        _preferred = _ALL_METHODS[0]
    _ordered_methods = [_preferred] + [m for m in _ALL_METHODS if m != _preferred]
    bot_activity_logger.info(f"OCR method order: {_ordered_methods}")

    # ── OCR log: record every image processed ─────────────────────────────────
    ocr_logger.info("=" * 60)
    ocr_logger.info(f"[OCR START] file={image_name}  size={len(image_bytes):,} bytes  order={_ordered_methods}")

    extracted_text = None

    # ── Run through each method in priority order ────────────────────────────
    for _method in _ordered_methods:

        if extracted_text:
            break

        if _method == "external":
            if not EXTERNAL_OCR_SERVICE_URL:
                bot_activity_logger.debug("Skipping external OCR: EXTERNAL_OCR_SERVICE_URL not set.")
                ocr_logger.info(f"[SKIP] external — EXTERNAL_OCR_SERVICE_URL not set")
                continue
            try:
                ocr_logger.info(f"[TRY] external — {EXTERNAL_OCR_SERVICE_URL}")
                bot_activity_logger.info(f"Trying external OCR service at {EXTERNAL_OCR_SERVICE_URL}")
                extracted_text = await _sync_external_service_image_to_text(image_bytes)
                ocr_logger.info(f"[OK]  external — {len(extracted_text):,} chars extracted")
                bot_activity_logger.info("Successfully extracted text using external OCR service")
            except Exception as e:
                ocr_logger.info(f"[FAIL] external — {e}")
                bot_activity_logger.warning(f"External OCR failed: {e}")

        elif _method == "tesseract":
            if not TESSERACT_AVAILABLE:
                bot_activity_logger.debug(
                    "Skipping Tesseract OCR: not available "
                    "(INSTALL_TESSERACT=false or pytesseract/tesseract-ocr not installed)."
                )
                ocr_logger.info(f"[SKIP] tesseract — not available (INSTALL_TESSERACT=false or missing package)")
                continue
            try:
                ocr_logger.info(f"[TRY] tesseract")
                image_data_fallback = io.BytesIO(image_bytes)
                extracted_text = await loop.run_in_executor(
                    None, _sync_tesseract_image_to_text, image_data_fallback
                )
                ocr_logger.info(f"[OK]  tesseract — {len(extracted_text):,} chars extracted")
                bot_activity_logger.info("Successfully extracted text using Tesseract OCR")
            except Exception as e:
                ocr_logger.info(f"[FAIL] tesseract — {e}")
                bot_activity_logger.error(f"Tesseract failed: {e}")

    if not extracted_text:
        ocr_logger.info(f"[RESULT] ALL METHODS FAILED — returning empty string")
        bot_activity_logger.warning(
            "All OCR methods failed or were skipped — returning empty text silently."
        )
        return ""

    # Clean the text for stock market alerts
    cleaned_text = clean_ocr_text(extracted_text)
    ocr_logger.info(f"[RESULT] clean_len={len(cleaned_text):,}  preview={cleaned_text[:120]!r}")
    return cleaned_text

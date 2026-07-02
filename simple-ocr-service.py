from flask import Flask, request, jsonify
from PIL import Image
import pytesseract
import io
import os
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

@app.route('/api/ocr', methods=['POST'])
def ocr():
    image_file = None
    if 'image' in request.files:
        image_file = request.files['image']
    elif 'file' in request.files:
        image_file = request.files['file']
    else:
        return jsonify({'error': 'No image file provided'}), 400
    try:
        img = Image.open(io.BytesIO(image_file.read()))
        text = pytesseract.image_to_string(img)
        return jsonify({'text': text})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    port = int(os.getenv('PORT', 8181))
    app.run(host='0.0.0.0', port=port)

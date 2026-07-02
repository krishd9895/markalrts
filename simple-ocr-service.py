from flask import Flask, request, jsonify
from PIL import Image
import pytesseract
import io

app = Flask(__name__)

@app.route('/ocr', methods=['POST'])
def ocr():
    if 'image' not in request.files:
        return jsonify({'error': 'No image file provided'}), 400
    
    image_file = request.files['image']
    try:
        img = Image.open(io.BytesIO(image_file.read()))
        text = pytesseract.image_to_string(img)
        return jsonify({'text': text})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8181)

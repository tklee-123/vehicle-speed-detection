import pymongo
import base64
from io import BytesIO
from PIL import Image

client = pymongo.MongoClient("mongodb://localhost:27017")
db = client["car_speed"]
collection = db["60-80"]

cursor = collection.find({})

# Lặp qua từng bản ghi
for document in cursor:
    # Lấy dữ liệu ảnh từ trường image_base64
    image_base64 = document.get("image_base64", "")
    
    # Giải mã ảnh từ base64
    image_data = base64.b64decode(image_base64)
    
    # Hiển thị ảnh (ở đây sử dụng thư viện Pillow)
    image = Image.open(BytesIO(image_data))
    image.show()

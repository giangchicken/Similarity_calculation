#!/bin/bash

# Tạo và kích hoạt môi trường ảo
python3 -m venv venv
source venv/bin/activate

sudo apt install -y python3.10 python3.10-venv python3.10-dev python3-pip

# Đảm bảo pip được cập nhật
pip install --upgrade pip

# Hiển thị phiên bản Python và pip để kiểm tra
python --version
pip --version

echo "Môi trường ảo đã được thiết lập thành công!"

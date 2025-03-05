#!/bin/bash

# Cập nhật hệ thống
sudo apt update -y && sudo apt upgrade -y

# Cài đặt Git
sudo apt install -y git

# Clone repository
git clone https://github.com/giangchicken/Similarity_calculation.git


# Cài đặt Python 3.10 và các gói cần thiết
sudo apt install -y python3.10 python3.10-venv python3.10-dev python3-pip

# Tạo và kích hoạt môi trường ảo
python3.10 -m venv venv
source venv/bin/activate

# Di chuyển vào thư mục của project
cd Similarity_calculation || { echo "Clone thất bại!"; exit 1; }

# Đảm bảo pip được cập nhật
pip install --upgrade pip

# Cài đặt các thư viện cần thiết từ requirements.txt
if [ -f "requirements.txt" ]; then
    pip install -r requirements.txt
else
    echo "File requirements.txt không tồn tại!"
fi

# Hiển thị phiên bản Python và pip để kiểm tra
python --version
pip --version

echo "Môi trường ảo đã được thiết lập thành công!"

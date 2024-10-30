# !/usr/bin/env python
# -*-coding:utf-8 -*-

"""
# File       : run_parallels.py
# Time       ：2024/10/22 16:32
# Author     ：jianbang
# version    ：python 3.10
# company    : IFLYTEK Co.,Ltd.
# emil       : whdx072018@foxmail.com
# Description：
"""
from src import run_parallels
import os
from datetime import datetime


if __name__ == '__main__':
    size = 0.5 * 1024 * 1024 * 1024  # 1GB
    tmp_dir = datetime.now().strftime('%Y%m%d%H%M%S')
    upload_dir = os.path.join("datasets", tmp_dir)
    os.makedirs(upload_dir, exist_ok=True)
    log_file = r"/Users/whu/Desktop/all/datasets/model0712.log"
    code,file_path_list = run_parallels(log_file,upload_dir,size)
    if code!=0:
        print("处理失败")
    else:
        print("处理成功")
        print(file_path_list)

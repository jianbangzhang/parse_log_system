# !/usr/bin/env python
# -*-coding:utf-8 -*-

"""
# File       : data_api.py
# Time       ：2024/7/10 07:42
# Author     ：jianbang
# version    ：python 3.10
# company    : IFLYTEK Co.,Ltd.
# emil       : whdx072018@foxmail.com
# Description：
"""

import os
from fastapi import FastAPI, APIRouter
import gradio as gr



class DataApi:
    def __init__(self, infer, demo: gr.Blocks, app: FastAPI):

        print(f'api create success {infer}')

        self.router = APIRouter()
        self.app = app
        self.demo = demo
        self.status = "start"
        self.Infer = infer
        self.app.add_api_route("/ping", self.ping, methods=["GET"])
        self.app.add_api_route("/restartall", self.restartall, methods=["GET"])
        self.app.add_api_route("/stop", self.stop, methods=["GET"])

    def ping(self):
        return {"ping": "pong"}

    def stop(self):
        self.status = "stop"
        return {}

    def restartall(self):
        self.status = "stop"
        os.system("pm2 restart all")
        return {}

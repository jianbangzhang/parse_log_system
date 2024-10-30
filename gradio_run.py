# !/usr/bin/env python
# -*-coding:utf-8 -*-

"""
# File       : gradio_run.py
# Time       ：2024/7/10 07:36
# Author     ：jianbang
# version    ：python 3.10
# company    : IFLYTEK Co.,Ltd.
# emil       : whdx072018@foxmail.com
# Description：
"""
import os
import shutil
import gradio as gr
from argparse import ArgumentParser
from fastapi import FastAPI
import time
from datetime import datetime
from src.data_api import DataApi
from src.data_infer import DataInfer
from src import run_parallels


parser = ArgumentParser()
parser.add_argument("--port", type=int, default=9003, help="port")
args, unknown_args = parser.parse_known_args()

Infer = DataInfer()


async def process_log(file,size):
    start=time.time()
    current_file_path = os.path.abspath('__file__')
    project_path = os.path.dirname(current_file_path)
    date_dir = datetime.now().strftime('%Y%m%d%H%M%S')
    upload_dir = os.path.join(project_path,"datasets",date_dir)
    os.makedirs(upload_dir,exist_ok=True)

    shutil.copy(file.name, upload_dir)

    fileName = os.path.basename(file.name)
    log_file_Path = os.path.join(upload_dir, fileName)
    print(log_file_Path)

    size = float(size) * 1024 * 1024 * 1024  # 1GB
    code, file_path_list = run_parallels(log_file_Path, upload_dir, size)

    if code != 0:
        status="数据处理失败!"
        return None,None,status
    else:
        status=f"用时{round((time.time()-start)/60,2)}分钟,数据处理成功！"
        return file_path_list[0][0],file_path_list[0][-1],status

def log_demo():
    with gr.Blocks(analytics_enabled=False) as log_interface:
        with gr.Row():
            gr.HTML('<center style="font-size: 20px;"><b> 数据处理 </b></center>')
        with gr.Row():
            gr.HTML('<p style="font-size: 15px; text-align: right;">作者：jianbang</p>')

        with gr.Tab("日志数据解析"):
            with gr.Row().style(equal_height=False):
                with gr.Column(variant='panel'):
                    log_file = gr.File(label="上传 .log/.zip文件", file_types=['log','zip'])
                    file_size = gr.Slider(minimum=0.02, maximum=1, value=0.5, label="设置拆分文件大小阈值 (GB)",step=0.01)
                    process_button = gr.Button('处理数据', variant='primary',
                                               style={'width': '150px', 'height': '50px'})

                    download_1 = gr.File(label="下载 Agent 文件")
                    download_2 = gr.File(label="下载 Intent 文件")
                    process_status = gr.Textbox(label="处理状态")

                    async def handle_process_log(file,file_size):
                        agent_path, intent_path, status = await process_log(file,file_size)
                        return agent_path, intent_path, status

            process_button.click(
                fn=handle_process_log,
                inputs=[log_file,file_size],
                outputs=[download_1, download_2, process_status]
            )

        return log_interface


def wait_on_server(api: DataApi):
    while 1:
        if api.status == "stop":
            break
        time.sleep(1)


# pm2 start app_gradio.py --name agent --interpreter python3  -- --port=8100
if __name__ == "__main__":
    demo = log_demo()
    demo.queue(max_size=10, api_open=True)

    if not hasattr(FastAPI, 'original_setup'):
        def fastapi_setup(self):
            self.docs_url = "/docs"
            self.redoc_url = "/redoc"
            self.original_setup()


        FastAPI.original_setup = FastAPI.setup
        FastAPI.setup = fastapi_setup

    app, local_url, share_url = demo.launch(debug=False,
                                            server_name="0.0.0.0",
                                            server_port=args.port,
                                            prevent_thread_lock=True,
                                            show_error=True,
                                            show_tips=True,
                                            quiet=False,
                                            show_api=True)
    print(f'demo app')

    api = DataApi(Infer, demo, app)
    print(f'create api success')
    wait_on_server(api)
    demo.close()


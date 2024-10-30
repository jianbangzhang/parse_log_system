# !/usr/bin/env python
# -*-coding:utf-8 -*-

"""
# File       : data_utils.py
# Time       ：2024/7/11 16:44
# Author     ：jianbang
# version    ：python 3.10
# company    : IFLYTEK Co.,Ltd.
# emil       : whdx072018@foxmail.com
# Description：
"""
import os
import shutil
import json
import chardet



def check_and_write_complete_conversation(key, end_mark, logs_dict, line_positions, output_file):
    if end_mark in logs_dict[key][-1] and len(logs_dict[key]) >= 2:
        with open(output_file, 'a', encoding='utf-8') as out_file:
            for line in logs_dict[key]:
                out_file.write(line + "\n")
        del logs_dict[key]
        del line_positions[key]





def parse_log_line(line):
    info_lst = [e.strip() for e in line.split("|#|") if e.strip()]
    if len(info_lst) >= 7:
        timestamp = info_lst[0]
        date, time = timestamp.split()
        session_id = info_lst[4]
        user_phone = info_lst[5]
        raw_text = info_lst[-1]
        code = 0
        return date, time, session_id, user_phone, raw_text, code
    else:
        code = -1
        return None, None, None, None, None, code

def data_reader(file_path, chunk_size=1000):
    with open(file_path, 'r', encoding='utf-8') as file:
        chunk = []
        for line in file:
            line=line.replace("：",":").strip()
            chunk.append(line)
            if len(chunk) == chunk_size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk





def saveJsonLines(data_list, file_out_path,mode="a"):
    try:
        code = 0
        with open(file_out_path, mode=mode, encoding="utf-8") as f:
            for data in data_list:
                f.write(json.dumps(data, ensure_ascii=False) + "\n")
    except:
        code = -1
    return code



def readJsonLines(json_path):
    total_data = []
    with open(json_path, "r", encoding="utf-8") as f:
        for line in f:
            data = json.loads(line)
            total_data.append(data)
    print(f"{json_path}提取到{len(total_data)}个对话数据!")
    return total_data


def transform_data(data_list):
    total_data=[]
    for data in data_list:
        special_condition = any([True if token in data else False for token in
                                 ["Thought", "Action", "Action_Parameter", "Observation", "Finish", "<ret>", "<end>",
                                  "<User>", "<Bot>"]])
        if "|#|INFO|#|modelLogger|#|" not in data and special_condition:
            if len(total_data)==0:
                total_data.append(data)
            else:
                total_data[-1]+=data
        else:
            total_data.append(data)
    return total_data


async def delete_folder_and_contents(folder_path):
    try:
        shutil.rmtree(folder_path)
        print(f"文件夹 '{folder_path}' 及其内容已被删除。")
    except FileNotFoundError:
        print(f"文件夹 '{folder_path}' 不存在，无法删除。")
    except Exception as e:
        print(f"删除文件夹时发生错误: {e}")



def get_encode(raw_log_file_path):
    """
    :param raw_log_file_path:
    :return:
    """
    with open(raw_log_file_path, 'rb') as f:
        raw_data = f.read(10000)
        result = chardet.detect(raw_data)
        encoding = result['encoding'] if result['confidence'] > 0.5 else 'utf-8'
    return encoding




def write_log(directory, session_id, data, spliter="\n",is_list=False):
    log_file_path = os.path.join(directory, f"{session_id}.log")
    if not is_list:
        with open(log_file_path, 'a', encoding='utf-8') as log_file:
            log_file.write(data + spliter)
    else:
        with open(log_file_path,"a",encoding="utf-8") as log_file:
            for e in data:
                log_file.write(str(e)+spliter)



def delete_non_xlsx_files(folder_path):
    if not os.path.exists(folder_path):
        print("指定的文件夹不存在")
        return

    for root, dirs, files in os.walk(folder_path):
        for file_name in files:
            if not file_name.endswith('.xlsx'):
                file_path = os.path.join(root, file_name)
                os.remove(file_path)
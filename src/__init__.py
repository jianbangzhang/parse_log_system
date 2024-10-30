# !/usr/bin/env python
# -*-coding:utf-8 -*-

"""
# File       : __init__.py.py
# Time       ：2024/7/10 07:32
# Author     ：jianbang
# version    ：python 3.10
# company    : IFLYTEK Co.,Ltd.
# emil       : whdx072018@foxmail.com
# Description：
"""
from .data_process import ParseLogs
from .data_utils import get_encode,delete_non_xlsx_files

import os
import zipfile
import pandas as pd
import time
from concurrent.futures import ProcessPoolExecutor


def split_log_file(log_file_path, size, output_dir):
    """
    :param log_file_path: 原始日志文件的路径
    :param size: 每个小文件的最大大小（字节）
    :param output_dir: 输出目录，存放拆分后的日志文件
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    log_file_path_list=[]
    save_file_dir_list=[]

    file_size = os.path.getsize(log_file_path)
    if file_size <= size:
        print(f"File size is {file_size} bytes, which is less than or equal to {size} bytes. No splitting needed.")
        log_file_path_list.append(log_file_path)
        log_output_dir = os.path.join(output_dir,"all")
        save_file_dir_list.append(log_output_dir)
        return log_file_path_list,save_file_dir_list

    part_num = 1
    buffer = []
    buffer_size = 0

    encoding = get_encode(log_file_path)
    print(f"Detected file encoding: {encoding}")

    with open(log_file_path, encoding=encoding, errors='replace') as file:
        for line in file:
            line = line.strip()
            if not line:
                continue
            buffer.append(line)
            buffer_size += len(line.encode(encoding))

            if buffer_size >= size:
                output_file_path = os.path.join(output_dir,str(part_num), f"part_{part_num}.log")
                save_file_path = os.path.join(output_dir,str(part_num), f"part_{part_num}")
                os.makedirs(os.path.dirname(output_file_path),exist_ok=True)
                with open(output_file_path, 'w', encoding='utf-8') as output_file:
                    output_file.write("\n".join(buffer) + "\n")
                print(f"Wrote {output_file_path} with size {buffer_size} bytes.")
                log_file_path_list.append(output_file_path)
                save_file_dir_list.append(save_file_path)
                part_num += 1
                buffer.clear()
                buffer_size = 0

        if buffer:
            output_file_path = os.path.join(output_dir, str(part_num), f"part_{part_num}.log")
            save_file_path = os.path.join(output_dir, str(part_num), f"part_{part_num}")
            os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
            with open(output_file_path, 'w', encoding='utf-8') as output_file:
                output_file.write("\n".join(buffer) + "\n")
            print(f"Wrote {output_file_path} with size {buffer_size} bytes.")
            log_file_path_list.append(output_file_path)
            save_file_dir_list.append(save_file_path)

    print(f"Log file split into {part_num} parts.")
    os.remove(log_file_path)
    return log_file_path_list,save_file_dir_list



def merge_excel_files(excel_files, output_file):
    """
    合并指定目录中的所有 Excel 文件并保存为一个新的 Excel 文件。
    :param input_dir: 输入目录，包含要合并的 Excel 文件
    :param output_file: 输出文件的路径
    """
    if not excel_files:
        print("没有找到任何 Excel 文件。")
        return

    all_data = []
    for file_path in excel_files:
        print(f"正在处理文件: {file_path}")

        try:
            df = pd.read_excel(file_path)
            all_data.append(df)
        except Exception as e:
            print(f"读取文件 {file_path} 时出错: {e}")

    if all_data:
        merged_df = pd.concat(all_data, ignore_index=True)
        merged_df.to_excel(output_file, index=False)
        print(f"总数据量：{len(merged_df)}")
        print(f"合并完成，结果保存为: {output_file}")
    else:
        print("没有数据可以合并。")


def unzip(zip_path, extract_path):
    if not os.path.exists(extract_path):
        os.makedirs(extract_path)

    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_path)
        print(f"文件已成功解压到 {extract_path}")


def get_delay(size):
    if size <= 0.2 * 1024 * 1024 * 1024:
        delay = 5
    elif size <= 0.4 * 1024 * 1024 * 1024:
        delay = 10
    elif size <= 0.7 * 1024 * 1024 * 1024:
        delay = 20
    else:
        delay = 30
    return delay



def run_parallels(log_file,output_dir,size,new_method=True):
    """
    :param log_file:
    :param output_dir:
    :param size:
    :return:
    """
    if log_file.endswith(".zip"):
        log_file_dir=os.path.dirname(log_file)
        unzip(log_file,log_file_dir)
        log_file_name=[file for file in os.listdir(log_file_dir) if file.endswith(".log")]
        if len(log_file_name)==0:
            os.remove(log_file)
            delete_non_xlsx_files(os.path.dirname(log_file))
            raise FileNotFoundError("解压文件里面没有log文件！")
        else:
            log_name=log_file_name[0]
            os.remove(log_file)
        log_file = os.path.join(log_file_dir,log_name)

    log_file_path_list,save_dir_list = split_log_file(log_file,size,output_dir)
    pl = ParseLogs(t5_token="T5", agent_token="AGENT")
    if len(log_file_path_list)==1:
        code,files_lst=pl.process_log_file(log_file_path_list[0],save_dir_list[0])
        delete_non_xlsx_files(os.path.dirname(log_file))
        return code,files_lst
    else:
        if not new_method:
            total_files_list = []
            size=min(len(log_file_path_list)//2,4)
            with ProcessPoolExecutor(max_workers=size) as executor:
                futures = {executor.submit(pl.process_log_file, data[0], data[-1]): data for
                           data in zip(log_file_path_list,save_dir_list)}

                for future in futures:
                    try:
                        code, files_lst = future.result()
                        total_files_list += files_lst
                    except Exception as e:
                        print(f"处理文件 {futures[future]['file']} 时出错: {e}")
                        continue
        else:
            total_files_list = []
            workers = min(len(log_file_path_list),32)
            delay=get_delay(size)
            with ProcessPoolExecutor(max_workers=workers) as executor:
                futures = {}
                for data in zip(log_file_path_list, save_dir_list):
                    future = executor.submit(pl.process_log_file, data[0], data[-1])
                    futures[future] = data
                    time.sleep(delay)

                for future in futures:
                    try:
                        code, files_lst = future.result()
                        total_files_list += files_lst
                    except Exception as e:
                        print(f"处理文件 {futures[future][0]} 时出错: {e}")
                        continue
        try:
            delete_non_xlsx_files(os.path.dirname(log_file))
            agent_excel_dir=[lst[0] for lst in total_files_list if lst]
            total_agent_excel = os.path.join(output_dir,"agent_all.xlsx")
            t5_excel_dir =[lst[-1] for lst in total_files_list if lst]
            total_t5_excel = os.path.join(output_dir, "t5_all.xlsx")
            merge_excel_files(agent_excel_dir,total_agent_excel)
            merge_excel_files(t5_excel_dir,total_t5_excel)
            print("处理完成")
            return 0, [(total_agent_excel, total_t5_excel)]
        except Exception as e:
            print(str(e))
            delete_non_xlsx_files(os.path.dirname(log_file))
            return -1,[]









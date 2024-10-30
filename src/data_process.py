# !/usr/bin/env python
# -*-coding:utf-8 -*-

"""
# File       : data_process.py
# Time       ：2024/7/10 07:42
# Author     ：jianbang
# version    ：python 3.10
# company    : IFLYTEK Co.,Ltd.
# emil       : whdx072018@foxmail.com
# Description：
"""
import os
import json
import threading
import pandas as pd
from .data_utils import data_reader,\
    parse_log_line,\
    saveJsonLines,\
    readJsonLines,\
    transform_data,\
    get_encode,\
    delete_non_xlsx_files



class ParseLogs(object):
    def __init__(self, t5_token="T5", agent_token="AGENT"):
        """
        :param t5_token:
        :param agent_token:
        """
        self.t5_token = t5_token
        self.agent_token = agent_token

    def _makedir(self,save_path):
        self.user_dir = save_path
        self.t5_dir = os.path.join(self.user_dir, "t5")
        self.session_dir = os.path.join(self.user_dir, "session")
        self.all_dir = os.path.join(self.user_dir, "all")
        self.t5_json_path = os.path.join(self.user_dir, "t5.json")
        self.session_json_path = os.path.join(self.user_dir, "session.json")
        self.t5_excel_path = os.path.join(self.user_dir, "t5.xlsx")
        self.session_excel_path = os.path.join(self.user_dir, "session.xlsx")
        os.makedirs(self.t5_dir, exist_ok=True)
        os.makedirs(self.session_dir,exist_ok=True)
        os.makedirs(self.all_dir,exist_ok=True)

    def preprocess(self,raw_log_file_path):
        special_tokens = ["Thought", "Action", "Action_Parameter", "Observation", "Finish", "<ret>", "<end>", "<User>",
                          "<Bot>"]

        encoding = get_encode(raw_log_file_path)
        print(f"Detected file encoding: {encoding}")

        try:
            with open(raw_log_file_path, 'r', encoding=encoding, errors='replace') as file:
                buffer = []
                for line in file:
                    line = line.replace("：", ":").strip()
                    special_condition = any(token in line for token in special_tokens)

                    if special_condition and "|#|INFO|#|modelLogger|#|" not in line:
                        if buffer:
                            buffer[-1] += line
                        else:
                            buffer.append(line)
                    else:
                        buffer.append(line)

                    if len(buffer) >= 10000:
                        self.write_log(self.all_dir, "all", buffer, spliter="\n", is_list=True)
                        buffer.clear()

                if buffer:
                    self.write_log(self.all_dir, "all", buffer, spliter="\n", is_list=True)

            os.remove(raw_log_file_path)
        except UnicodeDecodeError as e:
            print(f"Error decoding file: {e}")
            
            
    def split_and_classify(self):
        session_lst=[]
        all_log_path=os.path.join(self.all_dir,"all.log")
        for data_chunk in data_reader(all_log_path):
            for data in data_chunk:
                date, time, session_id, user_phone, raw_text, code = parse_log_line(data)
                special_condition=any([True if token in data else False for token in ["Thought","Action","Action_Parameter","Observation","Finish","<ret>","<end>","<User>","<Bot>"]])
                if code != 0 and "|#|INFO|#|modelLogger|#|" in data:
                    continue

                if session_id is not None and session_id not in session_lst:
                    session_lst.append(session_id)

                if session_id is None and special_condition and "|#|INFO|#|modelLogger|#|" not in data:
                    session_id=session_lst[-1]
                    self.write_log(self.session_dir, session_id, data)

                if session_id is not None and session_id[-1] in data and "|#|INFO|#|modelLogger|#|" in data:
                    self.write_log(self.session_dir, session_id, data)

                if raw_text is not None and self.t5_token in raw_text:
                    self.write_log(self.t5_dir, session_id, data)
                else:
                    continue

    def write_log(self, directory, session_id, data, spliter="\n",is_list=False):
        log_file_path = os.path.join(directory, f"{session_id}.log")
        if not is_list:
            with open(log_file_path, 'a', encoding='utf-8') as log_file:
                log_file.write(data + spliter)
        else:
            with open(log_file_path,"a",encoding="utf-8") as log_file:
                for e in data:
                    log_file.write(str(e)+spliter)


    def process_session_log_files(self,session_dir,session_json_path):
        for file_name in os.listdir(session_dir):
            log_file_path = os.path.join(session_dir, file_name)
            try:
                all_text=""
                with open(log_file_path, 'r', encoding='utf-8') as log_file:
                    total_data=log_file.read()
                    log_data = []
                    session_data = {}
                    agent_trajectory = ""

                    if self.agent_token not in total_data:
                        continue

                    total_data_lst=total_data.split("\n")
                    total_data_lst=transform_data(total_data_lst)
                    for line in total_data_lst:
                        line=line.replace("：",":")
                        if "记录用户请求-问题" in line:
                            session_data["query"]=line.split("记录用户请求-问题:")[-1].strip()
                        date, time, session_id, user_phone, raw_text, code = parse_log_line(line)
                        if code != 0:
                            continue

                        all_text += line + "\n"
                        session_data["date"] = date
                        session_data["time"] = time
                        session_data["session_id"] = session_id
                        if "记录13B模型响应信息" in raw_text:
                            raw_text = raw_text.replace("记录13B模型响应信息:", "").strip()
                            session_data["intent"] = raw_text
                        elif "记录T5模型响应信息" in raw_text:
                            raw_text = raw_text.replace("记录T5模型响应信息:", "").strip()
                            session_data["intent"] = raw_text

                        elif "记录AGENT模型请求信息" in raw_text:
                            raw_text = raw_text.replace("记录AGENT模型请求信息:", "").strip()
                            session_data["prompt"] = raw_text
                            if "<end><Bot>" in raw_text and "<end><User>" in raw_text and "<end><Bot> Thought" in raw_text:
                                query, trajectory = raw_text.split("<end><Bot>")
                                query = query.split("<end><User>")[-1].strip()
                                if trajectory not in agent_trajectory:
                                    agent_trajectory += raw_text.strip() + "\n"
                                if ("query" in session_data and len(session_data["query"]) == 0) or ("query" not in session_data):
                                    session_data["query"] = query
                        elif "记录AGENT模型响应信息" in raw_text:
                            raw_text = raw_text.replace("记录AGENT模型响应信息:", "").strip()
                            if raw_text not in agent_trajectory:
                                agent_trajectory += raw_text
                        else:
                            continue

                    if "接下来开始对话" in all_text and "<end><Bot>" in all_text and "<end><User>" in all_text:
                        query = all_text.split("<end><Bot>")[0]
                        query = query.split("<end><User>")[-1].strip()
                        if ("query" in session_data and len(session_data["query"]) == 0) or ("query" not in session_data):
                            session_data["query"] = query

                    agent_trajectory = agent_trajectory.split("<end><Bot>")[-1].replace("<end>", "\n").replace("<ret>", "\n").replace("Thought","\nThought").replace("Action","\nAction").replace("Observation","\nObservation").replace("Finish","\nFinish").replace("\n\n","\n").strip()
                    session_data["trajectory"] = agent_trajectory

                    log_data.append(session_data)
                saveJsonLines(log_data, session_json_path, "a")
                os.remove(log_file_path)
            except Exception as e:
                os.remove(log_file_path)
                print(f"{log_file_path}出错:",str(e))
                continue

    def process_t5_log_files(self, t5_directory, t5_json_path):
        for file_name in os.listdir(t5_directory):
            log_file_path = os.path.join(t5_directory, file_name)
            try:
                with open(log_file_path, 'r', encoding='utf-8') as log_file:
                    log_data = []
                    t5_data={}
                    for line in log_file.readlines()[-2:]:
                        date, time, session_id, user_phone, raw_text, code = parse_log_line(line)
                        if code != 0:continue
                        t5_data["date"]=date
                        t5_data["time"]=time
                        t5_data["session_id"]=session_id
                        if "记录T5模型请求信息" in raw_text:
                            raw_text=raw_text.replace("记录T5模型请求信息:","").strip()
                            t5_data["request"]=raw_text
                            if "history" in line:
                                try:
                                    try:
                                        intent = json.loads(raw_text)
                                        history = intent["payload"]["text"]["history"]
                                    except:
                                        intent = eval(raw_text)
                                        history = intent["payload"]["text"]["history"]
                                except:
                                    history = ""
                            else:
                                history = ""
                            t5_data["history"] = history
                        elif "记录T5模型响应信息" in raw_text:
                            raw_text = raw_text.replace("记录T5模型响应信息:", "").strip()
                            t5_data["intent"]=raw_text
                        else:
                            continue

                    log_data.append(t5_data)
                saveJsonLines(log_data,t5_json_path,"a")
                os.remove(log_file_path)
            except Exception as e:
                print(f"{log_file_path}出错:",str(e))
                os.remove(log_file_path)
                continue

    def combine2json(self):
        t5_thread = threading.Thread(target=self.process_t5_log_files, args=(self.t5_dir, self.t5_json_path,))
        session_thread = threading.Thread(target=self.process_session_log_files,args=(self.session_dir, self.session_json_path,))
        t5_thread.start()
        session_thread.start()
        t5_thread.join()
        session_thread.join()

    def save2excel(self):
        if os.path.exists(self.t5_json_path):
            t5_data = readJsonLines(self.t5_json_path)
            t5_df = pd.DataFrame(t5_data)
            t5_df.dropna(inplace=True)
            t5_df.to_excel(self.t5_excel_path, index=False)
        else:
            t5_df=pd.DataFrame()
            t5_df.to_excel(self.t5_excel_path, index=False)

        if os.path.exists(self.session_json_path):
            session_data = readJsonLines(self.session_json_path)
            session_df = pd.DataFrame(session_data)
            if os.path.exists(self.t5_json_path):
                column_order = ['date', 'time', 'prompt', 'session_id', 'query', "intent", 'trajectory']
            else:
                column_order = ['date', 'time', 'prompt', 'session_id', 'query', 'trajectory']
            session_df = session_df[column_order]
            session_df.dropna(subset=["query"])
            session_df.to_excel(self.session_excel_path, index=False)
        else:
            session_df = pd.DataFrame()
            session_df.to_excel(self.session_excel_path, index=False)


    def process_log_file(self,raw_log_file_path,save_path):
        try:
            self._makedir(save_path)
            print("0.开始预处理.......")
            self.preprocess(raw_log_file_path)
            print("1.开始分类......")
            self.split_and_classify()
            print("2.开始转为解析，保存为json......")
            self.combine2json()
            print("3.开始转为excel文件......")
            self.save2excel()

            out_file=[(self.session_excel_path,self.t5_excel_path)]
            delete_non_xlsx_files(save_path)
            return 0,out_file
        except Exception as e:
            print(str(e))
            return -1,[]



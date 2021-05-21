import re
import os
# import sys
import time
import codecs
import random
# import shutil
# import smtplib
import requests
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup as BS
from pathlib import Path
# from chardet import detect
# from functools import reduce
# from datetime import datetime, date, timedelta
# from dateutil.relativedelta import relativedelta

def mkdirs(path):
    if not os.path.exists(path):
        os.makedirs(path)

def dir_empty(dir_path):
    return not next(os.scandir(dir_path), None)

def _requests_session():
    session = requests.session()
    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36"
    }
    session.headers.update(headers)
    return session

def extract_table(time_id, table_id):
    url = "https://www.dramexchange.com/"
    rs = _requests_session()
    res = rs.get(url)
    soup = BS(res.text, "lxml")
    text_time = soup.find(id=time_id).text
    str_time = re.findall("\w+[\. ]?\d+ \d{4}", text_time)[0]
    time_spot = time.strptime(str_time.replace(".", ""), "%b%d %Y")
    date_spot = time.strftime("%Y-%m-%d", time_spot)
    table_spot = soup.find("tbody", {"id": table_id}).parent
    df_spot = pd.read_html(table_spot.prettify(), header=0)[0]
    df_spot_daily = df_spot[["Item", "Session Average"]]
    cols = ["Item", date_spot]
    df_spot_daily.columns = cols
    time.sleep(random.uniform(1, 2))
    return df_spot_daily

dram_id = ["NationalDramSpotPrice_show_day", "tb_NationalDramSpotPrice"]
flash_id = ["NationalFlashSpotPrice_show_day", "tb_NationalFlashSpotPrice"]
element = [dram_id, flash_id]
dfs_spot = [extract_table(x, y) for x, y in element]
df_daily = pd.concat(dfs_spot, axis=0, join="outer")
date_spot = df_daily.columns[-1]

file_name = "dram-{}.csv".format(date_spot)
folder_path = "./dram-daily"
mkdirs(folder_path)
file_path = "{}/{}".format(folder_path, file_name)
df_daily.to_csv(file_path, index=False, encoding="utf-8-sig")

merge_folder = "./dram-merge"
mkdirs(merge_folder)
if dir_empty(merge_folder):
    first_merge_file = "dram-merge-{}.csv".format(date_spot)
    first_merge_path = "{}/{}".format(merge_folder, first_merge_file)
    df_daily.to_csv(first_merge_path, index=False, encoding="utf-8-sig")

last_merge_path = list((Path(merge_folder).iterdir()))[-1]
df_merged = pd.read_csv(last_merge_path, encoding="utf-8-sig")
df_merge = df_merged.merge(df_daily, how="outer", on="Item")
df_merge.columns = df_merge.columns.str.replace("_y", "")
df_merge = df_merge.drop(list(df_merge.filter(regex = "_")), axis=1, inplace=False)
merge_filename = "dram-merge-{}.csv".format(date_spot)
merge_path = "{}/{}".format(merge_folder, merge_filename)
df_merge.to_csv(merge_path, index=False, encoding="utf-8-sig")

print("done!")
import os
import csv
import pandas as pd
import dask.dataframe as dd
from datetime import datetime
import re




def find_empty_txt_files(directory):
    empty_files = []
    for filename in os.listdir(directory):
        if filename.endswith('.txt'):
            file_path = os.path.join(directory, filename)
            if os.path.getsize(file_path) == 0:
                empty_files.append(filename)
    return empty_files

def process_txt_files(folder_path, skip_files):
    all_data = []
    txt_files = [f for f in os.listdir(folder_path) if f.endswith('.txt')]

    for txt_file in txt_files:
        if txt_file in skip_files:
            continue

        file_path = os.path.join(folder_path, txt_file)
        seen = {}

        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

            for line in lines:
                items = line.strip().split('\t')
                if len(items) > 1:
                    if items[1] not in seen:
                        seen[items[1]] = True
                        all_data.append(items[:-1])
                        
        print(f"Processed file: {txt_file}")

    return all_data

def transform_data_to_final_csv(data, output_file_path):
    df = pd.DataFrame(data,columns=['Tag', 'date', 'Value'])
    if df.empty:
        print("No data to transform.")
        return
    
    try:
        df[df.columns[2]] = pd.to_numeric(df[df.columns[2]], errors='coerce')
        # df_pivot = df.pivot_table(index=df.columns[1], columns=df.columns[0], values=df.columns[2])
        df_pivot = df.pivot_table(index='date', columns='Tag', values='Value')
        df['date'] = pd.to_datetime(df['date'])
        df_pivot.reset_index(inplace=True)
        df_pivot.fillna('', inplace=True)
        df_pivot.to_csv(output_file_path, index=False)
        print("Transformation complete, output file generated.")
    except ValueError as e:
        print("Error performing pivot operation:", e)
        print("Index column:", df.columns[1])
        print("Columns parameter:", df.columns[0])
        print("Values parameter:", df.columns[2])


def is_time_format(s):
    try:
        datetime.strptime(s, '%Y-%m-%d')
        return True
    except ValueError:
        return False

def del_txt(txt_path,out_path):
    '''txt文本文件时间前加入位号'''
    files = [file for file in os.listdir(txt_path) if file.endswith('.txt')]
    for file in files:
        filename_prefix = file.replace('.txt', '')
        file_path = os.path.join(txt_path, file)
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        output_filename = os.path.join(out_path, f"{filename_prefix}_output.txt")
        with open(output_filename, 'w', encoding='utf-8') as f_out:
            for line in lines:
                columns = line.strip().split()
                if len(columns) > 0 and is_time_format(columns[0]):
                    new_line = f"{filename_prefix}\t{line}"
                else:
                    new_line = line
                f_out.write(new_line)
        print(f"文件 {output_filename} 转换完成，输出文件名为 {file}")
    return  out_path

def main(DEBUGE=False):
       
    if DEBUGE:
        txt_path = '/usr/share/sc5tadm/gotoHomePubData/czh/test_program/testnew'
        out_path = '/usr/share/sc5tadm/gotoHomePubData/czh/test_program/testnew1'
        input_txt_folder = del_txt(txt_path,out_path) 


    input_txt_folder = '/usr/share/sc5tadm/gotoHomePubData/dataset/ConutiousReforming/JBSH/原始数据/202405京博石化连续重整/中聚20240101'  # 输入txt文件的文件夹路径
    # output_csv_path = '/usr/share/sc5tadm/gotoHomePubData/czh/test_program/testnew/中聚_合并_2023_11_11-2024_05_29.csv'  # 输出csv文件的路径
    output_csv_path = '/usr/share/sc5tadm/gotoHomePubData/dataset/ConutiousReforming/JBSH/处理后数据/v0/中聚_合并_2024_01_01-2024_05_07.csv'  # 输出csv文件的路径
    

    # Step 1: Find empty txt files to skip
    skip_files = find_empty_txt_files(input_txt_folder)
    if skip_files:
        print("以下txt文件为空，将被跳过：")
        for file in skip_files:
            print(file)

    # Step 2: Process txt files and gather data
    data = process_txt_files(input_txt_folder, skip_files)

    # Step 3: Transform data and generate final CSV
    transform_data_to_final_csv(data, output_csv_path)

if __name__ == "__main__":
    main()



    



    
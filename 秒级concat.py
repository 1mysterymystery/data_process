import os
import pandas as pd
from plottable import Table
import pyarrow.parquet as pq
import time
import gc
import glob
import pyarrow as pa
from concurrent.futures import ThreadPoolExecutor, as_completed

def concat_del(subdir_path, output_file_path):
# subdir_path = '/usr/share/sc5tadm/gotoHomePubData/czh/test_program/test'
    csv_files = [file for file in os.listdir(subdir_path) if file.endswith('.csv')]
# 读取CSV文件
    dfs = []
    for csv_file in csv_files:
            file_path = os.path.join(subdir_path, csv_file)
            df = pd.read_csv(file_path,index_col=[1], nrows=None,encoding = 'gbk',low_memory=False)
            df = df.drop(df.columns[0], axis=1)
            df.index = pd.to_datetime(df.index)
            df.index.name = 'date'
            dfs.append(df)
    merged_df = pd.concat(dfs, axis=0) 
    merged_df = merged_df.dropna(how='any', axis =1)
# 将DataFrame保存为Parquet文件
# output_file_path = '/usr/share/sc5tadm/gotoHomePubData/czh/test_program/test/testa.parquet'
    merged_df.to_parquet(output_file_path, engine='pyarrow',index='date')
    print("合并完成，结果保存为:", output_file_path)


def concat_data(dcs_data_dir,output_folder):
        # 获取path下所有文件及其路径
        FilePathlist = []       # 文件名列表，包含完整路径
        for home, dirs, files in os.walk(dcs_data_dir):
            for filename in files:
                if filename.endswith('.parquet'):
                    FilePathlist.append(os.path.join(home, filename))
        print("所有数据文件路径: ", FilePathlist)

        file_group = {}
        for item_path in FilePathlist:
            item = os.path.basename(item_path)
            key = item.split('_')[1]  # 修改这里，提取第一个下划线后面的部分作为分类键
            # key = item.split('_')[1]+'_'+item.split('_')[2]+'_'+item.split('_')[3]+'_'+item.split('_')[4]
            # key = '_'.join(item.split('_')[2:-1])
            if key not in file_group:
                file_group[key] = []  # 如果分类键不存在于字典中，则创建一个新的空列表
            file_group[key].append(item_path)  # 将当前项添加到分类键对应的列表中
        file_group = dict(sorted(file_group.items()))
        def process_group(group_name, group_file_list):
                    print('len group_file_list: ', len(group_file_list))
                    merged_df= pd.DataFrame()
                    for file_path in group_file_list:

                        tic_time = time.time()
                        print("————————————————————————————")
                        print(file_path)
                        # df = pd.read_csv(file_path,index_col=[1], nrows=None,encoding = 'gbk',low_memory=False, parse_dates=[1])
                        if file_path.lower().endswith('.csv'):
                            df = pd.read_csv(file_path,index_col=[1], encoding = 'gbk',low_memory=False, parse_dates=['时间'], nrows=100)
                            df = df.apply(pd.to_numeric, errors='coerce')
                            del df['序号']
                            df.index.name = 'date'
                        elif file_path.lower().endswith('.parquet'):
                            df = pd.read_parquet(file_path)
                        # dfs.append(df)
                    # if dfs:
                        # merged_df = pd.concat(dfs, axis=1)
                        merged_df = pd.concat([merged_df,df], axis=0)
                        print('time cost:', time.time()-tic_time)
                    merged_df = merged_df.loc[:, ~merged_df.columns.duplicated(keep='first')]
                    merged_df = merged_df.drop_duplicates(keep='first')
                    merged_df.index = pd.to_datetime(merged_df.index)
                    merged_df = merged_df.sort_values(by='date')
                    print('time cost:', time.time()-tic_time)
                    output_file = os.path.join(output_folder, f"{group_name}.parquet")
                    merged_df.to_parquet(output_file, engine='pyarrow',index=False)
                    print("合并完成，结果保存为:", output_file)
                    # del dfs
                    del merged_df
                    gc.collect()


        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(process_group, group_name, group_file_list) for group_name, group_file_list in file_group.items()]
            for future in as_completed(futures):
                future.result()
        return output_folder           



def clo_concat(dcs_data_dir,output_folder):
    parquet_file,save_name = concat_data(dcs_data_dir,output_folder)
    all_concat_df = pd.DataFrame()
    for file_name in os.listdir(parquet_file):
        file_path =  os.path.join(parquet_file, file_name)
        df_csv = pd.read_parquet(file_path)
        df_csv.set_index('date', inplace=True)



        all_concat_df = pd.concat([all_concat_df,df_csv],axis=1)
    all_concat_df.index.name = 'date'
    all_concat_df.reset_index(inplace=True)
    all_concat_df["date"] = pd.to_datetime(all_concat_df['date'])
    all_concat_df = all_concat_df.sort_values(by='date')
    all_concat_df = all_concat_df.dropna(how='all', axis=1)
    output_file = os.path.join(output_folder, f"{save_name}.parquet")
    all_concat_df.to_parquet(output_file, engine='pyarrow',index=False)





    
concat_data('/usr/share/sc5tadm/gotoHomePubData/dataset/湖北三宁/原始数据/历史趋势（5s）/气化公用_22-23',
       '/usr/share/sc5tadm/gotoHomePubData/dataset/湖北三宁/处理后数据/气化公用_22-23')

# clo_concat('/usr/share/sc5tadm/gotoHomePubData/dataset/湖北三宁汽化炉/湖北三宁解析后的数据/历史趋势/1.煤浆制备_2022_2.132 解析后的数据 20220202-20230204',
#        '/usr/share/sc5tadm/gotoHomePubData/dataset/湖北三宁汽化炉/湖北三宁解析后的数据/整理2')








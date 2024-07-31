import pandas as pd
from collections import defaultdict
import os
from plottable import Table
import pyarrow.parquet as pq
import time
import gc
import glob
import pyarrow as pa

def group_files_by_category(file_paths):
    category_files = defaultdict(list)

    for file_path in file_paths:
        category_key = extract_category_key(file_path)
        if category_key is not None:
            category_files[category_key].append(file_path)

    return category_files

def extract_category_key(file_path):
    path_parts = os.path.basename(file_path)
    parts = path_parts.split('_')
    if len(parts) >= 3:
        category_key = parts[1]
    else:
        category_key = None  # 如果文件名不符合要求，返回 None
    return category_key

def get_all_csv_files(root_dir):
    csv_files = []
    for root, dirs, files in os.walk(root_dir):
        for file in files:
            if file.endswith(".csv"):
                csv_files.append(os.path.join(root, file))
    return csv_files

def concat_csv_files(folder_path, output_folder):
    # 对文件进行分类
    csv_paths = get_all_csv_files(folder_path)
    category_files = group_files_by_category(csv_paths)
    # 遍历每个类别
    all_concat_df= pd.DataFrame()
    for category, csv_path in category_files.items():
        dfs = []
        for file_path in csv_path:
            df = pd.read_csv(file_path,encoding='gbk',nrows=100)
            dfs.append(df)   
        # 将同一类别的文件合并
        if dfs:
            concatenated_df = pd.concat(dfs,axis=0, ignore_index=True)
            concatenated_df.rename(columns={concatenated_df.columns[1]: 'date'}, inplace=True)
            concatenated_df.drop(df.columns[0], axis=1, inplace=True)
            concatenated_df.drop_duplicates(subset=['date'], keep='first', inplace=True)
            concatenated_df["date"] = pd.to_datetime(concatenated_df['date'])
            concatenated_df = concatenated_df.sort_values(by='date')
            output_file_path = os.path.join(output_folder, f"{category}_concat.csv")
            # concat_dfs.append(concatenated_df)
            # concatenated_df.to_csv(output_file_path, index=False)
            concatenated_df.to_parquet(output_file_path, engine='pyarrow',index=False)
            print(f"Saved concatenated file to {output_file_path}")

            all_concat_df = pd.concat([all_concat_df,concatenated_df],axis=1)
            all_concat_df_nan_col = all_concat_df.dropna(how='all', axis=1)  # 删除全为nan的位号
            # all_concat_df = all_concat_df.sort_values(by='date')
            # save_name = "_".join(category_dfs)+'_dropNanCol'
        save_name = "_".join(category_files.keys())+'_dropNanCol'
        new_file_path = os.path.join(output_folder, f"{save_name}.csv")
    # all_concat_df_nan_col.to_csv(new_file_path, index=False)
    all_concat_df_nan_col.to_parquet(new_file_path, engine='pyarrow',index=False)

    print('所有位号合并完成')



def concat_data(dcs_data_dir,output_folder):
    '''对每一个月的数据进行合并（秒级数据）'''
    subdirectories = [subdir for subdir in os.listdir(dcs_data_dir) if os.path.isdir(os.path.join(dcs_data_dir, subdir))]
    for subdir in subdirectories:
        subdir_path = os.path.join(dcs_data_dir, subdir)
        FilePathlist = []
        for home, dirs, files in os.walk(subdir_path):
            for filename in files:
                if filename.endswith('.csv'):
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
        all_concat_df= pd.DataFrame()
        for group_name, group_file_list in file_group.items():
            # for group_file_list_part in group_file_list:
                print('len group_file_list: ', len(group_file_list))
                dfs = []
                for file_path in group_file_list:
            # try:  

                    tic_time = time.time()
                    print("————————————————————————————")
                    print(file_path)
                    # df = pd.read_csv(file_path,index_col=[1], nrows=None,encoding = 'gbk',low_memory=False, parse_dates=[1])
                    if file_path.lower().endswith('.csv'):
                        df = pd.read_csv(file_path,index_col=[1], encoding = 'gbk',low_memory=False, parse_dates=['时间'], nrows=None)
                        df = df.apply(pd.to_numeric, errors='coerce')
                        del df['序号']
                        df.index.name = 'date'
                    elif file_path.lower().endswith('.parquet'):
                        df = pd.read_parquet(file_path)
                    dfs.append(df)
                if dfs:
                    merged_df = pd.concat(dfs, axis=0)
                    merged_df.index = pd.to_datetime(merged_df.index)
                    merged_df = merged_df.sort_values(by='date')
                    merged_df = merged_df.loc[:, ~merged_df.columns.duplicated()] 
                    # 重置索引并处理碎片化问题
                    newframe = merged_df.copy()
                    newframe.reset_index(inplace=True)
                    print('time cost:', time.time()-tic_time)
                    # first_df.append(merged_df)
                    # output_file = os.path.join(output_folder, f"{'_'.join(os.path.basename(file_path).split('_')[2:-1])}.parquet")
                    output_file = os.path.join(output_folder, f"{subdir}_{group_name}.parquet")
                    newframe.to_parquet(output_file, engine='pyarrow',index=True)
                    # all_concat_df = pd.concat([all_concat_df,merged_df],axis=1)
                    # all_concat_df = pd.merge(all_concat_df, merged_df, axis=1,     left_index= True,     right_index= True)
                    print("合并完成，结果保存为:", output_file)
                    del dfs
                    del merged_df
                    gc.collect()

    return output_folder

# concat_csv_files('/usr/share/sc5tadm/gotoHomePubData/dataset/湖北三宁汽化炉/湖北三宁解析后的数据/历史趋势/1.煤浆制备_2022_2.132 解析后的数据 20220202-20230204','/usr/share/sc5tadm/gotoHomePubData/czh/test_program/hgk')
concat_data('/usr/share/sc5tadm/gotoHomePubData/dataset/湖北三宁汽化炉/湖北三宁解析后的数据/历史趋势/2.煤浆制备_2023_2.132  解析后的数据 20230211-20240213',
            '/usr/share/sc5tadm/gotoHomePubData/dataset/湖北三宁汽化炉/湖北三宁解析后的数据/整理2')

   
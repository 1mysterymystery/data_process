import os
import pandas as pd
from plottable import Table
import pyarrow.parquet as pq
import time
import re

def merge_csv_files(folder_path, output_folder):
     
    subdirectories = [subdir for subdir in os.listdir(folder_path) if os.path.isdir(os.path.join(folder_path, subdir))]
    for subdir in subdirectories:
        subdir_path = os.path.join(folder_path, subdir)
        csv_files = [file for file in os.listdir(subdir_path) if file.endswith('.csv')]
        dfs = []
        for csv_file in csv_files[0:]:
            file_path = os.path.join(subdir_path, csv_file)
            df = pd.read_csv(file_path,index_col=[0], nrows=100,encoding='gbk')
            df.index = pd.to_datetime(df.index)
            df.index.name = 'date'
            dfs.append(df)
        merged_df = pd.concat(dfs, axis = 1)
        output_file = os.path.join(output_folder, f"{subdir}_merged_output.csv")
        merged_df.to_csv(output_file, index = 'date')
        print(f"成功合并了 {len(csv_files)} 个来自 {subdir} 的文件，并将结果保存为 {output_file}")
    return output_folder


def concat_csv_files(folder_path, output_folder):
        for root, dirs, files in os.walk(folder_path):
            dfs=[]
            for idx,file_name in enumerate(files):
                    subdir = os.path.basename(root)
                    csv_path = os.path.join(root, file_name)
                    df = pd.read_csv(csv_path, nrows=100,encoding='gbk')

                    # df.set_index('date', inplace=True)
                    # df.index = pd.to_datetime(df.index)
                    # df.index = pd.to_datetime(df.index)
                    # df['date'] = pd.to_datetime(df['date'])
                    # df = df.sort_values(by='date')
                    # df.index.name = 'date'
                    dfs.append(df)
                    if idx == len(files) - 1:
                        merged_df = pd.concat(dfs, axis=0,ignore_index=True)
                        # merged_df.drop_duplicates(subset=['date'], keep='first', inplace=True)
                        merged_df.rename(columns={df.columns[1]: 'date'}, inplace=True)
                        merged_df.drop(df.columns[0], axis=1, inplace=True)
                        output_file = os.path.join(output_folder, f"{subdir}_merged_output.csv")
                        merged_df.to_csv(output_file, index =False)
                        print(f"成功合并了 {len(files)} 个来自 {root} 的文件，并将结果保存为 {output_file}")

        return output_folder


def merge_and_combine(folder_path, output_folder):
    merged_folder = merge_csv_files(folder_path, output_folder)
    dfs = []
    for file in os.listdir(merged_folder):
        if file.endswith('.csv'):
            file_path = os.path.join(merged_folder, file)
                # 尝试读取文件并重命名列
            df = pd.read_csv(file_path, index_col=[0])
            df.index = pd.to_datetime(df.index)
            df.index.name = 'date'
            dfs.append(df)
    combined_df = pd.concat(dfs,  axis=0).sort_index()
    output_file_path = os.path.join(output_folder, 'combined_file.csv')
    combined_df.to_csv(output_file_path, index='date')
    print("合并完成，结果保存为:", output_file_path)

def screen_nan(*args):
    df = pd.read_csv(args[0])
    # 计算每列中的 NaN 值数量
    nan_counts = df.isna().sum().sort_values()
    print(nan_counts)
    df = df.dropna(axis=0, how='any')  
    df.to_csv(args[1], index=False)
    # df.to_parquet(args[1],engine='pyarrow',index=False)
    return nan_counts


def data_matching(*args,DEBUG=False):
    '''-筛选的位号匹配数据'''
    if args[0].lower().endswith('.csv'):
        df_csv = pd.read_csv(args[0], low_memory=False, nrows=None)
    else:
        df_csv = pd.read_parquet(args[0])            # 读取.parquet文件
        # df_csv = df_csv.head(1000)  
    # if DEBUG:
    #     df_csv.columns = [col.split('.')[0] for col in df_csv.columns]                     
    df_excel = pd.read_excel(args[1],sheet_name='气化装置关键变量')
    excel_positions = df_excel['位号名称'].tolist()
    # excel_positions = [col.split('.')[0] for col in excel_positions]
    # excel_positions = ['AI_2011_01.PV','AI_2011_02.PV', 'AI_2011_03.PV','AI_2011_04.PV']
    filtered_data = pd.DataFrame(data={'date': df_csv['date']})
    matched_columns = []
    for column in df_csv.columns:
        if DEBUG:
            position = column.split('.')[0]
            if position in excel_positions:
                filtered_data[column] = df_csv[column]
                matched_columns.append(position)
        else:
            if column in excel_positions:
                filtered_data[column] = df_csv[column]
                matched_columns.append(column)
    unmatched_columns = [pos for pos in excel_positions if pos not in matched_columns]
    filtered_data = filtered_data.drop_duplicates(subset='date', keep='first')  # 去除重复的时间
    filtered_data.to_csv(args[2], index=False)
    # filtered_data.to_parquet(args[2], engine='pyarrow',index=False)     # 保存为.parquet文件
    print(f"Filtered data appended to {args}")
    print(f"Matched columns: {len(matched_columns)}")
    print(f"Matched columns list:  {matched_columns}")
    print(f"Unmatched columns: {len(unmatched_columns)}")
    print(f"Unmatched columns list:  {unmatched_columns}")


def data_del(file_path,new_file_path):
    '''处理手动剔除异常值后的数据'''
    df = pd.read_csv(file_path)
    columns_to_drop = [col for col in df.columns if '记录数' in col]
    df.drop(columns=columns_to_drop, inplace=True)
    df['date'] = pd.to_datetime(df['date'])
    df['date'] = df['date'].dt.strftime('%Y-%m-%d %H:%M:%S')
    date_column = df.pop('date')
    df.insert(0, 'date', date_column)
    df.sort_values(by='date', inplace=True)
    df = df.dropna()
    df.to_csv(new_file_path, index=False)
    return df



def contains_ten_consecutive_nans(series):
    ''' 检查是否存在连续十个空值 '''
    return series.isna().astype(int).groupby(series.notna().astype(int).cumsum()).cumsum().max() >= 10

def fill_nans(file_path,out_path):
    df = pd.read_csv(file_path, parse_dates=['date'],nrows=None,index_col='date') 
    # 应用这个函数到每一列，找出需要删除的行
    cols_with_consecutive_nans = df.apply(contains_ten_consecutive_nans)
    rows_to_drop = df[cols_with_consecutive_nans].index
    df = df.drop(rows_to_drop)
    
    # 再检查每行，如果一行中超过2/3的值是空值，则删除这一行
    threshold = len(df.columns) * 2 / 3
    rows_to_drop = df[df.isnull().sum(axis=1) > threshold].index
    df_cleaned = df.drop(rows_to_drop)
    df_cleaned.fillna(method='ffill', inplace=True)
    # 输出处理后的数据
    print(df_cleaned)
    # 可以选择将清理后的数据保存到新的CSV文件
    df_cleaned.to_csv(out_path, index=False)
    # for col in df.columns:
    #     consecutive_nulls = 0
    #     indices_to_drop = []
    #     for i in range(len(df)):
    #         if pd.isnull(df[col][i]):
    #             consecutive_nulls += 1
    #         else:
    #             consecutive_nulls = 0
    #         if consecutive_nulls > 10:
    #             indices_to_drop.extend(list(range(i - consecutive_nulls + 1, i + 1)))
    #     df.drop(indices_to_drop, inplace=True)
        # df.drop(df.index[indices_to_drop], inplace=True)

#    # 遍历每一列
#     for column in df.columns:
#     # 判断是否存在连续的大于10个NaN值
#         nan_count = 0
#         for index, row in df.iterrows():
#             if pd.isna(row[column]):
#                 nan_count += 1 
#                 if nan_count > 10:
#                     # 删除这些NaN值所在的行
#                     df.drop(index, inplace=True)
#                     nan_count = 0
#             else:
#                 nan_count = 0
    # #   Step 2: Remove rows where the count of NaNs is greater than one-third of the columns
    # threshold = int(len(df.columns) * 2/3)  # Two-thirds of the number of columns
    # df = df.dropna(thresh=threshold)  # Rows need at least threshold non-NA values to stay
    # # Identify NaN locations before filling
    # nan_locations = df.isna().sum().sort_values()
    # # Print values at NaN locations before filling
    # print(f"Values at NaN locations before filling:{nan_locations}")
    # # Step 3: Fill NaNs by propagating the last valid observation forward to next valid
    # df.fillna(method='ffill', inplace=True)
    # # Print values at previously NaN locations after filling
    # print("\nValues at previously NaN locations after filling:")
    # for col in df.columns:
    #     if nan_locations[col].any():
    #         print(f"Column: {col}")
    #         print(df.loc[nan_locations[col], col])
    # # Optionally, save the modified DataFrame back to a CSV
    # df.to_csv(out_path, index=True)
    # threshold = int(len(df.columns) * 9 / 10)
    # 删除非NaN值少于阈值的行
    # df = df.dropna(thresh=threshold)
    # # 识别NaN位置
    # nan_locations = df.isna().sum().sort_values()
    # # 打印NaN位置的值
    # print(f"Values at NaN locations before filling:\n{nan_locations}")
    # # 用前向填充法填充NaN
    # df.fillna(method='ffill', inplace=True)
    # # 打印填充后NaN位置的值
    # print("\nValues at previously NaN locations after filling:")
    # for col in df.columns:
    #     if nan_locations[col] > 0:
    #         print(f"Column: {col}")
    #         print(df.loc[df.index[df[col].isna()], col])
    # # 保存修改后的DataFrame到CSV文件
    # df.to_csv(out_path, index=True)
    

def find_uncontinous_timespan(file_path, allowed_timespan):
    """
    找到不连续对应的时间段
    """
    df = pd.read_csv(file_path,parse_dates=['date'],index_col='date')
    # 计算时间差
    time_diff = df.index.to_series().diff().fillna(pd.Timedelta(seconds=0))

    # 找到超出允许时间跨度的开始和结束时间点
    endTime_list = df.index[time_diff > pd.Timedelta(seconds=allowed_timespan)].tolist()
    startTime_list = df.index[time_diff.shift(-1) > pd.Timedelta(seconds=allowed_timespan)].tolist()

    # 创建时间段列表
    timeSpan_list = []
    for startTime, endTime in zip(startTime_list, endTime_list):
        timeSpan_list.append((startTime.strftime('%Y-%m-%d %H:%M:%S'), endTime.strftime('%Y-%m-%d %H:%M:%S')))
    print(timeSpan_list)
    return timeSpan_list

def dppx_to_csv(path,output_file):
    ''' 从dppx格式的文件中提取数据并保存成csv文件'''
    # path = '/usr/share/sc5tadm/gotoHomePubData/czh/原始数据/新数据/批次1'
    # output_file='/usr/share/sc5tadm/gotoHomePubData/czh/test_program/abc'
    files = [file for file in os.listdir(path)if file.endswith('.dppx')]
    for file in files:
        filename_prefix = file.replace('.dppx', '')
        file_path = os.path.join(path, file)
        with open(file_path, 'r', encoding='utf-8') as f:
            text = f.read()
        text_list = text.split('\n')

        tag_nums = int(text_list[3][1:-1])
        tag_ids = []
        tag_describtion = []
        for line in range(4, 4+tag_nums):
            tmp = (text_list[line][1:-1].split('><'))
            tag_ids.append(tmp[1])
            tag_describtion.append(tmp[5])

        str_list = ['\|0', '\|192', '\|64']
        strip = "|".join(str_list)
        data = pd.DataFrame({'text': text_list})
        data['text'] = data['text'].apply(lambda x: re.sub(strip, "", x[1:-1]).split('><')).apply(list)
        result = pd.DataFrame(data['text'][4+tag_nums:].values.tolist(),
                                columns=['date']+tag_ids)
        result = result.replace("|", "")
        output_folde = os.path.join(output_file, f"{filename_prefix}.csv")
        result.to_csv(output_folde, index=False)
        print(f'{file} 转换成功')


if __name__ == '__main__':

    # screen_nan('/home/pubdata/dataset/ElectrolyticBath/electrolyzer_data/预处理后数据/dynamic/DRLZM_A/V8/DRA.csv')
    # fill_nan('/home/pubdata/czh/experimental procedure/data_concat/test.csv', '/home/pubdata/czh/experimental procedure/data_concat/test1.csv',col1 = 'HGLZM01.FIC1120A.PIDA.SP',col2 = 'HGLZM01.FIC1120B.PIDA.SP')
    # merge_and_combine('/home/pubdata/dataset/ElectrolyticBath/electrolyzer_data/原始数据/单位号数据/东瑞B套', '/home/pubdata/czh/experimental procedure/123/整理')
    # merge_csv_files('/home/pubdata/dataset/ElectrolyticBath/electrolyzer_data/原始数据/单位号数据/东瑞B套', '/home/pubdata/dataset/ElectrolyticBath/electrolyzer_data/预处理后数据/DR/东瑞B套_combined/v2')
    # merge_and_combine('/home/pubdata/dataset/ConutiousReforming/洛阳石化解析后数据（秒级）/2023年1-2月 OWS01200', '/home/pubdata/czh/experimental procedure/abc')

    # data_matching('/usr/share/sc5tadm/gotoHomePubData/dataset/湖北三宁/处理后数据/气化炉1和2_22-23/CS4/CS4.parquet',
    #               '/usr/share/sc5tadm/gotoHomePubData/dataset/湖北三宁/处理后数据/气化炉1和2_22-23/CS4/湖北三宁位号梳理cs4.xlsx',
    #               '/usr/share/sc5tadm/gotoHomePubData/dataset/湖北三宁/处理后数据/气化炉1和2_22-23/CS4/v1/HBSN_1-2_cs4.csv')


    # data_del('/usr/share/sc5tadm/gotoHomePubData/dataset/湖北三宁/处理后数据/气化炉1和2_22-23/CS4/HBSN_del.csv',
    # '/usr/share/sc5tadm/gotoHomePubData/dataset/湖北三宁/处理后数据/气化炉1和2_22-23/CS4/HBSN_del_v1.csv')

    # screen_nan('/usr/share/sc5tadm/gotoHomePubData/dataset/湖北三宁/处理后数据/气化炉1和2_22-23/CS4/HBSN_1-2_cs4.csv',
    # '/usr/share/sc5tadm/gotoHomePubData/dataset/湖北三宁/处理后数据/气化炉1和2_22-23/CS4/dropna_HBSN_1-2_cs4.csv')

     
    # fill_nans('/usr/share/sc5tadm/gotoHomePubData/dataset/湖北三宁/处理后数据/气化炉1和2_22-23/CS4/v1/HBSN_1-2_cs4.csv',
    #     '/usr/share/sc5tadm/gotoHomePubData/dataset/湖北三宁/处理后数据/气化炉1和2_22-23/CS4/v1/dropna_HBSN_1-2_cs4.csv')
     

    find_uncontinous_timespan('/home/pubdata/dataset/ElectrolyticBath/NMXF/处理后数据/v6/NMXF_nodel111.csv', 30)

    # fill_nans('/usr/share/sc5tadm/gotoHomePubData/czh/test_program/test1.csv',
    #           '/usr/share/sc5tadm/gotoHomePubData/czh/test_program/fillnan_test1.csv')

 



    # df_a = pd.read_csv('/usr/share/sc5tadm/gotoHomePubData/dataset/ElectrolyticBath/NMXF/处理后数据/v6/NMXF_del_v6.csv',parse_dates=['date'])
    # df_b = pd.read_csv('/usr/share/sc5tadm/gotoHomePubData/czh/dcs_preprocess/Common_data_processing/NMXF_notdel.csv',parse_dates=['date'])
    # # 对齐日期
    # df_b_aligned = df_b.set_index('date').reindex(df_a['date']).reset_index()
    # df_b_aligned.to_csv('/usr/share/sc5tadm/gotoHomePubData/czh/dcs_preprocess/Common_data_processing/NMXF_notdel111.csv', index=False)


    # merge_csv_files1('/usr/share/sc5tadm/gotoHomePubData/czh/test_program/湖北三宁','/usr/share/sc5tadm/gotoHomePubData/czh/test_program/def')

    





#     df1 = pd.read_csv('/usr/share/sc5tadm/gotoHomePubData/dataset/ElectrolyticBath/NMXF/处理后数据/v6/NMXF_del_v6.csv',parse_dates=['date'])
#     df2 = pd.read_csv('/usr/share/sc5tadm/gotoHomePubData/czh/dcs_preprocess/Common_data_processing/NMXF_notdel111.csv',parse_dates=['date'])
#     # 沿列方向合并两个DataFrame
#     result = pd.concat([df1, df2], axis=1, ignore_index=False)
#     result = result.loc[:, ~result.columns.duplicated(keep='first')] 
#     # result = result.sort_values(by='date')
#     result.to_csv('/usr/share/sc5tadm/gotoHomePubData/dataset/ElectrolyticBath/NMXF/处理后数据/v6_2/NMXF_v6_2.csv', index=False)

    


    
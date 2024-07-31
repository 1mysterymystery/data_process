import pandas as pd
import pyarrow.parquet as pq
import os



def read_path(dcs_data_dir):
        # 获取path下所有文件及其路径
        FilePathlist = []       # 文件名列表，包含完整路径
        for home, dirs, files in os.walk(dcs_data_dir):
            for filename in files:
                if filename.endswith('.parquet'):
                    FilePathlist.append(os.path.join(home, filename))
        print("所有数据文件路径: ", FilePathlist)
        print('len FilePathlist: ', len(FilePathlist))

        file_group = {}
        for item_path in FilePathlist:
            item = os.path.basename(item_path)
            key = item.split('_')[1]  # 修改这里，提取第一个下划线后面的部分作为分类键
            # key = '_'.join(item.split('_')[2:-1])
            if key not in file_group:
                file_group[key] = []  # 如果分类键不存在于字典中，则创建一个新的空列表
            file_group[key].append(item_path)  # 将当前项添加到分类键对应的列表中
        file_group = dict(sorted(file_group.items()))
        file_group = file_group['CS4']
        print('len file_group: ', len(file_group))

        return file_group

def data_matching(dcs_data_dir, excel_file_path, output_file_path, DEBUG=False):
    '''筛选的位号匹配数据'''
    parquet_file_paths = read_path(dcs_data_dir)
    filtered_data_list = []
    for  parquet_file_path in parquet_file_paths:
        if parquet_file_path.lower().endswith('.parquet'):
            parquet_file = pq.ParquetFile(parquet_file_path)
        else:
            raise ValueError("Only Parquet files are supported in this function.")
        
        df_excel = pd.read_excel(excel_file_path, sheet_name='Sheet1')
        
        excel_positions = df_excel['位号'].tolist()
        # excel_positions = ['II_P033101A.PV','II_P033101B.PV', 'SC_P032103B.OUT','SC_P032104A.OUT', 'SC_P032104B.OUT']
        

        num_row_groups = parquet_file.num_row_groups
        
        for row_group in range(num_row_groups):
            df_csv = parquet_file.read_row_group(row_group).to_pandas()

            # Process data in chunks
            # num_rows = df_csv.shape[0]
            # for start_row in range(0, num_rows, row_group_size):
            #     end_row = min(start_row + row_group_size, num_rows)
            #     chunk = df_csv.iloc[start_row:end_row]
                
            filtered_data = pd.DataFrame(data={'date': df_csv['date']})
            matched_columns = []

            # for column in chunk.columns:
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
            
            filtered_data = filtered_data.drop_duplicates(subset='date', keep='first')
            filtered_data = filtered_data.dropna(axis=0, how='any')
            filtered_data_list.append(filtered_data)
    
    combined_filtered_data = pd.concat(filtered_data_list, ignore_index=True)
    combined_filtered_data.to_csv(output_file_path, index=False)
    # combined_filtered_data.to_parquet(output_file_path, engine='pyarrow', index=False)     # 保存为.parquet文件
    
    matched_columns = list(set([col for df_csv in filtered_data_list for col in df_csv.columns if col != 'date']))
    unmatched_columns = [pos for pos in excel_positions if pos not in matched_columns]
    
    print(f"Filtered data appended to {output_file_path}")
    print(f"Matched columns: {len(matched_columns)}")
    print(f"Matched columns list: {matched_columns}")
    print(f"Unmatched columns: {len(unmatched_columns)}")
    print(f"Unmatched columns list: {unmatched_columns}")

data_matching('/usr/share/sc5tadm/gotoHomePubData/dataset/湖北三宁/原始数据/历史趋势par（1s）/气化炉1和2_22-23', 
'/usr/share/sc5tadm/gotoHomePubData/dataset/湖北三宁/处理后数据/气化炉1和2_22-23/CS4_1s/湖北三宁秒级数据位号梳理v1(1).xlsx', 
'/usr/share/sc5tadm/gotoHomePubData/dataset/湖北三宁/处理后数据/气化炉1和2_22-23/CS4_1s/HBSN_v1_1s.csv')
import pandas as pd
import os

# 指定包含CSV文件的目录路径
directory_path = '/home/pubdata/dataset/湖北三宁/处理后数据/PID'

# 初始化一个空列表来存储读取的DataFrame
dfs = []

# 遍历目录下的所有文件
for filename in os.listdir(directory_path):
    # 检查文件是否为CSV格式
    if filename.endswith('.csv'):
        # 构建完整的文件路径
        file_path = os.path.join(directory_path, filename)
        # 读取CSV文件并添加到列表中
        print(f'读取：{file_path}')
        df = pd.read_csv(file_path,parse_dates=['date'])
        dfs.append(df)

# 按列合并所有DataFrame
merged_df = pd.concat(dfs, axis=1)
merged_df = merged_df.loc[:, ~merged_df.columns.duplicated(keep='first')]
merged_df = merged_df.dropna(axis=0, how='any')

# 打印合并后的DataFrame的前几行
print('开始保存')

# （可选）保存合并后的DataFrame到新的CSV文件
output_path = '/home/pubdata/czh/dcs_preprocess/Common_dataprocessing/merged_file.csv'
merged_df.to_csv(output_path, index=False)



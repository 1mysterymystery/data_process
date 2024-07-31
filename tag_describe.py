import pandas as pd
import os

def get_excel_data(path3):
    file_data = {}
    table_names = []
    count = 1
    for root, dirs, files in os.walk(path3):
        for file in files:
            if file.endswith('.xls'):
                file_path = os.path.join(root, file)
                var_name = f"tag_table{count}"
                # df = pd.read_excel(file_path)
                # file_data[var_name] = df  # 存储 DataFrame
                # table_names.append(var_name)  # 添加 DataFrame 到 table_names 列表中
                file_data[var_name] = pd.read_excel(file_path, sheet_name=None)
                table_names.append(pd.read_excel(file_path, sheet_name=None))
                
            # else:
            #     pass
            count += 1
    return file_data, table_names

def get_data_taglist(data_columns_list, tag_table_list):
    """
    data_columns_list:list, 表示需要匹配的位号列表
    tag_table_list: list, 表示DCS导出的详细的所有位号列表。
    new_df: dataframe。输出data_columns list转dataframe，并加上位号描述。

    """
    new_df = pd.DataFrame(columns=['tag', 'csv-tag',  'describe'])
    for element in data_columns_list:
        element_new = element.split(".")[0]
        num = 0
        for tag_table in tag_table_list:                # 循环多个excel表
            for df_name, df in tag_table.items():       # 循环每个excel中多个sheet表格
                if df.__len__() != 0:
                    # 找到与当前元素匹配的行
                    describe = df['描述'][df['名称'] == element_new]
                    # 将匹配的行追加到新的DataFrame中
                    if describe.__len__() != 0:
                        new_df.loc[len(new_df)] = [element_new, element, describe.iloc[0]]
                        num = num + 1
                        print(element, "找到个数", num)
        if num == 0:
            print(element, "找不到")
            new_df.loc[len(new_df)] = [element_new, element, '']
    
    new_df.set_index('csv-tag', inplace=True)
    return new_df




data=pd.read_parquet('/usr/share/sc5tadm/gotoHomePubData/dataset/湖北三宁/处理后数据/气化炉1和2_22-23/CS4/CS4.parquet')
path3 = '/usr/share/sc5tadm/gotoHomePubData/dataset/湖北三宁/原始数据/组态和位号表信息'
path2='/usr/share/sc5tadm/gotoHomePubData/dataset/湖北三宁/处理后数据/气化炉1和2_22-23'
excel_files, table_names = get_excel_data(path3)

data_columns_describe = get_data_taglist(data_columns_list=data.columns.tolist(),
                                         tag_table_list=table_names)

data_columns_describe.to_excel(os.path.join(path2, "CS4_tag_describe.xlsx"))



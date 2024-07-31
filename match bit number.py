import pandas as pd
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

# 读取Excel文件
df = pd.read_excel('/home/pubdata/czh/test_program/新建 XLSX 工作表.xlsx')
# 转换列为字符串类型
df['DCS位号'] = df['DCS位号'].astype(str)
df['PI点位'] = df['PI点位'].astype(str)
# 定义一个函数用来进行模糊匹配
def fuzzy_match(dcs_number, pi_points):
    for pi_point in pi_points:
        if dcs_number in pi_point:
            return pi_point
    return None
# 应用函数并创建匹配列
df['匹配的PI点位'] = df.apply(lambda row: fuzzy_match(row['DCS位号'], df['PI点位']), axis=1)
# 保存到新的Excel文件
df.to_excel('/home/pubdata/czh/test_program/匹配结果.xlsx', index=False)
print("处理完成，已保存到'匹配结果.xlsx'文件中。")






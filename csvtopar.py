import pandas as pd
import os
import time
import logging


def setup_logger(log_file_path):
    logging.basicConfig(filename=log_file_path, level=logging.ERROR,
                        format='%(asctime)s:%(levelname)s:%(message)s')
    

def upsampling(dcs_data_dir,out_file):
    # 在目标目录下创建日志文件
    log_file_path = os.path.join(out_file, '%s_%s.log'%('error_log', time.strftime('%Y_%m_%d')))
    setup_logger(log_file_path)

    subdirectories = [subdir for subdir in os.listdir(dcs_data_dir) if os.path.isdir(os.path.join(dcs_data_dir, subdir))]
    for subdir in subdirectories:
        new_folder = os.path.join(out_file, subdir)
        # new_folder = os.path.join(out_file)
        os.makedirs(new_folder, exist_ok=True)
        subdir_path = os.path.join(dcs_data_dir, subdir)
        FilePathlist = []
        for home, dirs, files in os.walk(subdir_path):
            for filename in files:
                if filename.endswith('.csv'):
                    FilePathlist.append(os.path.join(home, filename))
        print("所有数据文件路径: ", FilePathlist)
        print('len FilePathlist: ', len(FilePathlist))

        for item_path in FilePathlist:
            save_naem = os.path.basename(item_path)
            save_name = '_'.join(save_naem.split('_')[:-1])
            tic_time = time.time()
            print("————————————————————————————")
            print(item_path)
            # df = pd.read_csv(item_path,index_col=[1], parse_dates=['时间'],nrows=None,encoding='gbk')
            try:
                df = pd.read_csv(item_path,index_col=[1], parse_dates=['时间'],nrows=None,encoding='gbk',low_memory=False)
            except (TypeError, ValueError) as e:
                error_message = f"Error processing {item_path}: {e}"
                print(error_message)
                logging.error(error_message)
                df = pd.read_csv(item_path,index_col=[1], parse_dates=['时间'],nrows=None,encoding='gbk',engine='python')
                print('重新读取成功')
                # continue  # 跳过出错的文件
            df = df.apply(pd.to_numeric, errors='coerce')
            del df['序号']
            df.index.name = 'date'
            # try:
            #     df_resampled = df.resample('5s').first()
            # except (TypeError, ValueError) as e:
            #     error_message = f"Error processing {item_path}: {e}"
            #     print(error_message)
            #     logging.error(error_message)
            #     continue  # 跳过出错的文件
            df.reset_index(inplace=True)
            output_file = os.path.join(new_folder, f"{save_name}_1.parquet")
            df.to_parquet(output_file, engine='pyarrow',index=False)
            print('time cost:', time.time()-tic_time)
            print("转换完成，结果保存为:", output_file)





upsampling('/home/pubdata/dataset/湖北三宁/原始数据/历史趋势（1s）/气化炉3_22-23',
'/home/pubdata/dataset/湖北三宁/原始数据/历史趋势par（1s）/气化炉3_22-23')



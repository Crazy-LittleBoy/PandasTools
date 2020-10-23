import math
import psutil
import copy
import pandas as pd
import multiprocessing
import time


pd.set_option('display.max_columns', 1000)
pd.set_option('display.max_rows', 1000)
pd.set_option('max_colwidth', 100)
pd.set_option('display.width', 1000)


def pd_bigdata_read_csv(file, **pd_read_csv_params):
    """
    读取速度提升不明显
    但是内存占用显著下降
    """
    reader = pd.read_csv(file, **pd_read_csv_params, iterator=True)
    loop = True
    try:
        chunk_size = pd_read_csv_params['chunksize']
    except:
        chunk_size = 1000000
    chunks = []
    while loop:
        try:
            chunk = reader.get_chunk(chunk_size)
            chunks.append(chunk)
        except StopIteration:
            loop = False
            print('[Info]: Iteration is stopped.')
    df = pd.concat(chunks, ignore_index=True, axis=0)
    return df


def run_time_count(func):
    """
    计算函数运行时间
    装饰器:@run_time_count
    """
    def run(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        print("[Info]: function [{0}] run time is {1} second(s).".format(func.__name__, round(time.time()-start,4)))
        return result
    return run


def count_file_lines(file):
    """
    计算文件行数
    count file lines
    :param file: file path
    :return: lines of input file
    """
    count = 0
    fp = open(file, "rb")
    byte_n = bytes("\n", encoding="utf-8")
    while 1:
        buffer = fp.read(1 * 1024 * 1024)
        if not buffer:
            break
        count += buffer.count(byte_n)
    fp.close()
    return count


def read_chunk(file, n_rows, skip_rows, columns, pd_read_csv_params):
    """
    read chunk
    :param file: file path
    :param n_rows: read lines
    :param skip_rows: lines of start read
    :param columns: names of columns
    :param pd_read_csv_params: pandas.read_csv params
    :return: pandas.DataFrame chunk
    """
    return pd.read_csv(file, nrows=n_rows, skiprows=skip_rows, names=columns, **pd_read_csv_params)


@run_time_count
def pd_multi_read_csv(file, processes=None, chunk_size=None, **pd_read_csv_params):
    """
    主函数,多进程读取大数据
    read big data by multiprocessing
    Notice that,if you used param 'nrows',it will be defaulting to pandas implementation.
    Of course you can set param 'processes' and 'chunk_size' replace 'nrows',nrows=processes*chunk_size
    :param file: file path
    :param processes: num of processes
    :param chunk_size: chunk size
    :param pd_read_csv_params: pandas.read_csv params
    :return: pandas.DataFrame
    """
    param = copy.deepcopy(pd_read_csv_params)
    for p in ['nrows', 'usecols']:
        try:
            param.pop(p)
        except:
            pass
    if pd_read_csv_params.get('names'):
        columns = pd_read_csv_params['names']
    else:
        columns = pd.read_csv(file, nrows=1, **param).columns.tolist()
    params = copy.deepcopy(pd_read_csv_params)
    for p in ['nrows', 'skiprows', 'names']:
        try:
            params.pop(p)
        except:
            pass
    if pd_read_csv_params.get('nrows'):
        print("[Warning]: 'read_csv' with 'nrows' defaulting to pandas implementation.You can set param 'processes' and 'chunk_size' replace 'nrows','nrows=processes*chunk_size'")
        return pd.read_csv(file, **pd_read_csv_params)
    else:
        lines = count_file_lines(file)
        print('[Info]: file lines is:', lines)
    if not processes and not chunk_size:
        use_processes = math.floor(psutil.cpu_count()*0.75)
        use_chunk_size = math.ceil(lines/use_processes)
    elif processes and not chunk_size:
        use_processes = processes
        use_chunk_size = math.ceil(lines/processes)
    elif not processes and chunk_size:
        use_processes = math.ceil(lines/chunk_size)
        use_chunk_size = chunk_size
    else:
        use_processes = processes
        use_chunk_size = chunk_size
    print('[Info]: use processes:',use_processes)
    print('[Info]: chunk size:',use_chunk_size)
    pool = multiprocessing.Pool(use_processes)
    chunks = []
    for i in range(use_processes):
        if pd_read_csv_params.get('names'):
            try:
                chunk = pool.apply_async(read_chunk, (file, use_chunk_size, use_chunk_size * i, columns, params))
                chunks.append(chunk)
            except StopIteration:
                print('[Info]: Iteration is stopped.')
        else:
            try:
                chunk = pool.apply_async(read_chunk, (file, use_chunk_size, use_chunk_size * i + 1, columns, params))
                chunks.append(chunk)
            except StopIteration:
                print('[Info]: Iteration is stopped.')
    pool.close()
    pool.join()
    chunks = [x.get() for x in chunks]
    df = pd.concat(chunks, ignore_index=True, sort=False, axis=0)
    print('[Info]: dataframe.shape:%s' % str(df.shape))
    return df


if __name__ == '__main__':
    # Example
    data = pd_multi_read_csv('example.csv',
                              sep=',',
                              processes=12,
                              low_memory=False,
                              )

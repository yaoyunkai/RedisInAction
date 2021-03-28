"""
@date: 2021-03-28
@author: liberty
@file: 4.1.1_SAP

the is a part of "RedisInAction"

"""
import os


# conn = redis.Redis('192.168.20.123', db=2)


def update_progress(conn, fname, offset):
    # 更新正在处理的日志文件的名字和偏移量。
    conn.mset({
        'progress:file': fname,
        'progress:position': offset
    })
    # 这个语句负责执行实际的日志更新操作，
    # 并将日志文件的名字和目前的处理进度记录到Redis里面。
    conn.execute()


def process_log(conn, path, callback):
    cur_file, offset = conn.mget('progress:file', 'progress:position')
    pipe = conn.pipeline()

    # 有序地遍历各个日志文件。
    for file_name in sorted(os.listdir(path)):
        # 略过所有已处理的日志文件。
        if file_name < cur_file:
            continue

        inp = open(os.path.join(path, file_name), 'rb')
        # 在接着处理一个因为系统崩溃而未能完成处理的日志文件时，略过已处理的内容。
        if file_name == cur_file:
            inp.seek(int(offset, 10))
        else:
            offset = 0

        cur_file = None

        # 枚举函数遍历一个由文件行组成的序列，
        # 并返回任意多个二元组，
        # 每个二元组包含了行号lno和行数据line，
        # 其中行号从0开始。
        for line_no, line in enumerate(inp):
            callback(pipe, line)
            # 更新已处理内容的偏移量。
            offset += int(offset) + len(line)

            # 每当处理完1000个日志行或者处理完整个日志文件的时候，
            # 都更新一次文件的处理进度。
            if not (line_no + 1) % 1000:
                update_progress(pipe, file_name, offset)

        update_progress(pipe, file_name, offset)
        inp.close()

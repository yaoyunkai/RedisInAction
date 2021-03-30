"""
@date: 2021-03-30
@author: liberty
@file: 6.4.1_fifo

the is a part of "RedisInAction"

"""
import json
import time

QUIT = False


def send_sold_email_via_queue(conn, seller, item, price, buyer):
    data = {
        'seller_id': seller,
        'item_id': item,
        'price': price,
        'buyer_id': buyer,
        'time': time.time()
    }

    conn.rpush('queue:email', json.dumps(data))


def process_sold_email_queue(conn):
    while not QUIT:
        packed = conn.blpop('queue:email', 30)
        if not packed:
            continue

        to_send = json.loads(packed[1])
        try:
            # fetch_data_and_send_sold_email(to_send)
            pass
        except Exception as err:
            print("Failed to send sold email", err, to_send)
        else:
            print("Sent sold email", to_send)


def worker_watch_queue(conn, queue, callbacks):
    while not QUIT:
        packed = conn.blpop([queue], 30)
        if not packed:
            continue

        name, args = json.loads(packed[1])
        if name not in callbacks:
            print("Unknown callback %s" % name)
            continue
        callbacks[name](*args)

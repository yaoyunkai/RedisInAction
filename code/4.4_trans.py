"""
@date: 2021-03-28
@author: liberty
@file: 4.4_trans

the is a part of "RedisInAction"

三个表
    hash: users:id
            name,
            funds,

    set: inventory:<user_id>
            item1 check item1 not dotted "."
            item2

    zset:  market:
                itemA.4   35

如果 A 卖 item1 B 卖 item1 C 买了 A 和 B 的item1 ？？？？？？？


"""
import time


def list_item(conn, item_id, seller_id, price):
    inventory = 'inventory:{}'.format(seller_id)
    item = '{}.{}'.format(item_id, seller_id)

    end_time = time.time() + 5
    pipe = conn.pipeline()

    while time.time() < end_time:
        try:
            pipe.watch(inventory)

            if not pipe.sismember(inventory, item_id):
                pipe.unwatch()
                return False

            pipe.multi()
            pipe.zadd('market:', item, price)
            pipe.srem(inventory, item_id)
            pipe.execute()
            return True

        except Exception as e:
            print(e)

    return False


def purchase_item(conn, buyer_id, item_id, seller_id, l_price):
    buyer = 'users:{}'.format(buyer_id)
    seller = 'users:{}'.format(seller_id)
    market_item = '{}.{}'.format(item_id, seller_id)
    inventory = 'inventory:{}'.format(buyer_id)

    end_time = time.time() + 10
    pipe = conn.pipeline()

    while time.time() < end_time:
        try:
            pipe.watch('market:', buyer)

            price = pipe.zscore('market:', market_item)
            funds = int(pipe.hget(buyer, 'funds'))

            if price != l_price or price > funds:
                pipe.unwatch()
                return None

            pipe.multi()
            pipe.hincrby(seller, 'funds', int(price))
            pipe.hincrby(buyer, 'funds', int(-price))
            pipe.sadd(inventory, item_id)
            pipe.zrem('market:', market_item)
            pipe.execute()
            return True

        except Exception as e:
            print(e)

    return False


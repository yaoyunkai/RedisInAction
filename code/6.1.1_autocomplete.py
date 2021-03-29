"""
@date: 2021-03-29
@author: liberty
@file: 6.1.1_autocomplete

the is a part of "RedisInAction"

redis == 2.7.0

"""


def add_update_contact(conn, user, contact):
    ac_list = 'recent:{}'.format(user)
    pipe = conn.pipeline(transaction=True)
    pipe.lrem(ac_list, contact)
    pipe.lpush(ac_list, contact)
    pipe.ltrim(ac_list, 0, 99)
    pipe.execute()


def remove_contact(conn, user, contact):
    conn.lrem('recent:' + user, contact, num=1)


def fetch_autocomplete_list(conn, user, prefix):
    _data = conn.lrange('recent:' + user, 0, -1)
    matches = []
    for one in _data:
        if one.lower().startswith(prefix):
            matches.append(one)

    return matches

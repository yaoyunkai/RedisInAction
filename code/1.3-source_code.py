"""
@date: 2021-03-24
@author: liberty
@file: 1.3-source_code

the is a part of "RedisInAction"

文章投票排序功能实现

"""
import time

import redis

ONE_WEEK_IN_SECONDS = 7 * 86400
# 86400 / 200
VOTE_SCORE = 432
ARTICLES_PER_PAGE = 25


def article_vote(conn, user, article):
    """

    :param conn: redis connection
    :param user: like this: user:<id>
    :param article: like this: article:<id>
    :return: Nothing
    """
    cutoff = time.time() - ONE_WEEK_IN_SECONDS

    if conn.zscore('time:', article) < cutoff:
        return

    article_id = article.partition(':')[-1]
    if conn.sadd('voted:' + article_id, user):
        conn.zincrby('score:', article, VOTE_SCORE)
        conn.hincrby(article, 'votes', 1)


def post_article(conn, user, title, link):
    # 生成一个新的文章ID。
    article_id = str(conn.incr('article:'))

    voted = 'voted:' + article_id
    # 将发布文章的用户添加到文章的已投票用户名单里面，
    # 然后将这个名单的过期时间设置为一周（第3章将对过期时间作更详细的介绍）。
    conn.sadd(voted, user)
    conn.expire(voted, ONE_WEEK_IN_SECONDS)

    now = time.time()
    article = 'article:' + article_id
    # 将文章信息存储到一个散列里面。
    conn.hmset(article, {
        'title': title,
        'link': link,
        'poster': user,
        'time': now,
        'votes': 1,
    })

    # 将文章添加到根据发布时间排序的有序集合和根据评分排序的有序集合里面。
    conn.zadd('score:', article, now + VOTE_SCORE)
    conn.zadd('time:', article, now)

    return article_id


def get_articles(conn, page, order='score:'):
    start = (page - 1) * ARTICLES_PER_PAGE
    end = start + ARTICLES_PER_PAGE - 1

    # 获取多个文章ID。
    ids = conn.zrevrange(order, start, end)
    articles = []
    # 根据文章ID获取文章的详细信息。
    for id in ids:
        article_data = conn.hgetall(id)
        article_data['id'] = id
        articles.append(article_data)

    return articles


def add_remove_groups(conn, article_id, to_add=None, to_remove=None):
    if to_add is None:
        to_add = []
    if to_remove is None:
        to_remove = []
    article = 'article:' + article_id
    for group in to_add:
        conn.sadd('group:' + group, article)
    for group in to_remove:
        conn.srem('group:' + group, article)


def get_group_articles(conn, group, page, order='score:'):
    # 为每个群组的每种排列顺序都创建一个键。
    key = order + group
    # 检查是否有已缓存的排序结果，如果没有的话就现在进行排序。
    if not conn.exists(key):
        # 根据评分或者发布时间，对群组文章进行排序。
        conn.zinterstore(key, ['group:' + group, order], aggregate='max', )
        # 让Redis在60秒钟之后自动删除这个有序集合。
        conn.expire(key, 60)
    # 调用之前定义的get_articles()函数来进行分页并获取文章数据。
    return get_articles(conn, page, key)


if __name__ == '__main__':
    red_conn = redis.Redis(host='192.168.20.123', db=2)
    art_id = post_article(red_conn, 'user:1', 'DemoDemo', 'https://demodemo.com')
    print('get art id is: {}'.format(art_id))

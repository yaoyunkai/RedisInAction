# Redis In Action

> Written by Liberty

## 1. 初始redis ##

### 1.2 redis 数据结构简介 ###

redis中有五种数据结构: `STRING` `LIST` `SET` `HASH` `ZSET`

#### 1.2.1 redis string ####

| command | behavior |
| :------ | -------- |
| GET     |          |
| SET     |          |
| DEL     |          |

```bash
127.0.0.1:6379> set hello world
OK
127.0.0.1:6379> get hello
"world"
127.0.0.1:6379> del hello
(integer) 1
127.0.0.1:6379> get hello
(nil)
127.0.0.1:6379> 
```

#### 1.2.2 redis list ####

一个列表可以有序地存储多个字符串。

| Command | behavior                                       |
| ------- | ---------------------------------------------- |
| RPUSH   | 将给定值推入列表的右端                         |
| LRANGE  | 获取列表在给定范围上的值 ，`-1` 表示列表的最后 |
| LINDEX  | get element by the index                       |
| LPOP    | 从列表的左边弹出值，并返回被弹出的值           |

```bash
127.0.0.1:6379> rpush list-key item
(integer) 1
127.0.0.1:6379> rpush list-key item2
(integer) 2
127.0.0.1:6379> rpush list-key item2
(integer) 3
127.0.0.1:6379> rpush list-key item
(integer) 4
127.0.0.1:6379> lrange list-key 0 -1
1) "item"
2) "item2"
3) "item2"
4) "item"
127.0.0.1:6379> LINDEX list-key 1
"item2"
127.0.0.1:6379> LINDEX list-key 44
(nil)
127.0.0.1:6379> LPOP list-key
"item"
127.0.0.1:6379> lrange list-key 0 -1
1) "item2"
2) "item2"
3) "item"
127.0.0.1:6379> 
```

#### 1.2.3 reids set （集合） ####

合集保证值的唯一性，类似于python中的SET

其中 redis set 是 unordered 无序的。

| command   | behavior                    |
| --------- | --------------------------- |
| SADD      | add element to set          |
| SMEMBERS  | get all of the set elements |
| SISMEMBER | 判断给定元素是否在集合中    |
| SREM      | 移除集合中存在的某个元素    |

```bash
127.0.0.1:6379> sadd set1 a
(integer) 1
127.0.0.1:6379> sadd set1 b
(integer) 1
127.0.0.1:6379> sadd set1 c
(integer) 1
127.0.0.1:6379> sadd set1 b
(integer) 0
127.0.0.1:6379> 0
(error) ERR unknown command `0`, with args beginning with: 
127.0.0.1:6379> SADD set1 1
(integer) 1
127.0.0.1:6379> SADD set1 "1"
(integer) 0
127.0.0.1:6379> SMEMBERS set1
1) "1"
2) "c"
3) "b"
4) "a"
127.0.0.1:6379> SISMEMBER set1 5
(integer) 0
127.0.0.1:6379> SISMEMBER set1 1
(integer) 1
127.0.0.1:6379> srem set1 1
(integer) 1
127.0.0.1:6379> srem set1 1
(integer) 0
127.0.0.1:6379> 
```

#### 1.2.4 redis hash ####

类似于python的dict ，其中 key可以是数字，而且可以执行自增或自减操作。

| command | behavior                          |
| ------- | --------------------------------- |
| hset    | 在指定的hash表中 加入k-v键值对    |
| hget    | 在指定的hash表中 根据k获取v       |
| hgetall | 获取指定hash表中 所有键值对       |
| hdel    | 在指定的hash表中 移除如果存在的键 |

```bash
127.0.0.1:6379> hset hash1 k1 v1
(integer) 1
127.0.0.1:6379> hset hash1 k2 v2
(integer) 1
127.0.0.1:6379> hset hash1 k1 v3
(integer) 0
127.0.0.1:6379> hset hash1 k3 v3
(integer) 1
127.0.0.1:6379> HGETALL hash1
1) "k1"
2) "v3"
3) "k2"
4) "v2"
5) "k3"
6) "v3"
127.0.0.1:6379> HDEL hash1 k1
(integer) 1
127.0.0.1:6379> HDEL hash1 k2
(integer) 1
127.0.0.1:6379> HGETALL hash1
1) "k3"
2) "v3"
127.0.0.1:6379> 
```

#### 1.2.5 redis ZSET ####

有序集合：键值对，值是分值(score)。

| command       | behavior                           |
| ------------- | ---------------------------------- |
| zadd          | 将一个带分值的key 添加到指定的zset |
| zrange        | index索引，根据索引获取元素        |
| zrangebyscore | 根据分值获取元素                   |

```bash
127.0.0.1:6379> zadd demo 123 k1
(integer) 1
127.0.0.1:6379> zadd demo 125 k2
(integer) 1
127.0.0.1:6379> zadd demo 125 k3
(integer) 1
127.0.0.1:6379> zadd demo 190 k4
(integer) 1
127.0.0.1:6379> ZRANGE demo 0 -1
1) "k1"
2) "k2"
3) "k3"
4) "k4"
127.0.0.1:6379> ZRANGE demo 0 -1 withscores
1) "k1"
2) "123"
3) "k2"
4) "125"
5) "k3"
6) "125"
7) "k4"
8) "190"
127.0.0.1:6379> zrangebysocre demo 0 800 withscores
(error) ERR unknown command `zrangebysocre`, with args beginning with: `demo`, `0`, `800`, `withscores`, 
127.0.0.1:6379> ZRANGEBYSCORE demo  150 190 withscores
1) "k4"
2) "190"
127.0.0.1:6379> zrem demo k2
(integer) 1
127.0.0.1:6379> ZRANGE demo 0 -1 withscores
1) "k1"
2) "123"
3) "k3"
4) "125"
5) "k4"
6) "190"
127.0.0.1:6379> 
```

### 1.3 hello redis ###

一个对文章进行评分排序的例子：

**数据表结构**

- 文章表 hash ---- articlle:id {title: , content: }
- 文章发布时间表 zset ---- time: {article:id, }
- 文章评分表 zset ---- score: {article:id, }
- 用户-文章投票表 set ---- voted:article_id {user:id, }

评分计算规则：票数*常量 + 发布时间


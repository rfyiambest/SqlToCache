## 介绍:
support Mongodb,Mysql,Hbase,ES .... ,And there Sql'result to redis cache .

```
from sqltocache import cache_it_msgpack

@cache_it_msgpack(cache=self.c)
def add_it(a, b=10, c=5):
    return a + b + c
add_it(3)
```

## ToFix:

1. 现在用的还是json和pickle组合模式，msgpack还存在少许bug


## ToDo:

1. 尽量多的兼容二次的序列化

2. 缓存的web api


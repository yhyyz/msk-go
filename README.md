## MSK kafka-go 生产消费测试

### build
```shell
GOOS=linux GOARCH=amd64 go build msk.go
# 编译好的
https://dxs9dnjebzm6y.cloudfront.net/tmp/msk
```
### producer
```shell
Usage:
  msk producer [flags]

Flags:
      --ack int           ack (default -1)
      --batchSize int     send records batch size (default 100)
      --broker string     msk broker (default "localhost:9092")
  -h, --help              help for producer
      --recordsNum int    send records number (default 1000)
      --runningTime int   total running time (default 600000)
      --timeout int       send batch records timeout (default 3000)
      --topic string      producer topic

# 执行命令会一直运行,直到设定的--runningTime时间超时，--timeout指的是一个batch的超时, 需要注意的是kafka-go的
# WriteMessages方法当数据不够100条时，会等待1s. 有个BatchSize参数默认是100，程序参数batchSize控制是多少条消息进行一次封装
# 比如10000条消费进行一次封装，kafka-go依然会100条作为批次提交。
./msk producer  --topic=test-data --broker=localhost:9092 --ack=-1 --recordsNum 1000 --batchSize 100 --timeout=1

```

### consumer
```shell
Usage:
  msk consumer [flags]

Flags:
      --broker string    msk broker (default "localhost:9092")
      --commitSize int   the number of records commit for each offset (default 100)
      --group string     consumer group
  -h, --help             help for consumer
      --offset string    consumer offset, earliest/latest
      --topic string     consumer topic

# 手动进行offset提交，commitSize指定发送多少条数据执行一次提交，消费数据会直接以日志打出来
./msk consumer --topic=test-data --broker=localhost:9092 --group cg2 --commitSize=100 --offset=earliest
```

![](https://pcmyp.oss-cn-beijing.aliyuncs.com/markdown/202308251424131.png)
![](https://pcmyp.oss-cn-beijing.aliyuncs.com/markdown/202308251427251.png)
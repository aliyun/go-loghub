# Aliyun LOG Go Consumer Library

Aliyun LOG Go Consumer Library 是一个易于使用且高度可配置的golang 类库，专门为大数据高并发场景下的多个消费者协同消费同一个logstore而编写的纯go语言的类库。

## 功能特点
1. 线程安全 - consumer 内所有的方法以及暴露的接口都是线程安全的。
2. 异步拉取 - 调用consumer的拉取日志接口，会把当前拉取任务新开一个groutine中去执行，不会阻塞主groutine的执行。
3. 自动重试 - 对程序运行当中出现的可重试的异常，consumer会自动重试，重试过程不会导致数据的重复消费。
4. 优雅关闭 - 调用关闭程序接口，consumer会等待当前所有已开出的groutine任务结束后在结束主程序，保证下次开始不会重复消费数据。
5. 本地调试 - 可通过配置支持将日志内容输出到本地或控制台，并支持轮转、日志数、轮转大小设置。
6. 高性能 - 基于go语言的特性，go的goroutine在并发多任务处理能力上有着与生俱来的优势。所以consumer 对每一个获得的可消费分区都会开启一个单独的groutine去执行消费任务，相对比直接使用cpu线程处理，对系统性能消耗更小，效率更高。
7. 使用简单 - 在整个使用过程中，不会产生数据丢失，以及重复，用户只需要配置简单配置文件，创建消费者实例，写处理日志的代码就可以，用户只需要把重心放到自己的消费逻辑上面即可，不需关心消费断点保存，以及错误重试等问题。

## 功能优势

使用consumer library 相对于直接通过 API 或 SDK 从 LogStore 拉取数据进行消费会有如下优势。

- 用户可以创建多个消费者对同一Logstore中的数据进行消费，而且不用关心消费者之间的负载均衡，consumer library 会进行自动处理，并且保证数据不会被重复消费。在cpu等资源有限情况下可以尽最大能力去消费logstore中的数据，并且会自动为用户保存消费断点到服务端。
- 当网络不稳定出现网络震荡时，consumer library可以在网络恢复时继续消费并且保证数据不会丢失及重复消费。
- 提供了更多高阶用法，使用户可以通过多种方法去调控运行中的consumer library

## 安装

请先克隆代码到自己的GOPATH路径下(源码地址：[aliyun-go-consumer-library](https://github.com/aliyun/aliyun-log-go-sdk))，项目使用了vendor工具管理第三方依赖包，所以克隆下来项目以后无需安装任何第三方工具包。

```shell
git clone git@github.com:aliyun/aliyun-log-go-sdk.git
```

## 原理剖析及快速入门

参考教程: [ALiyun LOG Go Consumer Library 快速入门及原理剖析](https://developer.aliyun.com/article/693820)

## 使用步骤

1.**配置LogHubConfig**

LogHubConfig是提供给用户的配置类，用于配置消费策略，您可以根据不同的需求设定不同的值，各参数含义如其中所示
|参数|含义|详情|
| --- | --- | --- |
|Endpoint|sls的endpoint|必填，如cn-hangzhou.sls.aliyuncs.com|
|AccessKeyId|aliyun的AccessKeyId|当 CredentialsProvider 为 nil 时必填|
|AccessKeySecret|aliyun的AccessKeySecret|当 CredentialsProvider 为 nil 时必填|
|CredentialsProvider|自定义接口|可选，可自定义CredentialsProvider，来提供动态的 AccessKeyId/AccessKeySecret/StsToken，该接口应当缓存 AK，且必须线程安全|
|Project|sls的project信息|必填|
|Logstore|sls的logstore|必填|
|ConsumerGroupName|消费组名称|必填|
|Consumer|消费者名称|必填，sls的consumer需要自行指定，请注意不要重复|
|CursorPosition|消费的点位|必填，支持 1.BEGIN_CURSOR: logstore的开始点位 2. END_CURSOR: logstore的最新数据点位 3.SPECIAL_TIME_CURSOR: 自行设置的unix时间戳|
||sls的logstore|必填|
|HeartbeatIntervalInSecond|心跳的时间间隔|非必填，默认时间为20s, sdk会根据心跳时间与服务器确认alive|
|HeartbeatTimeoutInSecond|心跳的超时间隔|非必填，默认时间为HeartbeatIntervalInSecond的3倍, sdk会根据心跳时间与服务器确认alive，持续心跳失败达到超时时间后后，服务器可重新分配该超时shard|
|DataFetchIntervalInMs|数据默认拉取的间隔|非必填，默认为200ms|
|MaxFetchLogGroupCount|数据一次拉取的log group数量|非必填，默认为1000|
|CursorStartTime|数据点位的时间戳|非必填，CursorPosition为SPECIAL_TIME_CURSOR时需填写|
|InOrder|shard分裂后是否in order消费|非必填，默认为false，当为true时，分裂shard会在老的read only shard消费完后再继续消费|
|Logger|自定义日志Logger|非必填，此logger只用于记录消费者自身状态。如果为 nil，会使用默认的logger。若指定了自定义logger，会忽略 AllowLogLevel、LogFileName、LogMaxSize、LogMaxBackups、LogCompass等参数|
|AllowLogLevel|允许的日志级别|非必填，默认为info，日志级别由低到高为debug, info, warn, error，仅高于此AllowLogLevel的才会被log出来|
|LogFileName|程序运行日志文件名称|非必填，默认为stdout|
|IsJsonType|是否为json类型|非必填，默认为logfmt格式，true时为json格式|
|LogMaxSize|日志文件最大size|非必填，默认为10|
|LogMaxBackups|最大保存的old日志文件|非必填，默认为10|
|LogCompass|日志是否压缩|非必填，默认不压缩，如果压缩为gzip压缩|
|HTTPClient|指定http client|非必填，可指定http client实现一些逻辑，sdk发送http请求会使用这个client|
|SecurityToken|aliyun SecurityToken|非必填，参考https://help.aliyun.com/document_detail/47277.html|
|AutoCommitDisabled|是否禁用sdk自动提交checkpoint|非必填，默认不会禁用|
|AutoCommitIntervalInMS|自动提交checkpoint的时间间隔|非必填，单位为MS，默认时间为60s|
|Query|过滤规则  基于规则消费时必须设置对应规则 如 *| where a = 'xxx'|非必填|

2.**覆写消费逻辑**

```
func process(shardId int, logGroupList *sls.LogGroupList, checkpointTracker CheckPointTracker) (string, error) {
    err := dosomething()
    if err != nil {
        return "", nil
    }
    fmt.Println("shardId %v processing works success", shardId)
    // 标记给CheckPointTracker process已成功，保存存档点，
    // false 标记process已成功，但并不直接写入服务器，等待一定的interval后sdk批量写入 (AutoCommitDisable为false情况SDK会批量写入)
    // true  标记已成功, 且直接写入服务器
    // 推荐大多数场景下使用false即可
    checkpointTracker.SaveCheckPoint(false); // 代表process成功保存存档点，但并不直接写入服务器，等待一定的interval后写入
    // 不需要重置检查点情况下，请返回空字符串，如需要重置检查点，请返回需要重置的检查点游标。
    // 如果需要重置检查点的情况下，比如可以返回checkpointTracker.GetCurrentCursor, current checkpoint即尚未process的这批数据开始的检查点
    // 如果已经返回error的话，无需重置到current checkpoint，代码会继续process这批数据，一般来说返回空即可
    return "", nil
}
```

在实际消费当中，您只需要根据自己的需要重新覆写消费函数process即可，上图只是一个简单的demo,将consumer获取到的日志进行了打印处理，注意，该函数参数和返回值不可改变，否则会导致消费失败。
另外的，如果你在process时有特别的需求，比如process暂存，实际异步操作，这里可以实现自己的Processor接口，除了Process函数，可以实现Shutdown函数对异步操作等进行优雅退出。
但是，请注意，checkpoint tracker是线程不安全的，它仅可负责本次process的checkpoint保存，请不要保存起来这个实例异步进行save！
```
type Processor interface {
	Process(int, *sls.LogGroupList, CheckPointTracker) string
	Shutdown(CheckPointTracker) error
}

```

3.**创建消费者并开始消费**

```
// option是LogHubConfig的实例
consumerWorker := consumerLibrary.InitConsumerWorkerWithCheckpointTracker(option, process)
// 如果实现了自己的processor，可以使用下面的语句
// consumerWroer := consumerLibrary.InitConsumerWorkerWithProcessor(option, myProcessor)
// 调用Start方法开始消费
consumerWorker.Start()
```
> 注意目前已废弃`InitConsumerWorker(option, process)`，其代表在process函数后，sdk会执行一次`checkpointTracker.SaveCheckPoint(false)`，但是无法手动强制写入服务器/获取上一个的checkpoint等功能

调用InitConsumerWorkwer方法，将配置实例对象和消费函数传递到参数中生成消费者实例对象,调用Start方法进行消费。

4.**关闭消费者**

```
ch := make(chan os.Signal, 1) //将os信号值作为信道
signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT)
consumerWorker.Start() 
if _, ok := <-ch; ok { // 当获取到os停止信号以后，例如ctrl+c触发的os信号，会调用消费者退出方法进行退出。
    consumerWorker.StopAndWait() 
}
```

上图中的例子通过go的信道做了os信号的监听，当监听到用户触发了os退出信号以后，调用StopAndWait()方法进行退出，用户可以根据自己的需要设计自己的退出逻辑，只需要调用StopAndWait()即可。


## 简单样例

为了方便用户可以更快速的上手consumer library 我们提供了两个简单的通过代码操作consumer library的简单样例，请参考[consumer library example](https://github.com/aliyun/aliyun-log-go-sdk/tree/master/example/consumer)

## 问题反馈
如果您在使用过程中遇到了问题，可以创建 [GitHub Issue](https://github.com/aliyun/aliyun-log-go-sdk/issues) 或者前往阿里云支持中心[提交工单](https://workorder.console.aliyun.com/#/ticket/createIndex)。

## 依赖
使用此扩展，需要设置 go env 中的 CGO_ENABLED=1，且环境中已安装了合适的编译器，linux 上通常是 gcc。  
可以通过以下命令查看当前 CGO_ENABLED 是否打开。

```bash
go env | grep CGO_ENABLED
```
查看默认的编译器。
```bash
go env | grep CC
```

如果 CGO_ENABLED 值是 1，则可跳过下面开启 CGO_ENABLED 的步骤。  

### 全局永久开启
```bash 
go env -w CGO_ENABLED=1
```

### 临时开启
```bash
CGO_ENABLED=1 go build
```

## 使用方法
开启 cgo-zstd 扩展

```golang
import (
    cgo "github.com/aliyun/aliyun-log-go-sdk/cgo"
    sls "github.com/aliyun/aliyun-log-go-sdk"
)
cgo.SetZstdCgoCompressor(1)
```


使用 zstd 压缩写入日志的示例
```golang
import (
	"time"

	cgo "github.com/aliyun/aliyun-log-go-sdk/cgo"
	sls "github.com/aliyun/aliyun-log-go-sdk"
	"github.com/golang/protobuf/proto"
)

func main() {
	cgo.SetZstdCgoCompressor(1)
	client := sls.CreateNormalInterface("endpoint",
		"accessKeyId", "accessKeySecret", "")
	lg := &sls.LogGroup{
		Logs: []*sls.Log{
			{
				Time: proto.Uint32(uint32(time.Now().Unix())),
				Contents: []*sls.LogContent{
					{
						Key:   proto.String("HELLO"),
						Value: proto.String("world"),
					},
				},
			},
		},
	}
	err := client.PostLogStoreLogsV2(
		"your-project",
		"your-logstore",
		&sls.PostLogStoreLogsRequest{
			LogGroup:     lg,
			CompressType: sls.Compress_ZSTD, // 指定压缩方式为 ZSTD
		},
	)
	if err != nil {
		panic(err)
	}

}
```
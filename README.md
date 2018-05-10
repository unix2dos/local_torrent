# torrent_client


### dht-server 可以用作所有dht_node的引导节点

```
go run dht-server/main.go
```


### torrent-create 通过文件创建 .torrent 文件

```
go run torrent-create/main.go lihaile > 1.torrent
```


### torrent 客户端 下载文件, 或做种子

```
go run torrent/main.go 1.torrent
```


### torrent-magnet 把种子文件转为磁力链接

```
go run torrent-magnet/main.go < 1.torrent
```
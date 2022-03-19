# bati 

用Rust实现的一个Websocket网关  
***NOTE***: 项目处于开发状态，没有进入稳定版本，不建议现在在生产环境使用

## 整体架构
![avatar](https://github.com/batigo/resource/blob/master/bati-arch.png)

## 名词解释
1. ****Client****: 通过Websocket接入bati的客户端
2. ****Service****: 后端业务实现，比如直播业务、IM业务、群聊室业务等

## Feature
1. 开箱即用，多语言Service和Client SDK
2. 网关层维护群组/Room，对需要群组广播消息场景(直播、群聊室等)有着极佳的性能表现
3. Rust实现，性能、内存表现优秀
4. 支持多Service，理论上一个bati集群能满足业务所有长连接需求

## Client接入
Client和bati之间的消息通信用protobuf协议

### Client协议
Client message proto协议见 [cmsproto](https://github.com/batigo/cmsgproto)

### Client SDK
[Android](https://github.com/batigo/bati-android-sdk) 

[Go](https://github.com/batigo/baticli-go)

其他SDK开发中

## Service接入
Service和bati之间的消息通信用protobuf协议

### Service协议
Service message proto协议见 [smsproto](https://github.com/batigo/smsgproto)

### Service SDK
[Go](https://github.com/batigo/bati-go)

其他SDK开发中

## bati接入示例
[example](https://github.com/batigo/examples)
# Asteroid-MQ

Asteroid-MQ 是一个可嵌入式的消息队列系统，它提供了灵活的消息传递机制和丰富的消息处理模式。

## 特性

- 多种消息传递模式支持
- 主题-兴趣匹配机制
- 灵活的消息状态管理
- 多语言 SDK 支持
- 可嵌入式架构
- 支持 Kubernetes 集群部署

## 核心概念

### Topic（主题队列）
- 每个 Topic 维护自己的消息队列
- 支持阻塞模式：前一个消息被标记为 Resolved 前，后续消息不会被发出
- 支持两种拒绝模式：
  - RejectNew：拒绝新到达的消息
  - DropOld：丢弃队列头部的旧消息

### Node（节点）
- 连接的基本单元
- 可挂载多个端点
- 负责消息的发送和接收

### Endpoint（端点）
- 消息接收的基本单元
- 可订阅多个兴趣（Interest）
- 支持多种状态：Busy、Offline、Available

### 主题-兴趣机制
- 消息可以包含多个主题（Subject）
- 端点可以订阅多个兴趣（Interest）
- 支持 glob 匹配模式，例如：
  - `user/*` 匹配 `user/a`、`user/b`
  - `user/*/quit` 匹配 `user/a/quit`、`user/b/quit`
  - `user/**` 匹配所有 user 下的多级路径

### 消息类型
1. Push：推送到单个感兴趣的端点
2. Online：推送到所有在线且感兴趣的端点
3. Durable：持久化消息，支持配置：
   - 最大接收者数量（可选）
   - 最大存在时间

### 消息状态
- 正常状态：
  - Sent：消息已发送
  - Received：消息已接收
  - Processed：消息已处理
- 异常状态：
  - Unreachable：端点不可达
  - Failed：消息处理失败

## 技术栈

- 后端：Rust
- 异步运行时：Tokio
- 集群支持：Kubernetes
- 序列化：CBOR、JSON、Bincode
- 分布式一致性：OpenRaft

## SDK 支持

| 语言 | 描述 |
|------|------|
| Rust | Rust 客户端 SDK |
| Java | Java 客户端 SDK |
| TypeScript/JavaScript | Web 端 SDK |

## 部署

支持多种部署方式：
- 嵌入式部署
- Kubernetes 集群部署（通过 StatefulSet）

## 构建和运行

项目使用 Cargo 作为包管理器：

```bash
# 构建项目
cargo build

# 运行测试
cargo test

# 运行服务
cargo run --bin asteroid-server
```

## 许可证

MIT/Apache-2.0 双许可证

## 贡献者

- 4t145 (u4t145@163.com)
- RWDai (rwdai@foxmail.com)

## 项目状态

当前版本：0.1.0-alpha.16

这个项目目前处于活跃开发阶段，API 可能会发生变化。欢迎社区贡献和反馈。 
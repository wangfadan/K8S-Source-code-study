# Client-go
## Client-go目录介绍
目录	作用
kubernetes/	ClientSet 入口，生成强类型客户端（最常用）
rest/	REST 客户端底层实现，处理 HTTP 请求
dynamic/	动态客户端，处理任意 CRD 资源
discovery/	API 发现机制（Group/Version/Resource 信息）
informers/	Informer 机制（List-Watch + 本地缓存）
listers/	从 Informer 本地缓存中读取对象的接口
scale/	Deployment/StatefulSet 等伸缩相关接口
applyconfigurations/	Server-Side Apply 配置对象（kubectl apply）
metadata/	通用 metadata 访问层
transport/	HTTP 传输层（认证、TLS、RoundTripper）
util/	工具函数（wait、cert、workqueue 等）
tools/	client-go 提供的工具（cache、record、leaderelection 等）
plugin/	认证插件机制（OpenShift 等插件）
openapi/ / openapi3/	OpenAPI Schema 相关
restmapper/	REST 资源映射
pkg/	通用基础包（version、apis 等）
features/	Feature Gate 定义
testing/	测试辅助（fake client、fixture）
examples/	使用示例
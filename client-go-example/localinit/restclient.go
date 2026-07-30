package localinit

import (
	"context"
	"flag"
	"fmt"
	"path/filepath"

	corev1 "k8s.io/api/core/v1"                                   // 引入 core/v1 资源定义
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"                 // 引入元数据类型
	"k8s.io/client-go/kubernetes/scheme"                          // 引入 K8s 内置 scheme(注册了所有内置类型 + Codecs + ParameterCodec)
	"k8s.io/client-go/rest"                                       // 引入 RESTClient
	"k8s.io/client-go/tools/clientcmd"                            // 引入 kubeconfig 加载工具
	"k8s.io/client-go/util/homedir"                               // 引入获取用户主目录的工具
)

// RestClientway 演示直接使用底层 RESTClient 操作 K8s 资源
// typed client 和 dynamic client 底层都是基于 RESTClient 实现的
// 直接用 RESTClient = 手动拼 URL + 手动序列化,灵活但啰嗦
func RestClientway() {
	// 定义 kubeconfig 路径变量
	var kubeconfig *string
	// 如果用户主目录存在,拼接出 ~/.kube/config 默认路径
	if home := homedir.HomeDir(); home != "" {
		// flag.String 返回 *string,指向默认路径
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to kubeconfig")
	} else {
		// 主目录为空时,默认路径设为空字符串
		kubeconfig = flag.String("kubeconfig", "", "absolute path to kubeconfig")
	}
	// 解析命令行参数
	flag.Parse()

	// 从 kubeconfig 文件构建 rest.Config 配置对象
	// rest.Config 是所有 client(typed/dynamic/rest)的共同基础
	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		// 构建失败直接 panic
		panic(err)
	}

	// 设置 GroupVersion,告诉 RESTClient 我们要操作 core/v1 组
	// 这决定了 URL 前缀:core 组 → /api/v1,其他组 → /apis/<group>/<version>
	config.GroupVersion = &corev1.SchemeGroupVersion

	// 设置序列化器
	// scheme.Codecs 是一个 CodecFactory,知道如何把 Go 对象序列化成 JSON 发出去,以及把返回的 JSON 反序列化回 Go 对象
	// WithoutConversion() 返回一个不做版本转换的 NegotiatedSerializer
	config.NegotiatedSerializer = scheme.Codecs.WithoutConversion()

	// 对于 core/v1 组,APIPath 应为 /api;其他组为 /apis
	// 显式设置可以避免歧义
	config.APIPath = "/api"

	// 创建 RESTClient 实例
	// RESTClient 是 typed client 和 dynamic client 的底层实现
	client, err := rest.RESTClientFor(config)
	if err != nil {
		panic(err)
	}

	// 准备一个 PodList 结构体来接收 API 返回结果
	podList := &corev1.PodList{}

	// 使用 RESTClient 的 Builder 模式直接发起 GET 请求
	// 等价于:GET /api/v1/namespaces/default/pods?limit=10
	err = client.Get().
		Resource("pods").                                    // 指定资源类型为 pods
		Namespace("default").                                // 指定命名空间为 default
		VersionedParams(&metav1.ListOptions{Limit: 10}, scheme.ParameterCodec). // 附带查询参数,限制返回10条
		Do(context.TODO()).                                  // 执行请求
		Into(podList)                                        // 将响应体反序列化到 podList
	if err != nil {
		panic(err)
	}

	// 打印查询结果
	fmt.Printf("Found %d pods in default namespace:\n", len(podList.Items))
	for _, pod := range podList.Items {
		// 逐行打印每个 Pod 的名称
		fmt.Printf("  - %s\n", pod.Name)
	}
}

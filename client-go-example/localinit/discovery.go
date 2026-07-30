package localinit

// typed和dynmaic是操作资源 discovery 是查询信息
import (
	"flag"
	"fmt"
	"path/filepath"

	"github.com/davecgh/go-spew/spew"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

func Dsicovery() {
	var kubeconfig *string
	if home := homedir.HomeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}
	flag.Parse()

	// use the current context in kubeconfig
	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		panic(err.Error())
	}

	// *查询服务端支持的组列表
	discoveryClient := discovery.NewDiscoveryClientForConfigOrDie(config)
	gs, err := discoveryClient.ServerGroups()
	if err != nil {
		panic(err.Error())
	}
	spew.Dump(gs.Groups)

	// *查询core/v1 组下的资源列表
	rs, err := discoveryClient.ServerResourcesForGroupVersion("v1")
	if err != nil {
		panic(err.Error())
	}
	for _, r := range rs.APIResources {
		fmt.Println(r.Name)
	}

}

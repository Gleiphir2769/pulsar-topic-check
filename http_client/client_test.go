package http_client

import (
	"fmt"
	"testing"
	"time"
)

func TestClient(t *testing.T) {
	fmt.Println(CreateTopic("http://10.105.4.96:8080/admin/v2/persistent/shenjiaqi/check-namespace-1", fmt.Sprintf("check-topic-%d", time.Now().UnixMicro()), "p"))
}

func TestDeleteTopic(t *testing.T) {
	fmt.Println(DeleteTopic("http://10.105.4.96:8080/admin/v2/persistent/shenjiaqi/check-namespace-6", "check-topic-1646185888459527", "p"))
}

//
//func TestCreateNamespace(t *testing.T) {
//	fmt.Println(CreateNamespace("http://10.105.4.96:8080/admin/v2/namespaces/shenjiaqi/my-namespace", "my-shenjiaqi"))
//}

func TestDeleteNamespace(t *testing.T) {
	fmt.Println(DeleteNamespace("http://10.105.4.96:8080/admin/v2/namespaces/shenjiaqi/check-namespace-0?force=true"))
}

package http_client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)

const Token = "Bearer eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJhZG1pbiJ9.dK_KJT4AqzwrjTrZCsgcI4hFD87eD7n8Qv-RTlgNbvA-eWdyXJ1kLxLwOgXoNaNur1LaJ8T77AB0rEBh9yyhZV-e30nij7F2bvsg2WnkOkEzMzCun2GF0r6SpLhhEEcPn8yCeH9v3dO0hm--u_a7nPKoH-4kx1tgSIQML4sB7sk2aXu8fPiQUN4hsR2UoteNnuP-eodwfJlzPUKQCHRCy4600c855mGNzrCxfCWe1pYqzX8Tr2nu8krD8aE3K6e8DpkESLfuXsQAy1thiMCgf3OB4rMf2IW743hqe_F8s5oYGNx9iIcvagXPzxaj7COyGcFOJXQFM3ui_nj7jzwKuQ"

var Client *http.Client

func init() {
	Client = &http.Client{
		Timeout: time.Second * 100,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			// you can check old responses for a status code
			if len(via) != 0 {
				req.Header.Add("Authorization", Token)
			}
			return nil
		},
	}
}

// GetTopic pp = persistent partitioned; p = persistent non-partitioned; np = non-persistent partitioned; n = non-persistent non-partitioned
func GetTopic(serviceUrl string, mode string) ([]string, error) {
	var url string
	switch mode {
	case "pp":
		{
			url = fmt.Sprintf("%s/partitioned", serviceUrl)
		}
	case "p":
		{
			url = serviceUrl
		}
	case "np":
		{
			url = strings.Replace(fmt.Sprintf("%s/partitioned", serviceUrl), "persistent", "non-persistent", 1)
		}
	case "n":
		{
			url = strings.Replace(serviceUrl, "persistent", "non-persistent", 1)
		}
	}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("request for get topic from: %s built failed: %s", url, err)
	}

	req.Header.Set("Authorization", Token)

	resp, err := Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("query namespace: %s failed %s", url, err)
	}
	defer resp.Body.Close()

	response, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("response read failed %s: %s", url, err)
	}
	topics := make([]string, 0)
	err = json.Unmarshal(response, &topics)
	if err != nil {
		return nil, fmt.Errorf("json unmarshal failed %s: %s", url, err)
	}

	return topics, nil
}

func CreateTopic(serviceUrl string, topic string, mode string) ([]string, error) {
	var url string
	switch mode {
	case "pp":
		{
			url = fmt.Sprintf("%s/%s/partitions", serviceUrl, topic)
		}
	case "p":
		{
			url = fmt.Sprintf("%s/%s", serviceUrl, topic)
		}
	case "np":
		{
			url = strings.Replace(fmt.Sprintf("%s/%s/partitions", serviceUrl, topic), "persistent", "non-persistent", 1)
		}
	case "n":
		{
			url = strings.Replace(fmt.Sprintf("%s/%s", serviceUrl, topic), "persistent", "non-persistent", 1)
		}
	}
	var req *http.Request
	var err error
	req, err = http.NewRequest("PUT", url, nil)
	if mode == "pp" || mode == "np" {
		req, err = http.NewRequest("PUT", url, bytes.NewBufferString("1"))
		req.Header.Set("Content-Type", "text/plain")
	}

	if err != nil {
		return nil, fmt.Errorf("request for creaete topic from: %s built failed: %s", url, err)
	}

	req.Header.Set("Authorization", Token)

	resp, err := Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("create topic: %s failed %s", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 204 {
		return nil, fmt.Errorf("craete request: %s error, Code: %d", url, resp.StatusCode)
	}

	return GetTopic(serviceUrl, mode)
}

func CreateNamespace(serviceUrl string) error {
	req, err := http.NewRequest("PUT", serviceUrl, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", Token)
	resp, err := Client.Do(req)
	if err != nil {
		return fmt.Errorf("create namespace: %s failed %s", serviceUrl, err)
	}
	if resp.StatusCode != 204 {
		return fmt.Errorf("craete namespace: %s error, Code: %d", serviceUrl, resp.StatusCode)
	}
	return nil
}

func DeleteNamespace(serviceUrl string) error {
	req, err := http.NewRequest("DELETE", fmt.Sprintf("%s?force=true", serviceUrl), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", Token)
	resp, err := Client.Do(req)
	if err != nil {
		return fmt.Errorf("delete namespace: %s failed %s", serviceUrl, err)
	}
	if resp.StatusCode != 204 {
		return fmt.Errorf("delete namespace: %s request error, Code: %d", serviceUrl, resp.StatusCode)
	}
	return nil
}

func DeleteTopic(serviceUrl string, topic string, mode string) error {
	var url string
	switch mode {
	case "pp":
		{
			url = fmt.Sprintf("%s/%s/partitions", serviceUrl, topic)
		}
	case "p":
		{
			url = fmt.Sprintf("%s/%s", serviceUrl, topic)
		}
	case "np":
		{
			url = strings.Replace(fmt.Sprintf("%s/%s/partitions", serviceUrl, topic), "persistent", "non-persistent", 1)
		}
	case "n":
		{
			url = strings.Replace(fmt.Sprintf("%s/%s", serviceUrl, topic), "persistent", "non-persistent", 1)
		}
	}
	var req *http.Request
	var err error
	req, err = http.NewRequest("DELETE", url, nil)

	if err != nil {
		return fmt.Errorf("request for delete topic from: %s built failed: %s", url, err)
	}

	req.Header.Set("Authorization", Token)

	resp, err := Client.Do(req)
	if err != nil {
		return fmt.Errorf("delete topic: %s failed %s", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 204 {
		return fmt.Errorf("delete request: %s error, Code: %d", url, resp.StatusCode)
	}

	return nil
}

func StringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

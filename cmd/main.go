package main

import (
	"fmt"
	"pulsar-topic-check/http_client"
	"pulsar-topic-check/lib/logger"
	"strings"
	"time"
)

const NamespaceServiceUrl = "http://10.105.4.96:8080/admin/v2/namespaces/shenjiaqi/my-namespace"
const TopicServiceUrl = "http://10.105.4.96:8080/admin/v2/persistent/shenjiaqi/my-namespace"

func main() {
	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "pulsar-topic-check",
		Ext:        "log",
		TimeFormat: "2006-01-02",
	})

	index := 12
	var topicsP []string
	var topicsPP []string
	var topicsN []string
	var topicsNP []string
	for {
		currentNamespace := fmt.Sprintf("check-namespace-%d", index)
		namespaceServiceUrl := strings.Replace(NamespaceServiceUrl, "my-namespace", currentNamespace, 1)
		err := http_client.CreateNamespace(namespaceServiceUrl)
		if err != nil {
			logger.Error(err)
			index++
			time.Sleep(time.Second * 1)
			continue
		}
		topicServiceUrl := strings.Replace(TopicServiceUrl, "my-namespace", currentNamespace, 1)

		logger.Info("create persistent partition topic start")
		for i := 5; i < 10; i++ {
			topicsPP, err = http_client.CreateTopic(topicServiceUrl, fmt.Sprintf("check-topic-%d", time.Now().UnixMicro()), "pp")
			if err != nil {
				logger.Error(err)
			}
			time.Sleep(time.Millisecond * 200)
		}
		logger.Info("create persistent partition topic finish")
		logger.Info("create persistent non-partition topic start")
		for i := 0; i < 5; i++ {
			topicsP, err = http_client.CreateTopic(topicServiceUrl, fmt.Sprintf("check-topic-%d", time.Now().UnixMicro()), "p")
			if err != nil {
				logger.Error(err)
			}
			time.Sleep(time.Millisecond * 200)
		}
		logger.Info("create persistent non-partition topic finish")

		logger.Info("create non-persistent partition topic start")
		for i := 15; i < 20; i++ {
			topicsNP, err = http_client.CreateTopic(topicServiceUrl, fmt.Sprintf("check-topic-%d", time.Now().UnixMicro()), "np")
			if err != nil {
				logger.Error(err)
			}
			time.Sleep(time.Millisecond * 200)
		}
		logger.Info("create non-persistent partition topic finish")

		logger.Info("create non-persistent non-partition topic start")
		for i := 10; i < 15; i++ {
			topicsN, err = http_client.CreateTopic(topicServiceUrl, fmt.Sprintf("check-topic-%d", time.Now().UnixMicro()), "n")
			if err != nil {
				logger.Error(err)
			}
			time.Sleep(time.Millisecond * 200)
		}
		logger.Info("create non-persistent non-partition topic finish")

		failedCount := 0
		lastCount := 0
		// 进行十分钟的压力测试，每十秒测试一轮，每个查询接口测试100次
		for j := 0; j < 60; j++ {
			for i := 0; i < 100; i++ {
				checkedTopicsP, err := http_client.GetTopic(topicServiceUrl, "p")
				if err != nil {
					failedCount++
					logger.Error(fmt.Errorf("check topic failed, count is %d, reason is: %s", failedCount, err))
					continue
				}
				if !http_client.StringSlicesEqual(checkedTopicsP, topicsP) {
					failedCount++
					logger.Error(fmt.Errorf("check topic failed, count is %d, checked topics: %s, expect topics: %s", failedCount, checkedTopicsP, topicsP))
				}
			}
			logger.Info("check persistent non-partition topic finished, fail count: ", failedCount-lastCount)

			for i := 0; i < 100; i++ {
				checkedTopicsPP, err := http_client.GetTopic(topicServiceUrl, "pp")
				if err != nil {
					failedCount++
					logger.Error(fmt.Errorf("check topic failed, count is %d, reason is: %s", failedCount, err))
					continue
				}
				if !http_client.StringSlicesEqual(checkedTopicsPP, topicsPP) {
					failedCount++
					logger.Error(fmt.Errorf("check topic failed, count is %d, checked topics: %s, expect topics: %s", failedCount, checkedTopicsPP, topicsPP))
				}
			}
			logger.Info("check persistent partition topic finished, fail count: ", failedCount-lastCount)

			for i := 0; i < 100; i++ {
				checkedTopicsN, err := http_client.GetTopic(topicServiceUrl, "n")
				if err != nil {
					failedCount++
					logger.Error(fmt.Errorf("check topic failed, count is %d, reason is: %s", failedCount, err))
					continue
				}
				if !http_client.StringSlicesEqual(checkedTopicsN, topicsN) {
					failedCount++
					logger.Error(fmt.Errorf("check topic failed, count is %d, checked topics: %s, expect topics: %s", failedCount, checkedTopicsN, topicsN))
				}
			}
			logger.Info("check non-persistent non-partition topic finished, fail count: ", failedCount-lastCount)

			for i := 0; i < 100; i++ {
				checkedTopicsNP, err := http_client.GetTopic(topicServiceUrl, "np")
				if err != nil {
					failedCount++
					logger.Error(fmt.Errorf("check topic failed, count is %d, reason is: %s", failedCount, err))
					continue
				}
				if !http_client.StringSlicesEqual(checkedTopicsNP, topicsNP) {
					failedCount++
					logger.Error(fmt.Errorf("check topic failed, count is %d, checked topics: %s, expect topics: %s", failedCount, checkedTopicsNP, topicsNP))
				}
			}
			logger.Info("check non-persistent partition topic finished, fail count: ", failedCount-lastCount)

			time.Sleep(time.Second * 10)
		}

		if failedCount == 0 {
			logger.Info(fmt.Sprintf("%s check succeed", topicServiceUrl))
		} else {
			logger.Error(fmt.Sprintf("%s check have failed attemps: %d", topicServiceUrl, failedCount))
		}

		for _, topicP := range topicsP {
			err = http_client.DeleteTopic(topicServiceUrl, GetTopicFromFullName(topicP), "p")
			if err != nil {
				logger.Error(err)
			}
		}

		for _, topicPP := range topicsPP {
			err = http_client.DeleteTopic(topicServiceUrl, GetTopicFromFullName(topicPP), "pp")
			if err != nil {
				logger.Error(err)
			}
		}

		for _, topicN := range topicsN {
			err = http_client.DeleteTopic(topicServiceUrl, GetTopicFromFullName(topicN), "n")
			if err != nil {
				logger.Error(err)
			}
		}

		for _, topicNP := range topicsNP {
			err = http_client.DeleteTopic(topicServiceUrl, GetTopicFromFullName(topicNP), "np")
			if err != nil {
				logger.Error(err)
			}
		}

		err = http_client.DeleteNamespace(namespaceServiceUrl)
		if err != nil {
			logger.Error(err)
		}
		index++
	}
}

func GetTopicFromFullName(topicServiceUrl string) string {
	strs := strings.Split(topicServiceUrl, "/")
	return strs[len(strs)-1]
}

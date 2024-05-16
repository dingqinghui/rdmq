/**
 * @Author: dingQingHui
 * @Description:
 * @File: producer_test
 * @Version: 1.0.0
 * @Date: 2024/5/11 14:50
 */

package rdmq

import (
	"testing"
)

func TestProducer_Publish(t *testing.T) {
	config := NewProducerConfig()
	config.Address = []string{"192.168.1.140:6379"}
	producer := NewProducer(config)
	if err := producer.Publish("topic2", "Hello, World!"); err != nil {
		t.Logf("Publish error: %v", err)
		return
	}
}

func TestProducer_Partition(t *testing.T) {
	config := NewProducerConfig()
	config.Address = []string{"192.168.1.140:6379"}
	config.PartitionNum = 10
	producer := NewProducer(config)
	if err := producer.Publish("topic1", "Hello, World!"); err != nil {
		t.Logf("Publish error: %v", err)
		return
	}
}

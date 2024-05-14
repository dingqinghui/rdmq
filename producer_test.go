/**
 * @Author: dingQingHui
 * @Description:
 * @File: producer_test
 * @Version: 1.0.0
 * @Date: 2024/5/11 14:50
 */

package rdmq

import (
	"github.com/go-redis/redis/v8"
	"testing"
)

func TestProducer_Publish(t *testing.T) {

	rdb := redis.NewClient(&redis.Options{
		Addr:     "192.168.1.140:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	producer := NewProducer(rdb,
		WithProducerName("Producer1"),
		WithProducerPartition(partitionNum),
		WithProducerMaxLen(10000))
	if err := producer.Publish("topic1", "Hello, World!"); err != nil {
		t.Logf("Publish error: %v", err)
		return
	}
}

func TestProducer_Partition(t *testing.T) {
	for i := 0; i < 30; i++ {
		rdb := redis.NewClient(&redis.Options{
			Addr:     "192.168.1.140:6379",
			Password: "", // no password set
			DB:       0,  // use default DB
		})

		producer := NewProducer(rdb,
			WithProducerName("Producer."+itoa(i)),
			WithProducerPartition(5),
			WithProducerMaxLen(10000))
		if err := producer.Publish("partition", "Hello, World!"+itoa(i)); err != nil {
			t.Logf("Publish error: %v", err)
			return
		}
	}
}

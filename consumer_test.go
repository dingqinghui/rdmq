/**
 * @Author: dingQingHui
 * @Description:
 * @File: consumer_test
 * @Version: 1.0.0
 * @Date: 2024/5/11 15:29
 */

package rdmq

import (
	"fmt"
	"github.com/go-redis/redis/v8"
	"testing"
	"time"
)

var partitionNum = 1

func Test_Consumer(t *testing.T) {
	var rdbs []*redis.Client
	for i := 0; i < partitionNum; i++ {
		rdb := redis.NewClient(&redis.Options{
			Addr:     "192.168.1.140:6379",
			Password: "", // no password set
			DB:       0,  // use default DB
		})
		rdbs = append(rdbs, rdb)
	}
	NewConsumer(rdbs, "topic1", WithConsumerCount(10),
		WithConsumerName("consumer1"),
		WithConsumerGroupName("group3"),
		WithConsumerPartition(partitionNum),
		WithConsumerHandler(func(msg interface{}) error {
			fmt.Printf("consumer1 receive message:  %v\n", msg)
			return nil
		}))
	time.Sleep(time.Hour)
}

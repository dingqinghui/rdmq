/**
 * @Author: dingQingHui
 * @Description:
 * @File: consumer_test
 * @Version: 1.0.0
 * @Date: 2024/5/11 15:29
 */

package rdmq

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func Test_Consumer(t *testing.T) {
	config := NewConsumerConfig()
	config.Address = []string{"192.168.1.140:6379"}
	config.Handler = func(ctx context.Context, msg interface{}) error {
		fmt.Printf("consumer1 receive message:  %v\n", msg)
		return nil
	}
	NewConsumer(context.Background(), "topic2", config)
	time.Sleep(time.Hour)
}

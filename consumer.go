/**
 * @Author: dingQingHui
 * @Description:
 * @File: consumer
 * @Version: 1.0.0
 * @Date: 2024/5/11 15:11
 */

package rdmq

import (
	"github.com/go-redis/redis/v8"
)

type ConsumerHandler func(interface{}) error

type ConsumerOptionsFunc func(options *ConsumerOptions)

type ConsumerOptions struct {
	//
	// partition
	// @Description: 分区数量
	//
	partition int
	//
	// group
	// @Description: 分组名字
	//
	group string
	//
	// consumer
	// @Description: 消费者名字
	//
	consumer string
	//
	// count
	// @Description:  每次消费数量
	//
	count int64
	//
	// handler
	// @Description:  处理函数
	//
	handler ConsumerHandler
}

func WithConsumerPartition(partition int) ConsumerOptionsFunc {
	return func(options *ConsumerOptions) {
		options.partition = partition
	}
}

func WithConsumerGroupName(group string) ConsumerOptionsFunc {
	return func(options *ConsumerOptions) {
		options.group = group
	}
}

func WithConsumerName(consumer string) ConsumerOptionsFunc {
	return func(options *ConsumerOptions) {
		options.consumer = consumer
	}
}

func WithConsumerCount(count int64) ConsumerOptionsFunc {
	return func(options *ConsumerOptions) {
		options.count = count
	}
}

func WithConsumerHandler(handler ConsumerHandler) ConsumerOptionsFunc {
	return func(options *ConsumerOptions) {
		options.handler = handler
	}
}

type Consumer struct {
	rdbs  []*redis.Client
	opts  *ConsumerOptions
	topic string
}

func NewConsumer(rdbs []*redis.Client, topic string, opts ...ConsumerOptionsFunc) *Consumer {
	c := &Consumer{
		rdbs:  rdbs,
		topic: topic,
		opts:  &ConsumerOptions{},
	}
	for _, opt := range opts {
		opt(c.opts)
	}
	assert(len(rdbs) == c.opts.partition)
	c.run()
	return c
}

func (c *Consumer) run() {
	for i := 0; i < c.opts.partition; i++ {
		p := newPartition(c.rdbs[i], c.opts, buildSteam(c.topic, uint32(i)))
		go p.run()
	}
}

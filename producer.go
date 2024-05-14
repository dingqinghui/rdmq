/**
 * @Author: dingQingHui
 * @Description:
 * @File: producer
 * @Version: 1.0.0
 * @Date: 2024/5/11 14:06
 */

package rdmq

import (
	"github.com/go-redis/redis/v8"
)

type ProducerOptionsFunc func(*ProducerOptions)

type ProducerOptions struct {
	name      string
	maxLen    int64
	partition int
}

func WithProducerName(name string) ProducerOptionsFunc {
	return func(o *ProducerOptions) {
		o.name = name
	}
}

func WithProducerMaxLen(maxLen int64) ProducerOptionsFunc {
	return func(o *ProducerOptions) {
		o.maxLen = maxLen
	}
}

func WithProducerPartition(partition int) ProducerOptionsFunc {
	return func(o *ProducerOptions) {
		o.partition = partition
	}
}

func NewProducer(rdb *redis.Client, opts ...ProducerOptionsFunc) *Producer {
	assert(rdb != nil)
	p := &Producer{
		rdb:  rdb,
		opts: &ProducerOptions{},
	}
	for _, opt := range opts {
		opt(p.opts)
	}
	return p
}

type Producer struct {
	rdb  *redis.Client
	opts *ProducerOptions
}

func (p *Producer) Publish(topic string, msgs ...string) error {
	// 通过producer name  hash 值计算消息分区ID
	partitionId := consistentHashString(p.opts.name, uint32(p.opts.partition))
	// 生成流名字
	stream := buildSteam(topic, partitionId)
	// 打包多个消息
	values := make(map[string]interface{})
	for i, msg := range msgs {
		values[itoa(i)] = msg
	}
	// 写入消息
	if _, err := p.rdb.XAdd(p.rdb.Context(), &redis.XAddArgs{
		ID:     "*", // redis自动生成消息ID
		Stream: stream,
		Values: values,
		MaxLen: p.opts.maxLen,
	}).Result(); err != nil {
		return err
	}
	return nil
}

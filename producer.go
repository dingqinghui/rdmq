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

func NewProducer(config *ProducerConfig) *Producer {
	assert(config != nil)
	p := &Producer{
		config: config,
		rdb:    newClient(config.Address, config.Password),
	}
	return p
}

type Producer struct {
	config *ProducerConfig
	rdb    *client
}

func (p *Producer) Publish(topic string, msgs ...string) error {
	// 通过producer name  hash 值计算消息分区ID
	partitionId := consistentHashString(p.config.Name, uint32(p.config.PartitionNum))
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
		MaxLen: p.config.MaxLen,
	}).Result(); err != nil {
		return err
	}
	return nil
}

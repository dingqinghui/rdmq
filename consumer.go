/**
 * @Author: dingQingHui
 * @Description:
 * @File: consumer
 * @Version: 1.0.0
 * @Date: 2024/5/11 15:11
 */

package rdmq

import "context"

type Consumer struct {
	config *ConsumerConfig
	topic  string
	rdb    *client
}

func NewConsumer(ctx context.Context, topic string, config *ConsumerConfig) *Consumer {
	assert(config != nil)
	c := &Consumer{
		config: config,
		topic:  topic,
		rdb:    newClient(config.Address, config.Password),
	}
	c.run(ctx)
	return c
}

func (c *Consumer) run(ctx context.Context) {
	for i := 0; i < c.config.PartitionNum; i++ {
		p := newPartition(c.rdb, c.config, buildSteam(c.topic, uint32(i)))
		go p.run(ctx)
	}
}

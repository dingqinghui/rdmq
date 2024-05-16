/**
 * @Author: dingQingHui
 * @Description:
 * @File: partition
 * @Version: 1.0.0
 * @Date: 2024/5/11 17:30
 */

package rdmq

import (
	"context"
	"github.com/go-redis/redis/v8"
	"time"
)

type partition struct {
	rdb     *client
	config  *ConsumerConfig
	id      uint32
	stream  string
	pending []string // 处理完成需要ack的消息
}

func newPartition(rdb *client, config *ConsumerConfig, stream string) *partition {
	p := &partition{rdb: rdb, config: config, stream: stream}
	return p
}

func (p *partition) run(ctx context.Context) {
	if err := p.xGroupCreate(ctx); err != nil {
		debugLog("xGroupCreate error: %v\n", err)
		return
	}
	if err := p.process(ctx); err != nil {
		debugLog("xGroupCreate error: %v\n", err)
		return
	}
	return
}

func (p *partition) xGroupCreate(ctx context.Context) error {
	// 0-0 表示从头开始消费
	// $  表示从最新的消息开始消费
	err := p.rdb.XGroupCreateMkStream(ctx, p.stream, p.config.Group, "0-0").Err()
	if err != nil && (err.Error() != "BUSYGROUP Consumer Group name already exists") {
		return err
	}
	return nil
}

func (p *partition) process(ctx context.Context) error {
	for {
		select {
		case _ = <-ctx.Done():
			return nil
		default:
		}
		// 0:是从pending-list中的第一个消息开始
		if n, err := p.receive(ctx, "0", time.Millisecond); err != nil {
			return err
		} else {
			// pending消息未处理完
			if n >= int(p.config.Count) {
				continue
			}
		}
		// ">"：从下一个未消费的消息开始
		if _, err := p.receive(ctx, ">", time.Duration(0)); err != nil {
			return err
		}
	}
}

// receive
// @Description: 收到消息数量
// @receiver p
// @param startPos
// @param block
// @return bool
// @return error
func (p *partition) receive(ctx context.Context, startPos string, block time.Duration) (int, error) {
	var xStream []redis.XStream
	var err error
	// 读取消息
	if xStream, err = p.xReadGroup(ctx, startPos, block); err != nil {
		return 0, err
	}
	// 处理消息
	n, err := p.handle(ctx, xStream)
	if err != nil {
		return n, err
	}
	// 确认消息
	if err = p.xAck(ctx); err != nil {
		return n, err
	}
	return n, nil
}

func (p *partition) xReadGroup(ctx context.Context, startPos string, block time.Duration) ([]redis.XStream, error) {
	xStream, err := p.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    p.config.Group,
		Streams:  []string{p.stream, startPos},
		Consumer: p.config.Consumer,
		Count:    p.config.Count,
		Block:    block,
	}).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}
	return xStream, nil
}

func (p *partition) handle(ctx context.Context, xStream []redis.XStream) (int, error) {
	var n = 0
	if p.config.Handler == nil {
		return n, nil
	}
	if len(xStream) <= 0 {
		return n, nil
	}
	stream := xStream[0]
	// 遍历所有消息
	for _, message := range stream.Messages {
		key := 0
		// 遍历消息所有field
		for {
			v, ok := message.Values[itoa(key)]
			if !ok {
				break
			}
			n++
			if err := p.config.Handler(ctx, v); err != nil {
				return n, err
			}
			p.pending = append(p.pending, message.ID)
			key++
		}
	}
	return n, nil
}

func (p *partition) xAck(ctx context.Context) error {
	if len(p.pending) <= 0 {
		return nil
	}
	if err := p.rdb.XAck(ctx, p.stream, p.config.Group, p.pending...).Err(); err != nil {
		return err
	}
	p.pending = nil
	return nil
}

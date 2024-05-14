/**
 * @Author: dingQingHui
 * @Description:
 * @File: tool
 * @Version: 1.0.0
 * @Date: 2024/5/11 14:47
 */

package rdmq

import (
	"fmt"
	"hash/crc32"
)

func consistentHashString(s string, div uint32) uint32 {
	if div <= 0 {
		return 0
	}
	return crc32.ChecksumIEEE([]byte(s)) % div
}

func assert(expression bool) {
	if !expression {
		panic("assert failed")
	}
}

func itoa(num interface{}) string {
	return fmt.Sprintf("%d", num)
}

func buildSteam(topic string, partitionId uint32) string {
	return fmt.Sprintf("%s.{%d}", topic, partitionId)
}

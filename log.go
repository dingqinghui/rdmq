/**
 * @Author: dingQingHui
 * @Description:
 * @File: log
 * @Version: 1.0.0
 * @Date: 2024/5/14 17:09
 */

package rdmq

import "fmt"

func debugLog(format string, a ...any) {
	fmt.Printf(format, a...)
}

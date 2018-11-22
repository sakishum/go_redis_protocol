package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"time"
)

const (
	// TODO, Read Redis config with .ini 
	RedisServerAddress = "127.0.0.1:6379"
	RedisServerNetwork = "tcp"
	RedisServerAuth    = "admin"
)

const (
	ErrorReply  = '-'
	StatusReply = '+'
	IntReply    = ':'
	StringReply = '$'
	ArrayReply  = '*'
)

//const Nil = RedisError("redis: nil")
//type RedisError string // Redis Error
//func (e RedisError) Error() string {
//	return string(e)
//}

type RedisError struct {
	msg string
}

func (this *RedisError) Error() string {
	return this.msg
}

// 连接到 redis server
func conn() (net.Conn, error) {
	conn, err := net.Dial(RedisServerNetwork, RedisServerAddress)
	if err != nil {
		fmt.Println(err.Error())
	}
	return conn, err
}

// 将参数转化为 Redis 请求协议
func getCmd(argv []string) []byte {
	cmdString := "*" + strconv.Itoa(len(argv)) + "\r\n"
	for _, v := range argv {
		cmdString += "$" + strconv.Itoa(len(v)) + "\r\n" + v + "\r\n"
	}
	cmdByte := make([]byte, len(cmdString))
	copy(cmdByte[:], cmdString)
	return cmdByte
}

func dealReply(reply []byte) (interface{}, error) {
	responseType := reply[0]
	fmt.Println("Type:", string(responseType))
	switch responseType {
	case StatusReply: // 状态响应(status reply)
		return dealStatusReply(reply)
	case StringReply: // 主体响应(bulk reply)
		return dealBulkReply(reply)
	case IntReply: // 整数响应(integer reply)
		return dealIntegerReply(reply)
	case ErrorReply: // 错误响应(error reply)
		return dealErrorReply(reply)
	//case ArrayReply: // TODO: 批量主体响应(array reply)
	// return dealArrayReply(reply)
	default:
		//return nil, RedisError("protocol wrong!")
		return nil, &RedisError{"protocol wrong!"}
	}
}

// 处理状态响应
func dealStatusReply(reply []byte) (interface{}, error) {
	statusByte := reply[1:]
	pos := 0
	for _, v := range statusByte {
		if v == '\r' {
			break
		}
		pos++
	}
	status := statusByte[:pos]
	return string(status), nil
}

// 处理整数响应
func dealIntegerReply(reply []byte) (interface{}, error) {
	integerByte := reply[1:]
	pos := 0
	for _, v := range integerByte {
		if v == '\r' {
			break
		}
		pos++
	}
	integer := integerByte[:pos]
	return string(integer), nil
}

// 处理错误响应
func dealErrorReply(reply []byte) (interface{}, error) {
	errorByte := reply[1:]
	pos := 0
	for _, v := range errorByte {
		if v == '\r' {
			break
		}
		pos++
	}
	err := errorByte[:pos]
	return string(err), nil
}

// 处理主体响应
func dealBulkReply(reply []byte) (interface{}, error) {
	statusByte := reply[1:]
	// 获得响应文本第一行标识的响应字符串长度
	pos := 0
	for _, v := range statusByte {
		if v == '\r' {
			break
		}
		pos++
	}
	strlen, err := strconv.Atoi(string(statusByte[:pos]))
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	if strlen == -1 {
		return "nil", nil
	}

	nextLinePost := 1
	for _, v := range statusByte {
		if v == '\n' {
			break
		}
		nextLinePost++
	}

	result := string(statusByte[nextLinePost : nextLinePost+strlen])
	return result, nil
}

func main() {
	args := os.Args[1:]
	if len(args) == 0 {
		fmt.Println("usage: go run proto.go + redis command\nfor example:\ngo run proto.go PING")
		os.Exit(1)
	}

	conn, err := conn()
	if err != nil {
		os.Exit(1)
	}
	defer conn.Close()
	cmd := getCmd(args)

	t1 := time.Now() // get current time
	authStr := make([]string, 2)
	authStr[0] = "auth"
	authStr[1] = RedisServerAuth
	authCmd := getCmd(authStr)
	conn.Write(authCmd)
	loginBuff := make([]byte, 1024)
	len, _ := conn.Read(loginBuff)
	loginRes, err := dealReply(loginBuff[:len])
	if err != nil {
		fmt.Println("Redis access denied. PLS retry.")
		os.Exit(1)
	}
	fmt.Println("Redis 登录校验返回的结果是: ", loginRes)

	conn.Write(cmd)
	buff := make([]byte, 1024)
	n, _ := conn.Read(buff)
	res, _ := dealReply(buff[:n])
	fmt.Println("Redis 返回的结果是: ", res)
	elapsed := time.Since(t1)
	fmt.Println("App elapsed: ", elapsed)
}

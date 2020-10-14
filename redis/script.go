// Copyright 2012 Gary Burd
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package redis

import (
	"crypto/sha1"
	"encoding/hex"
	"io"
	"strings"
)

// eval 命令 执行脚本 lua
// evalsha 执行脚本 由hash指定的脚本
// script debug 之后的脚本执行开启debug模式
// script exists 判断脚本是否在redis脚本缓存内
// script flush 清空缓存
// script kill 终止当前脚本的执行
// script load 加载脚本到脚本缓存
// Script encapsulates the source, hash and key count for a Lua script.
// See todo http://redis.io/commands/eval for information on scripts in Redis.
type Script struct {
	keyCount int
	src      string // 脚本代码
	hash     string // 脚本hash值, 防篡改
}

// NewScript returns a new script object. If keyCount is greater than or equal
// to zero, then the count is automatically inserted in the EVAL command
// argument list. If keyCount is less than zero, then the application supplies
// the count as the first value in the keysAndArgs argument to the Do, Send and
// SendHash methods.
func NewScript(keyCount int, src string) *Script {
	h := sha1.New()
	io.WriteString(h, src)
	return &Script{keyCount, src, hex.EncodeToString(h.Sum(nil))}
}

func (s *Script) args(spec string, keysAndArgs []interface{}) []interface{} {
	var args []interface{}
	if s.keyCount < 0 {
		args = make([]interface{}, 1+len(keysAndArgs))
		args[0] = spec
		copy(args[1:], keysAndArgs)
	} else {
		args = make([]interface{}, 2+len(keysAndArgs))
		args[0] = spec
		args[1] = s.keyCount
		copy(args[2:], keysAndArgs)
	}
	return args
}

// Hash returns the script hash.
func (s *Script) Hash() string {
	return s.hash
}

// Do evaluates the script. Under the covers, Do optimistically evaluates the
// script using the EVALSHA command. If the command fails because the script is
// not loaded, then Do evaluates the script using the EVAL command (thus
// causing the script to load).
func (s *Script) Do(c Conn, keysAndArgs ...interface{}) (interface{}, error) {
	// 先执行hash对应的脚本
	v, err := c.Do("EVALSHA", s.args(s.hash, keysAndArgs)...)
	if e, ok := err.(Error); ok && strings.HasPrefix(string(e), "NOSCRIPT ") { // 无缓存
		// 执行脚本
		v, err = c.Do("EVAL", s.args(s.src, keysAndArgs)...)
	}
	return v, err
}

// SendHash evaluates the script without waiting for the reply. The script is
// evaluated with the EVALSHA command. The application must ensure that the
// script is loaded by a previous call to Send, Do or Load methods.
func (s *Script) SendHash(c Conn, keysAndArgs ...interface{}) error {
	return c.Send("EVALSHA", s.args(s.hash, keysAndArgs)...)
}

// Send evaluates the script without waiting for the reply.
func (s *Script) Send(c Conn, keysAndArgs ...interface{}) error {
	return c.Send("EVAL", s.args(s.src, keysAndArgs)...)
}

// Load loads the script without evaluating it.
// 将脚本缓存到服务器
func (s *Script) Load(c Conn) error {
	_, err := c.Do("SCRIPT", "LOAD", s.src)
	return err
}

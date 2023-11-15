package shardkv

import (
	"crypto/rand"
	"math/big"

	"6.5840/shardctrler"
)

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// 判断是否是重复的
func (kv *ShardKV) ifDuplicate(clientId int64, seqId int) bool {
	// 本方法不加锁，建议调用该方法时持有锁
	lastSeqId, exists := kv.seqMap[clientId]
	if !exists {
		return false
	}
	return seqId <= lastSeqId
}

// get wait chan at index
func (kv *ShardKV) getWaitCh(index int) chan CommandReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, ok := kv.chanMap[index]
	if !ok {
		// （*）这里设置一个有1缓冲的，否则死锁
		kv.chanMap[index] = make(chan CommandReply, 1) // 内存泄漏？不会，因为会关闭通道
		ch = kv.chanMap[index]
	}
	return ch
}

// close chan at index and delete it
func (kv *ShardKV) closeAndDelete(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// 为避免panic: send on closed channel，不手动关闭通道
	// close(kv.chanMap[index]) // chan应该仅由发送方关闭（*）
	delete(kv.chanMap, index)
}

func (kv *ShardKV) cloneShard(configNum int, kvMap map[string]string) Shard {
	// 本方法不加锁，建议调用该方法时持有锁
	migrateShard := Shard{
		ConfigNum: configNum,
		KvMap:     make(map[string]string),
	}
	for k, v := range kvMap {
		migrateShard.KvMap[k] = v
	}
	return migrateShard
}

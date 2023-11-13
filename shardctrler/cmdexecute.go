package shardctrler

import (
	"sort"
)

func (sc *ShardCtrler) execJoin(op Op) {
	sc.rebalanceShardsForJoin(op.Servers)
	sc.logger.Debug("after join, sc.configs.last=%+v", sc.configs[len(sc.configs)-1])
}

func (sc *ShardCtrler) execLeave(op Op) {
	sc.rebalanceShardsForLeave(op.GIDs)
	sc.logger.Debug("after leave, sc.configs.last=%+v", sc.configs[len(sc.configs)-1])
}

func (sc *ShardCtrler) execMove(op Op) {
	if op.Shard < 0 || op.Shard >= NShards {
		return
	}
	sc.moveShared(op.Shard, op.GID)
	sc.logger.Debug("after move, sc.configs.last=%+v", sc.configs[len(sc.configs)-1])
}

func (sc *ShardCtrler) execQuery(op Op) Config {
	num := op.Num
	if num == -1 || num >= len(sc.configs) {
		// 获取最新或者越界？
		return sc.configs[len(sc.configs)-1]
	}
	return sc.configs[num]
}

// For Join
func (sc *ShardCtrler) rebalanceShardsForJoin(Servers map[int][]string) {
	// Shards数量固定为NShards
	// 需要将NShards个Shards尽可能均匀分布在NGroups个组中
	// 不同Raft节点执行同一个函数，得到的结果相同？即该函数是否具有随机性
	// 考虑到对map进行range key时顺序是不确定的，因此具有随机性
	// 获取旧配置
	oldConfig := sc.configs[len(sc.configs)-1]
	// 定义新配置
	newConfig := Config{
		Num: oldConfig.Num + 1,
	}
	// 分配新的Shards，要求尽量少移动
	NGroups := len(oldConfig.Groups) + len(Servers)
	m := NShards / NGroups // 平均每组多少个Shards
	n := NShards % NGroups // 余数
	gids := make([]int, 0, NGroups)
	for k := range oldConfig.Groups {
		gids = append(gids, k)
	}
	for k := range Servers {
		gids = append(gids, k)
	}
	sort.Ints(gids)
	newShards := make([]int, 0)
	for _, gid := range gids {
		for i := 0; i < m; i++ {
			newShards = append(newShards, gid)
		}
		if n > 0 {
			n--
			newShards = append(newShards, gid)
		}
	}
	newConfig.Shards = [NShards]int(newShards)
	// 分配新的Groups
	newGroups := make(map[int][]string)
	for gid := range oldConfig.Groups {
		newGroups[gid] = oldConfig.Groups[gid]
	}
	for gid := range Servers {
		newGroups[gid] = Servers[gid]
	}
	newConfig.Groups = newGroups
	//
	// sc.logger.Debug("oldConfig=%+v", oldConfig)
	// sc.logger.Debug("newConfig=%+v", newConfig)
	sc.configs = append(sc.configs, newConfig)
}

// For Leave
func (sc *ShardCtrler) rebalanceShardsForLeave(GIDs []int) {
	// 获取旧配置
	oldConfig := sc.configs[len(sc.configs)-1]
	// 定义新配置
	newConfig := Config{
		Num: oldConfig.Num + 1,
	}
	// 分配新的Groups
	newGroups := make(map[int][]string)
	for gid, servers := range oldConfig.Groups {
		newGroups[gid] = servers
	}
	for _, gid := range GIDs {
		delete(newGroups, gid)
	}
	newConfig.Groups = newGroups
	// 分配新的Shards，要求尽量少移动
	NGroups := len(newGroups)
	newShards := make([]int, 0)
	if NGroups != 0 {
		m := NShards / NGroups // 平均每组多少个Shards
		n := NShards % NGroups // 余数
		gids := make([]int, 0, NGroups)
		for k := range newGroups {
			gids = append(gids, k)
		}
		sort.Ints(gids)
		for _, gid := range gids {
			for i := 0; i < m; i++ {
				newShards = append(newShards, gid)
			}
			if n > 0 {
				n--
				newShards = append(newShards, gid)
			}
		}
	} else {
		newShards = make([]int, NShards)
	}
	newConfig.Shards = [NShards]int(newShards)
	//
	sc.configs = append(sc.configs, newConfig)
}

// For Move
func (sc *ShardCtrler) moveShared(Shard, GID int) {
	// 获取旧配置
	oldConfig := sc.configs[len(sc.configs)-1]
	// 定义新配置
	newConfig := Config{
		Num: oldConfig.Num + 1,
	}
	// 分配新的Shards，要求尽量少移动
	newShards := oldConfig.Shards
	newShards[Shard] = GID
	newConfig.Shards = newShards
	// 分配新的Groups
	newGroups := make(map[int][]string)
	for _, gid := range newShards {
		if _, exists := oldConfig.Groups[gid]; !exists {
			newGroups[gid] = oldConfig.Groups[gid]
		}
	}
	newConfig.Groups = newGroups
	//
	sc.configs = append(sc.configs, newConfig)
}

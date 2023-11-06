package raft

import (
	"fmt"
	"sync"
	"sync/atomic"

	"6.5840/debugutils"
)

type ApplyHelper struct {
	mu        sync.Mutex
	cond      *sync.Cond
	applyCh   chan ApplyMsg
	queue     []ApplyMsg
	lastIndex int
	dead      int32
	// debugutils
	logger debugutils.Logger
}

func makeApplyHelper(id int, applyChan chan ApplyMsg, lastApplied int) *ApplyHelper {
	applyHelper := &ApplyHelper{
		applyCh:   applyChan,
		lastIndex: lastApplied,
		queue:     make([]ApplyMsg, 0),
		logger:    *debugutils.NewLogger(fmt.Sprintf("ApplyHelper %d", id), debugutils.Slient),
	}
	applyHelper.cond = sync.NewCond(&applyHelper.mu)

	go applyHelper.applier()

	return applyHelper
}

func (ah *ApplyHelper) Kill() {
	atomic.StoreInt32(&ah.dead, 1)
}

func (ah *ApplyHelper) killed() bool {
	z := atomic.LoadInt32(&ah.dead)
	return z == 1
}

func (ah *ApplyHelper) applier() {
	for !ah.killed() {
		ah.mu.Lock()
		if len(ah.queue) == 0 {
			ah.cond.Wait()
		}
		applyMsg := ah.queue[0]
		ah.queue = ah.queue[1:]
		ah.logger.Info("want to apply msg=%+v", applyMsg)
		ah.mu.Unlock()
		ah.applyCh <- applyMsg
		ah.logger.Info("success apply msg=%+v", applyMsg)
	}
}

func (ah *ApplyHelper) tryApply(applyMsg ApplyMsg) {
	ah.mu.Lock()
	defer ah.mu.Unlock()
	ah.logger.Info("receive applyMsg=%+v", applyMsg)
	switch {
	case applyMsg.CommandValid:
		if applyMsg.CommandIndex == ah.lastIndex+1 {
			// 一般情况先判断，减少判断次数
			ah.queue = append(ah.queue, applyMsg)
			ah.lastIndex++
			ah.cond.Broadcast()
		} else if applyMsg.CommandIndex <= ah.lastIndex {
			// old applyMsg
			ah.logger.Warn("old applyMsg=%+v", applyMsg)
		} else {
			ah.logger.Error("ah.lastIndex=[%d]", ah.lastIndex)
			panic("applyHelper receive commandIndex > lastIndex + 1")
		}
	case applyMsg.SnapshotValid:
		if applyMsg.SnapshotIndex >= ah.lastIndex+1 {
			// 一般情况先判断，减少判断次数
			ah.queue = append(ah.queue, applyMsg)
			ah.lastIndex = applyMsg.SnapshotIndex
			ah.cond.Broadcast()
		} else {
			// old applyMsg
			ah.logger.Warn("old applyMsg=%+v", applyMsg)
		}
	default:
		panic("Unknown applyMsg type")
	}
}

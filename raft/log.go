package raft

import (
	"fmt"
	"strconv"
)

func makeEmptyLog() Log {
	log := Log{
		Entries:           make([]Entry, 0),
		LastIncludedIndex: 0,
		LastIncludedTerm:  -1,
	}
	return log
}

type Entry struct {
	Term    int // in which term it was created
	Index   int // entry index
	Command interface{}
}

// 不能用 *Entry 否则对 Entry 不起作用
func (e Entry) String() string {
	if value, ok := e.Command.(int); ok {
		str := strconv.Itoa(value)
		if len(str) > 6 {
			return fmt.Sprintf("{%d %d %.4s..}", e.Term, e.Index, str)
		}
		return fmt.Sprintf("{%d %d %s}", e.Term, e.Index, str)
	} else if value, ok := e.Command.(string); ok {
		if len(value) > 6 {
			return fmt.Sprintf("{%d %d %.4s..}", e.Term, e.Index, value)
		}
		return fmt.Sprintf("{%d %d %s}", e.Term, e.Index, value)
	}
	return fmt.Sprintf("{%d %d %v}", e.Term, e.Index, e.Command)
}

// index项在数组中实际下标为index-(LastIncludedIndex+1)
type Log struct {
	Entries           []Entry
	LastIncludedIndex int
	LastIncludedTerm  int
}

func (l *Log) append(entries ...Entry) {
	l.Entries = append(l.Entries, entries...)
}

// index项在数组中实际下标为index-(LastIncludedIndex+1)
func (l *Log) index(index int) *Entry {
	if index < l.LastIncludedIndex {
		panic("ERROR: index smaller than log snapshot!\n")
	}
	if index > l.LastIncludedIndex+len(l.Entries) {
		panic("ERROR: index greater than log length!\n")
	}
	if index == l.LastIncludedIndex {
		return &Entry{
			Term:    l.LastIncludedTerm,
			Index:   l.LastIncludedIndex,
			Command: nil,
		}
	}
	return &l.Entries[index-l.LastIncludedIndex-1]
}

// return nil if out of range
// return l.Entries[:index-l.Index0]
func (l *Log) before(index int) []Entry {
	if index-l.LastIncludedIndex-1 < 0 || index-l.LastIncludedIndex-1 >= len(l.Entries) {
		return nil
	}
	return l.Entries[:index-l.LastIncludedIndex]
}

// do nothing if out of range
// l.Entries = l.Entries[:index-l.Index0]
func (l *Log) truncateBefore(index int) {
	// 特别注意下标以及函数的表达意思
	// index = 125
	// l.LastIncludedIndex = 119
	// Index0 = 120
	// 0    1    2    3    4    5    6
	// 120, 121, 122, 123, 124, 125, 126
	// 实际下标为index-l.LastIncludedIndex-1
	if index-l.LastIncludedIndex-1 < 0 ||
		index-l.LastIncludedIndex-1 > len(l.Entries) {
		return
	}
	// index-l.LastIncludedIndex-1可以为0
	l.Entries = l.Entries[:index-l.LastIncludedIndex-1]
}

// return nil if out of range
// return l.Entries[index-l.Index0:]
func (l *Log) after(index int) []Entry {
	if index-l.LastIncludedIndex-1 < 0 || index-l.LastIncludedIndex-1 >= len(l.Entries) {
		return nil
	}
	return l.Entries[index-l.LastIncludedIndex-1:]
}

// l.Entries = l.Entries[lastIncludedIndex-l.LastIncludedIndex:]
func (l *Log) truncateAfter(lastIncludedIndex int, lastIncludedTerm int) {
	if lastIncludedIndex-l.LastIncludedIndex-1 < 0 {
		return
	}
	if lastIncludedIndex-l.LastIncludedIndex-1 >= len(l.Entries) {
		// 右边越界则清空
		l.Entries = make([]Entry, 0)
	} else {
		l.Entries = l.Entries[lastIncludedIndex-l.LastIncludedIndex:]
	}
	// set last included
	l.LastIncludedIndex = lastIncludedIndex
	l.LastIncludedTerm = lastIncludedTerm
}

// return nil if left >= right || left-l.Index0 < 0 || right-l.Index0 >= len(l.Entries)
// return l.Entries[left-l.Index0 : right-l.Index0]
func (l *Log) between(left, right int) []Entry {
	if left >= right || left-l.LastIncludedIndex-1 < 0 || right-l.LastIncludedIndex-1 > len(l.Entries) {
		return nil
	}
	return l.Entries[left-l.LastIncludedIndex-1 : right-l.LastIncludedIndex-1]
}

// return last log
func (l *Log) lastLog() Entry {
	if len(l.Entries) <= 0 {
		return Entry{
			Term:    l.LastIncludedTerm,
			Index:   l.LastIncludedIndex,
			Command: -1,
		}
	}
	return l.Entries[len(l.Entries)-1]
}

// return last index
func (l *Log) lastIndex() int {
	if len(l.Entries) <= 0 {
		return l.LastIncludedIndex
	}
	return l.Entries[len(l.Entries)-1].Index
}

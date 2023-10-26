package raft

import (
	"fmt"
	"strings"
)

type Log struct {
	Entries []Entry
	Index0  int // Entries[0].Index, default 0
}

type Entry struct {
	Term    int // in which term it was created
	Index   int // entry index
	Command interface{}
}

func (l *Log) append(entries ...Entry) {
	l.Entries = append(l.Entries, entries...)
	l.Index0 = l.Entries[0].Index
}

func (l *Log) setEntries(entries []Entry) {
	l.Entries = entries
	l.Index0 = l.Entries[0].Index
}

func makeEmptyLog() Log {
	log := Log{
		Entries: make([]Entry, 0),
		Index0:  0,
	}
	return log
}

func (l *Log) at(idx int) *Entry {
	// idx表示 Index为idx 的 Entry
	// idx - Index0 = 在Entries数组中的下标
	// 下同
	// return where Entry.Index == idx
	if idx-l.Index0 < 0 || idx-l.Index0 > l.len()-1 {
		return &Entry{
			Term:    -1,
			Index:   0,
			Command: 0,
		}
	}
	return &l.Entries[idx-l.Index0]
}

func (l *Log) truncate(idx int) {
	l.Entries = l.Entries[:idx-l.Index0]
}

func (l *Log) slice(idx int) []Entry {
	if len(l.Entries) <= idx-l.Index0 {
		return nil
	}
	return l.Entries[idx-l.Index0:]
}

func (l *Log) len() int {
	return len(l.Entries)
}

func (l *Log) lastLog() *Entry {
	if l.len() <= 0 {
		return &Entry{
			Term:    -1,
			Index:   0,
			Command: 0,
		}
	}
	return &l.Entries[l.len()-1]
}

func (e *Entry) String() string {
	return fmt.Sprint(e.Term)
}

func (l *Log) String() string {
	nums := []string{}
	for _, entry := range l.Entries {
		nums = append(nums, fmt.Sprintf("%4d", entry.Term))
	}
	return fmt.Sprint(strings.Join(nums, "|"))
}

package raft

import (
	"fmt"
	"strings"
)

func makeEmptyLog() Log {
	log := Log{
		Entries: make([]Entry, 0),
		Index0:  0,
	}
	return log
}

type Log struct {
	Entries []Entry
	Index0  int // Entries[0].Index, default 0 if not exists
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
	if len(l.Entries) > 0 {
		l.Index0 = l.Entries[0].Index
	} else {
		l.Index0 = 0
	}
}

// return Entry{-1, 0, 0} if not exists
// get entry where entry.Index == index
func (l *Log) get(index int) *Entry {
	// index表示Index为index的Entry，下同
	// idx - Index0 = 在Entries数组中的下标
	if len(l.Entries) == 0 || index-l.Index0 < 0 || index-l.Index0 >= len(l.Entries) {
		return &Entry{
			Term:    -1,
			Index:   0,
			Command: 0,
		}
	}
	return &l.Entries[index-l.Index0]
}

func (l *Log) clean() {
	l.Entries = make([]Entry, 0)
	l.Index0 = 0
}

// return nil if out of range
// return l.Entries[:index-l.Index0]
func (l *Log) before(index int) []Entry {
	if index-l.Index0 < 0 || index-l.Index0 >= len(l.Entries) {
		return nil
	}
	return l.Entries[:index-l.Index0]
}

// do nothing if out of range
// l.Entries = l.Entries[:index-l.Index0]
func (l *Log) truncateBefore(index int) {
	if index-l.Index0 <= 0 || index-l.Index0 > l.len() {
		return
	}
	l.Entries = l.Entries[:index-l.Index0]
}

// return nil if out of range
// return l.Entries[index-l.Index0:]
func (l *Log) after(index int) []Entry {
	if index-l.Index0 < 0 || index-l.Index0 >= len(l.Entries) {
		return nil
	}
	return l.Entries[index-l.Index0:]
}

// do nothing if out of range
// l.Entries = l.Entries[index-l.Index0:]
func (l *Log) truncateAfter(index int) {
	if index-l.Index0 < 0 || index-l.Index0 >= l.len() {
		return
	}
	l.Entries = l.Entries[index-l.Index0:]
	// set l.Index0
	l.Index0 = l.Entries[0].Index
}

// return nil if left >= right || left-l.Index0 < 0 || right-l.Index0 >= len(l.Entries)
// return l.Entries[left-l.Index0 : right-l.Index0]
func (l *Log) between(left, right int) []Entry {
	if left >= right || left-l.Index0 < 0 || right-l.Index0 > len(l.Entries) {
		return nil
	}
	return l.Entries[left-l.Index0 : right-l.Index0]
}

// do nothing if out of range
// l.Entries = l.Entries[left-l.Index0 : right-l.Index0]
func (l *Log) truncateBetween(left, right int) {
	if left >= right || left-l.Index0 < 0 || right-l.Index0 > len(l.Entries) {
		return
	}
	l.Entries = l.Entries[left-l.Index0 : right-l.Index0]
	// set l.Index0
	l.Index0 = l.Entries[0].Index
}

func (l *Log) len() int {
	return len(l.Entries)
}

// TODO return what? if len(l.Entries) == 0
func (l *Log) lastLog() Entry {
	if len(l.Entries) <= 0 {
		return Entry{
			Term:    -1,
			Index:   0,
			Command: 0,
		}
	}
	return l.Entries[len(l.Entries)-1]
}

// func (e *Entry) String() string {
// 	return fmt.Sprint(e.Term)
// }

func (l *Log) String() string {
	nums := []string{}
	for _, entry := range l.Entries {
		nums = append(nums, fmt.Sprintf("%4d", entry.Term))
	}
	return fmt.Sprint(strings.Join(nums, "|"))
}

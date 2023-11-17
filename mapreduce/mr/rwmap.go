package mr

import "sync"

type RWMap[T any] struct {
	sync.RWMutex
	mp map[int]T
}

func NewRWMap[T any]() *RWMap[T] {
	return &RWMap[T]{
		mp: make(map[int]T),
	}
}

func (rwmap *RWMap[T]) Get(key int) T {
	rwmap.RLock()
	defer rwmap.RUnlock()
	value := rwmap.mp[key]
	return value
}

func (rwmap *RWMap[T]) Set(key int, value T) {
	rwmap.Lock()
	defer rwmap.Unlock()
	rwmap.mp[key] = value
}

func (rwmap *RWMap[T]) Delete(key int) {
	rwmap.Lock()
	defer rwmap.Unlock()
	delete(rwmap.mp, key)
}

func (rwmap *RWMap[T]) Len() int {
	rwmap.RLock()
	defer rwmap.RUnlock()
	return len(rwmap.mp)
}

func (rwmap *RWMap[T]) MSet() map[int]T {
	rwmap.RLock()
	defer rwmap.RUnlock()
	return rwmap.mp
}

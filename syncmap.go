package gostatok

import "sync"

type SyncMap[K comparable, V any] struct {
	m sync.Map
}

func (sm *SyncMap[K, V]) Delete(key K) { sm.m.Delete(key) }

func (sm *SyncMap[K, V]) Load(key K) (value V, ok bool) {
	v, ok := sm.m.Load(key)
	if !ok {
		return value, ok
	}
	return v.(V), ok
}

func (sm *SyncMap[K, V]) Has(key K) bool {
	_, ok := sm.m.Load(key)
	return ok
}

func (sm *SyncMap[K, V]) Get(key K) V {
	v, ok := sm.m.Load(key)
	if ok {
		return v.(V)
	} else {
		var zero V
		return zero
	}
}

func (sm *SyncMap[K, V]) Clear() {
	sm.m.Range(func(key, value any) bool {
		sm.m.Delete(key)
		return true
	})
}

func (sm *SyncMap[K, V]) LoadAndDelete(key K) (value V, loaded bool) {
	v, loaded := sm.m.LoadAndDelete(key)
	if !loaded {
		return value, loaded
	}
	return v.(V), loaded
}

func (sm *SyncMap[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	a, loaded := sm.m.LoadOrStore(key, value)
	return a.(V), loaded
}

func (sm *SyncMap[K, V]) Range(f func(key K, value V) bool) {
	sm.m.Range(func(key, value any) bool { return f(key.(K), value.(V)) })
}

func (sm *SyncMap[K, V]) Set(key K, value V) { sm.m.Store(key, value) }

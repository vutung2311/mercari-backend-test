package mydb

import "sync"

type readReplica struct {
	BackendSQL
	online bool
	mutex  *sync.RWMutex
}

func (r *readReplica) isOnline() bool {
	// Master node will have null mutex since its status won't be change
	if r.mutex == nil {
		// Master node will always be online
		return true
	}
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	return r.online
}

func (r *readReplica) setOffline() {
	if r.mutex == nil {
		return
	}
	r.mutex.Lock()
	defer r.mutex.Unlock()

	r.online = false
}

func (r *readReplica) setOnline() {
	if r.mutex == nil {
		return
	}
	r.mutex.Lock()
	defer r.mutex.Unlock()

	r.online = true
}

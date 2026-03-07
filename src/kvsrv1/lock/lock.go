package lock

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck  kvtest.IKVClerk
	key string
	id  string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
		ck:  ck,
		key: l,
		id:  kvtest.RandValue(8),
	}

	lk.ck.Put(l, "", 0) // first, create the key in the map
	return lk
}

func (lk *Lock) Acquire() {
	for {
		val, ver, _ := lk.ck.Get(lk.key) // check: who holds the lock for that specific key?

		if val == lk.id {
			return // previous attempt succeeded via ErrMaybe, we already hold it
		}

		if val == "" { // lock is free
			// try to claim it
			if err := lk.ck.Put(lk.key, lk.id, ver); err == rpc.OK {
				return // got it
			}
		}

		// someone holds it, loop again
	}
}

func (lk *Lock) Release() {
	for {
		val, ver, _ := lk.ck.Get(lk.key)
		if val == "" { // confirmed free
			return
		}

		if lk.ck.Put(lk.key, "", ver) == rpc.OK {
			return // confirmed released
		}
	}
}

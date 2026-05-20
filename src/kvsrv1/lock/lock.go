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
	ck kvtest.IKVClerk
	// You may add code here
	identity string
	l string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.identity = kvtest.RandValue(8)
	lk.l = l
	lk.ck.Put(l, "free", 0)
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		value, version, _ := lk.ck.Get(lk.l)

		if value == lk.identity { // already holding the lock
			return
		}

		if value == "free" {
			err := lk.ck.Put(lk.l, lk.identity, version)
			if err == rpc.OK {
				return
			}
		}
	}

}

func (lk *Lock) Release() {
	// Your code here

	for {
		value, version, _ := lk.ck.Get(lk.l)

		if value != lk.identity {
			return
		}

		err := lk.ck.Put(lk.l, "free", version)

		if err == rpc.OK || err == rpc.ErrMaybe {
			return
		}
	} // otherwise, do nothing
}
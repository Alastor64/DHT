package node

import "sync"

type DhtNode interface {
	Run(waitgroup *sync.WaitGroup)

	Create()
	Join(addr string) bool

	Quit()
	ForceQuit()

	Put(key string, value string) bool
	Get(key string) (bool, string)
	Delete(key string) bool
}

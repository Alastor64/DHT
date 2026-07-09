package node

import (
	"net"
	"net/rpc"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

type hint = uint32

const (
	m    = hint(32)
	base = hint(269)
)

func Contain(x, l, r hint) bool {
	if l <= r {
		return l <= x && x <= r
	} else {
		return l <= x || x <= r
	}
}

func hashCode(s string) hint {
	val := hint(0)
	for i := 0; i < len(s); i++ {
		val *= base
		val += hint(s[i])
	}
	return val
}

type MyKey struct {
	key  string
	code hint
}

// Pair is used to store a key-value pair.
// Note: It must be exported (i.e., Capitalized) so that it can be
// used as the argument type of RPC methods.
type Pair struct {
	Key   MyKey
	Value string
}
type Info struct {
	Addr string // address and port number of the node, e.g., "localhost:1234"
	code hint
}

type Finger struct {
	info  Info
	start hint
}

type Chord struct {
	info   Info
	online bool

	listener net.Listener
	server   *rpc.Server

	data            map[MyKey]string
	dataLock        sync.RWMutex
	finger          [m]Finger
	fingerLock      sync.RWMutex
	predecessor     Info
	predecessorLock sync.RWMutex
}

func (n *Chord) successor() *Finger {
	return &n.finger[0]
}

// Initialize a node.
// Addr is the address and port number of the node, e.g., "localhost:1234".
func (n *Chord) Init(addr string) {
	n.info.Addr = addr
	n.info.code = hashCode(addr)
	n.data = make(map[MyKey]string)
	for i := hint(0); i < m; i++ {
		n.finger[i].start = n.info.code + (hint(1) << i)
	}
}

//连上RPC，并实时监听连接情况 不用改
func (n *Chord) RunRPCServer(wg *sync.WaitGroup) {
	n.server = rpc.NewServer()
	n.server.Register(n)
	var err error
	n.listener, err = net.Listen("tcp", n.info.Addr)
	wg.Done() //为什么不写在最后？
	if err != nil {
		logrus.Fatal("listen error: ", err)
	}
	for n.online {
		conn, err := n.listener.Accept()
		if err != nil {
			logrus.Error("accept error: ", err)
			return
		}
		go n.server.ServeConn(conn)
	}
}

//主动断连 不用改
func (n *Chord) StopRPCServer() {
	n.online = false
	n.listener.Close()
}

// RemoteCall calls the RPC method at addr.
//
// Note: An empty interface can hold values of any type. (https://tour.golang.org/methods/14)
// Re-connect to the client every time can be slow. You can use connection pool to improve the performance.
//封装调用其他节点methods，可以不改
func (n *Chord) RemoteCall(addr string, method string, args interface{}, reply interface{}) error {
	if method != "Chord.Ping" {
		logrus.Infof("[%s] RemoteCall %s %s %v", n.info.Addr, addr, method, args)
	}
	// Note: Here we use DialTimeout to set a timeout of 10 seconds.
	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		logrus.Error("dialing: ", err)
		return err
	}
	client := rpc.NewClient(conn)
	defer client.Close()
	err = client.Call(method, args, reply)
	if err != nil {
		logrus.Error("RemoteCall error: ", err)
		return err
	}
	return nil
}

//
// RPC Methods
//

// Note: The methods used for RPC must be exported (i.e., Capitalized),
// and must have two arguments, both exported (or builtin) types.
// The second argument must be a pointer.
// The return type must be error.
// In short, the signature of the method must be:
//   func (t *T) MethodName(argType T1, replyType *T2) error
// See https://golang.org/pkg/net/rpc/ for more details.

// Here we use "_" to ignore the arguments we don't need.
// The empty struct "{}" is used to represent "void" in Go.
//n是code的后继
func (n *Chord) MoveData(code hint, reply *map[MyKey]string) error {
	n.dataLock.RLock() //支持并发读
	for k, v := range n.data {
		if Contain(k.code, code, n.info.code) {
			(*reply)[k] = v
		}
	}
	n.dataLock.RUnlock()
	n.dataLock.Lock()
	for k, _ := range n.data {
		if Contain(k.code, code, n.info.code) {
			delete(n.data, k)
		}
	}
	n.dataLock.Unlock()
	return nil
}

//不用改
func (n *Chord) Ping(_ string, _ *struct{}) error {
	return nil
}

//不改
func (n *Chord) PutPair(pair Pair, _ *struct{}) error {
	n.dataLock.Lock()
	n.data[pair.Key] = pair.Value
	n.dataLock.Unlock()
	return nil
}

//删除
func (n *Chord) DeletePair(key MyKey, _ *struct{}) error {
	n.dataLock.Lock()
	delete(n.data, key)
	n.dataLock.Unlock()
	return nil
}

//
// DHT methods
//

func (n *Chord) Run(wg *sync.WaitGroup) {
	n.online = true
	go n.RunRPCServer(wg)
}

func (n *Chord) Create() {
	logrus.Info("Create")
}

func (n *Chord) GetPredecessor(_ struct{}, reply *Info) error {
	n.predecessorLock.RLock()
	*reply = n.predecessor
	n.predecessorLock.RUnlock()
	return nil
}

//finger 内id的严格前驱
func (n *Chord) closest_preceding_finger(id hint) Info {
	for i := m; i > 0; i-- {
		n.fingerLock.RLock()
		flag := false
		var tmp Info
		if Contain(n.finger[i-1].info.code, n.info.code+1, id-1) {
			tmp = n.finger[i-1].info
			flag = true
		}
		n.fingerLock.RUnlock()
		if flag {
			return tmp
		}
	}
	return n.info
}

//严格前驱
func (n *Chord) Find_predecessor(id hint, reply *Info) error {
	_n := n.info
	for {
		n.fingerLock.RLock()
		flag := Contain(id, _n.code+1, _n.successor().info.code)
		n.fingerLock.RUnlock()
		if flag {
			*reply = n.info
		} else {
			n.RemoteCall(n.closest_preceding_finger(id).Addr, "Chord.Find_predecessor", id, reply)
		}
	}
	return nil
}

func (n *Chord) Find_successor(id hint, reply *Info) error {
	var tmp Info
	n.RemoteCall(n.info.Addr, "Chord.Find_predecessor", id, &tmp)
	n.RemoteCall(tmp.Addr, "Chord.GetPredecessor", struct{}{}, reply)
	return nil
}

//Join（a）表示本node通过地址为a的节点加入网络
func (n *Chord) Join(addr string) bool {
	logrus.Infof("Join %s", addr)
	// Copy data from the node at addr.
	n.dataLock.Lock()
	n.RemoteCall(addr, "Chord.GetData", "", &n.data)
	n.dataLock.Unlock()
	return true
}

func (n *Chord) Put(key string, value string) bool {
	logrus.Infof("Put %s %s", key, value)
	n.dataLock.Lock()
	n.data[key] = value
	n.dataLock.Unlock()
	// Broadcast the new key-value pair to all the nodes in the network.
	n.broadcastCall("Chord.PutPair", Pair{key, value}, nil)
	return true
}

func (n *Chord) Get(key string) (bool, string) {
	logrus.Infof("Get %s", key)
	n.dataLock.RLock()
	value, ok := n.data[key]
	n.dataLock.RUnlock()
	return ok, value
}

func (n *Chord) Delete(key string) bool {
	logrus.Infof("Delete %s", key)
	// Check if the key exists.
	n.dataLock.RLock()
	_, ok := n.data[key]
	n.dataLock.RUnlock()
	if !ok {
		return false
	}
	// Delete the key-value pair.
	n.dataLock.Lock()
	delete(n.data, key)
	n.dataLock.Unlock()
	// Broadcast the deletion to all the nodes in the network.
	n.broadcastCall("Chord.DeletePair", key, nil)
	return true
}

func (n *Chord) Quit() {
	logrus.Infof("Quit %s", n.Addr)
	// Inform all the nodes in the network that this node is quitting.
	n.broadcastCall("Chord.RemovePeer", n.Addr, nil)
	n.StopRPCServer()
}

func (n *Chord) ForceQuit() {
	logrus.Info("ForceQuit")
	n.StopRPCServer()
}

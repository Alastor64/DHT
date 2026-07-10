// This package implements a naive DHT protocol. (Actually, it is not distributed at all.)
// The performance and scalability of this protocol is terrible.
// You can use this as a reference to implement other protocols.
//
// In this naive protocol, the network is a complete graph, and each node stores all the key-value pairs.
// When a node joins the network, it will copy all the key-value pairs from another node.
// Any modification to the key-value pairs will be broadcasted to all the nodes.
// If any RPC call fails, we simply assume the target node is offline and remove it from the peer list.
package node

import (
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// Note: The init() function will be executed when this package is imported.
// See https://golang.org/doc/effective_go.html#init for more details.
func init() {
	// You can use the logrus package to print pretty logs.
	// Here we set the log output to a file.
	f, _ := os.Create("dht-test.log")
	logrus.SetOutput(f)
}

type hint = uint8

const (
	m    = hint(8)
	base = 37 //hint(269)
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
	Key  string
	Code hint
}

// Pair is used to store a key-value pair.
// Note: It must be exported (i.e., Capitalized) so that it can be
// used as the argument type of RPC methods.
type Pair struct {
	Key   MyKey
	Value string
}

type Info struct {
	Addr    string // address and port number of the node, e.g., "localhost:1234"
	SucAddr string
	SucCode hint
	PreAddr string
	PreCode hint
	Code    hint
}

type Node struct {
	info   Info
	online bool

	listener net.Listener
	server   *rpc.Server
	data     map[MyKey]string
	dataLock sync.RWMutex
	relaLock sync.RWMutex
}

// Initialize a node.
// Addr is the address and port number of the node, e.g., "localhost:1234".
func (node *Node) Init(addr string) {
	node.info.Addr = addr
	node.info.SucAddr = addr
	node.info.PreAddr = addr
	node.info.Code = hashCode(addr)
	node.info.PreCode = node.info.Code
	node.info.SucCode = node.info.Code
	node.data = make(map[MyKey]string)
}

func (node *Node) RunRPCServer(wg *sync.WaitGroup) {
	node.server = rpc.NewServer()
	node.server.Register(node)
	var err error
	node.listener, err = net.Listen("tcp", node.info.Addr)
	wg.Done()
	if err != nil {
		logrus.Fatal("listen error: ", err)
	}
	for node.online {
		conn, err := node.listener.Accept()
		if err != nil {
			if node.online {
				logrus.Error("accept error: ", err)
			}
			return
		}
		go node.server.ServeConn(conn)
	}
}

func (node *Node) StopRPCServer() {
	node.online = false
	node.listener.Close()
}

// RemoteCall calls the RPC method at addr.
//
// Note: An empty interface can hold values of any type. (https://tour.golang.org/methods/14)
// Re-connect to the client every time can be slow. You can use connection pool to improve the performance.
func (node *Node) RemoteCall(addr string, method string, args interface{}, reply interface{}) error {
	if method != "Node.Ping" {
		logrus.Infof("[%s] RemoteCall %s %s %v", node.info.Addr, addr, method, args)
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
//保证reply是空map
//code是n的前驱
func (n *Node) MoveData(code hint, reply *map[MyKey]string) error {
	n.dataLock.Lock()
	for k, v := range n.data {
		if !Contain(k.Code, code+1, n.info.Code) {
			(*reply)[k] = v
			delete(n.data, k)
		}
	}
	n.dataLock.Unlock()
	return nil
}

func (node *Node) Ping(_ string, _ *struct{}) error {
	return nil
}

//
// DHT methods
//

func (node *Node) Run(wg *sync.WaitGroup) {
	node.online = true
	go node.RunRPCServer(wg)
}

func (node *Node) Create() {
	logrus.Info("Create")
}

func (node *Node) GetInfo(_ struct{}, reply *Info) error {
	*reply = node.info
	return nil
}

func (node *Node) FindSuc(id hint, reply *string) error {
	tmp := node.info
	for {
		logrus.Infoln(node.info.Addr, id, tmp.PreCode, tmp.Code)
		flag := Contain(id, tmp.PreCode+1, tmp.Code)
		if flag {
			break
		}
		node.RemoteCall(tmp.SucAddr, "Node.GetInfo", struct{}{}, &tmp)
	}
	logrus.Infof("finish")
	*reply = tmp.Addr
	return nil
}
func (node *Node) PreLink(x Info, reply *struct{}) error {
	node.info.PreAddr = x.Addr
	node.info.PreCode = x.Code
	return nil
}
func (node *Node) SucLink(x Info, reply *struct{}) error {
	node.info.SucAddr = x.Addr
	node.info.SucCode = x.Code
	return nil
}
func (node *Node) Join(addr string) bool {
	logrus.Infof("Join %s", addr)
	var tmp1, tmp2 Info
	for {
		node.RemoteCall(addr, "Node.FindSuc", node.info.Code, &node.info.SucAddr)
		node.RemoteCall(node.info.SucAddr, "Node.GetInfo", struct{}{}, &tmp1)
		if tmp1.Code != node.info.Code {
			break
		}
		node.info.Code++
	}
	logrus.Info("suc:", tmp1.Addr, tmp1.PreAddr, ";")
	node.info.PreAddr = tmp1.PreAddr
	node.RemoteCall(node.info.PreAddr, "Node.GetInfo", struct{}{}, &tmp2)
	node.info.SucCode = tmp1.Code
	node.info.PreCode = tmp2.Code
	node.data = make(map[MyKey]string)
	node.RemoteCall(node.info.SucAddr, "Node.MoveData", node.info.Code, &node.data)
	node.RemoteCall(node.info.PreAddr, "Node.SucLink", node.info, nil)
	node.RemoteCall(node.info.SucAddr, "Node.PreLink", node.info, nil)
	logrus.Infof("Join finish")
	return true
}

func (node *Node) PutPair(pair Pair, _ *struct{}) error {
	node.dataLock.Lock()
	node.data[pair.Key] = pair.Value
	node.dataLock.Unlock()
	return nil
}

type Prply struct {
	Ok  bool
	Val string
}

func (node *Node) GetPair(key MyKey, reply *Prply) error {
	node.dataLock.RLock()
	v, o := node.data[key]
	*reply = Prply{o, v}
	node.dataLock.RUnlock()
	return nil
}

func (node *Node) DeletePair(key MyKey, reply *bool) error {
	node.dataLock.Lock()
	_, ok := node.data[key]
	if ok {
		delete(node.data, key)
	}
	*reply = ok
	node.dataLock.Unlock()
	return nil
}

func (node *Node) Put(key string, value string) bool {
	logrus.Infof("Put %s %s", key, value)
	tmp := Pair{MyKey{key, hashCode(key)}, value}
	var x string
	node.FindSuc(tmp.Key.Code, &x)
	node.RemoteCall(x, "Node.PutPair", tmp, nil)
	return true
}

func (node *Node) Get(key string) (bool, string) {
	logrus.Infof("Get %s", key)
	var tmp Prply
	var x string
	k := MyKey{key, hashCode(key)}
	node.FindSuc(k.Code, &x)
	node.RemoteCall(x, "Node.GetPair", k, &tmp)
	return tmp.Ok, tmp.Val
}

func (node *Node) Delete(key string) bool {
	logrus.Infof("Delete %s", key)
	k := MyKey{key, hashCode(key)}
	var x string
	node.FindSuc(k.Code, &x)
	var tmp bool
	node.RemoteCall(x, "Node.DeletePair", k, &tmp)
	return tmp
}

func (node *Node) RecvData(d map[MyKey]string, _ *struct{}) error {
	node.dataLock.Lock()
	for k, v := range d {
		node.data[k] = v
	}
	node.dataLock.Unlock()
	return nil
}

func (node *Node) Quit() {
	logrus.Infof("Quit %s", node.info.Addr)
	if !node.online {
		logrus.Infof("Already quit")
		return
	}
	node.RemoteCall(node.info.SucAddr, "Node.RecvData", node.data, nil)
	var tmp1, tmp2 Info
	tmp1.Code = node.info.PreCode
	tmp1.Addr = node.info.PreAddr
	tmp2.Code = node.info.SucCode
	tmp2.Addr = node.info.SucAddr
	node.RemoteCall(node.info.SucAddr, "Node.PreLink", tmp1, nil)
	node.RemoteCall(node.info.PreAddr, "Node.SucLink", tmp2, nil)
	node.StopRPCServer()
}

func (node *Node) ForceQuit() {
	logrus.Info("ForceQuit")
	node.StopRPCServer()
}

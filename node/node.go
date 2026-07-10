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

type MyString struct {
	Val  string
	Code hint
}

// Pair is used to store a key-value pair.
// Note: It must be exported (i.e., Capitalized) so that it can be
// used as the argument type of RPC methods.
type Pair struct {
	Key   MyString
	Value string
}

type Smpl struct {
	Slf MyString
	Suc MyString
	Pre MyString
}

type Node struct {
	id     MyString
	online bool

	listener  net.Listener
	server    *rpc.Server
	data      map[MyString]string
	dataLock  sync.RWMutex
	routeLock sync.RWMutex
	suc       MyString
	pre       MyString
}

// Initialize a node.
// Addr is the address and port number of the node, e.g., "localhost:1234".
func (node *Node) Init(addr string) {
	node.id.Val = addr
	node.id.Code = hashCode(addr)
	node.suc = node.id
	node.pre = node.id
	node.data = make(map[MyString]string)
}

func (node *Node) RunRPCServer(wg *sync.WaitGroup) {
	node.server = rpc.NewServer()
	node.server.Register(node)
	var err error
	node.listener, err = net.Listen("tcp", node.id.Val)
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
		logrus.Infof("[%s] RemoteCall %s %s %v", node.id.Val, addr, method, args)
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
func (n *Node) MoveData(code hint, reply *map[MyString]string) error {
	n.dataLock.Lock()
	for k, v := range n.data {
		if !Contain(k.Code, code+1, n.id.Code) {
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

func (node *Node) Inform() Smpl {
	node.routeLock.RLock()
	tmp := Smpl{Slf: node.id, Suc: node.suc, Pre: node.pre}
	node.routeLock.RUnlock()
	return tmp
}

func (node *Node) GetInfo(_ struct{}, reply *Smpl) error {
	*reply = node.Inform()
	return nil
}

func (node *Node) FindSuc(id hint, reply *string) error {
	tmp := node.Inform()
	for {
		logrus.Infoln(node.id.Val, id, tmp.Pre.Code, tmp.Slf.Code)
		flag := Contain(id, tmp.Pre.Code+1, tmp.Slf.Code)
		if flag {
			break
		}
		node.RemoteCall(tmp.Suc.Val, "Node.GetInfo", struct{}{}, &tmp)
	}
	logrus.Infof("finish")
	*reply = tmp.Slf.Val
	return nil
}

func (node *Node) PreLink(x Smpl, reply *struct{}) error {
	node.routeLock.Lock()
	node.pre = x.Slf
	node.routeLock.Unlock()
	return nil
}

func (node *Node) SucLink(x Smpl, reply *struct{}) error {
	node.routeLock.Lock()
	node.suc = x.Slf
	node.routeLock.Unlock()
	return nil
}

func (node *Node) Join(addr string) bool {
	logrus.Infof("Join %s", addr)
	var tmp1 Smpl
	var s string
	for {
		node.RemoteCall(addr, "Node.FindSuc", node.id.Code, &s)
		node.RemoteCall(s, "Node.GetInfo", struct{}{}, &tmp1)
		if tmp1.Slf.Code != node.id.Code {
			break
		}
		node.id.Code++
	}
	logrus.Info("suc:", s, ";")
	node.routeLock.Lock()
	node.suc = tmp1.Slf
	node.pre = tmp1.Pre
	node.data = make(map[MyString]string)
	node.RemoteCall(node.pre.Val, "Node.SucLink", Smpl{Slf: node.id}, nil)
	node.RemoteCall(node.suc.Val, "Node.PreLink", Smpl{Slf: node.id}, nil)
	node.RemoteCall(node.suc.Val, "Node.MoveData", node.id.Code, &node.data)
	node.routeLock.Unlock()
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

func (node *Node) GetPair(key MyString, reply *Prply) error {
	node.dataLock.RLock()
	v, o := node.data[key]
	*reply = Prply{o, v}
	node.dataLock.RUnlock()
	return nil
}

func (node *Node) DeletePair(key MyString, reply *bool) error {
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
	tmp := Pair{MyString{key, hashCode(key)}, value}
	var x string
	node.FindSuc(tmp.Key.Code, &x)
	node.RemoteCall(x, "Node.PutPair", tmp, nil)
	return true
}

func (node *Node) Get(key string) (bool, string) {
	logrus.Infof("Get %s", key)
	var tmp Prply
	var x string
	k := MyString{key, hashCode(key)}
	node.FindSuc(k.Code, &x)
	node.RemoteCall(x, "Node.GetPair", k, &tmp)
	return tmp.Ok, tmp.Val
}

func (node *Node) Delete(key string) bool {
	logrus.Infof("Delete %s", key)
	k := MyString{key, hashCode(key)}
	var x string
	node.FindSuc(k.Code, &x)
	var tmp bool
	node.RemoteCall(x, "Node.DeletePair", k, &tmp)
	return tmp
}

func (node *Node) RecvData(d map[MyString]string, _ *struct{}) error {
	node.dataLock.Lock()
	for k, v := range d {
		node.data[k] = v
	}
	node.dataLock.Unlock()
	return nil
}

func (node *Node) Quit() {
	logrus.Infof("Quit %s", node.id.Val)
	if !node.online {
		logrus.Infof("Already quit")
		return
	}
	node.routeLock.Lock()
	node.RemoteCall(node.suc.Val, "Node.RecvData", node.data, nil)
	node.RemoteCall(node.suc.Val, "Node.PreLink", Smpl{Slf: node.pre}, nil)
	node.RemoteCall(node.pre.Val, "Node.SucLink", Smpl{Slf: node.suc}, nil)
	node.routeLock.Unlock()
	node.StopRPCServer()
}

func (node *Node) ForceQuit() {
	logrus.Info("ForceQuit")
	node.StopRPCServer()
}

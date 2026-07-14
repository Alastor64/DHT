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
	m        = hint(8)
	base     = 37 //hint(269)
	ticktime = 7 * time.Millisecond
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
type BUString struct {
	Origin MyString
	Key    MyString
}
type BUpair struct {
	Key BUString
	Val string
}
type Node struct {
	id     MyString
	online bool

	listener   net.Listener
	server     *rpc.Server
	data       map[MyString]string
	dataLock   sync.RWMutex
	backup     map[BUString]string
	backuplock sync.RWMutex
	routeLock  sync.RWMutex
	fingerLock sync.RWMutex
	suc        MyString
	pre        MyString
	finger     []MyString
	fix_cnt    hint
	periodLock sync.RWMutex
	ifperiod   bool
}

// Initialize a node.
// Addr is the address and port number of the node, e.g., "localhost:1234".
func (node *Node) Init(addr string) {
	node.id.Val = addr
	node.id.Code = hashCode(addr)
	node.suc = node.id
	node.pre = node.id
	node.backup = make(map[BUString]string, 0)
	node.data = make(map[MyString]string)
	node.finger = make([]MyString, m)
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
func (node *Node) RemoteCall(addr string, method string, args interface{}, reply interface{}, iflog bool) error {
	if method != "Node.Ping" {
		if iflog {
			logrus.Infof("[%s] RemoteCall %s %s %v", node.id.Val, addr, method, args)
		}
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
func (n *Node) SendData(x MyString, ifall bool) {
	logrus.Info("Sending : ", x.Code, "   ", n.id.Code, " if all:", ifall)
	flag := true
	n.dataLock.RLock()
	tmp := make(map[MyString]string, 0)
	for k, v := range n.data {
		tmp[k] = v
	}
	n.dataLock.RUnlock()
	var zz bool
	for k, v := range tmp {
		if ifall || !Contain(k.Code, x.Code+1, n.id.Code) {
			flag = false
			n.RemoteCall(x.Val, "Node.PutPair", Pair{k, v}, nil, false)
			n.DeletePair(k, &zz)
		}
	}
	if flag {
		logrus.Info("useless send!")
	}
}

func (node *Node) Ping(_ struct{}, _ *struct{}) error {
	return nil
}

//
// DHT methods
//
func (node *Node) ping(addr string) bool {
	return node.RemoteCall(addr, "Node.Ping", struct{}{}, nil, true) == nil
}
func (node *Node) LiveSuc() MyString {
	node.routeLock.RLock()
	tmp := node.suc
	node.routeLock.RUnlock()
	if node.ping(tmp.Val) {
		return tmp
	}
	tmp = node.id
	node.fingerLock.RLock()
	for i := hint(0); i < m; i++ {
		if Contain(node.finger[i].Code, node.id.Code, tmp.Code-1) && node.ping(node.finger[i].Val) {
			tmp = node.finger[i]
		}
	}
	node.fingerLock.RUnlock()
	node.routeLock.Lock()
	node.suc = tmp
	node.routeLock.Unlock()
	return node.suc
}
func (node *Node) eatBackup(Origin MyString) {
	node.backuplock.Lock()
	for k, v := range node.backup {
		if k.Origin == Origin {
			node.RemoteCall(node.id.Val, "Node.PutPair", Pair{Key: k.Key, Value: v}, nil, false)
			delete(node.backup, k)
		}
	}
	node.backuplock.Unlock()
}
func (node *Node) GetPre(_ struct{}, reply *MyString) error {
	node.routeLock.RLock()
	if node.pre.Val == "" {
		node.routeLock.RUnlock()
		reply.Val = ""
		return nil
	}
	if node.ping(node.pre.Val) {
		*reply = node.pre
		node.routeLock.RUnlock()
	} else {
		if node.pre != node.id {
			node.eatBackup(node.pre)
		}
		node.routeLock.RUnlock()
		node.routeLock.Lock()
		*reply = MyString{}
		node.pre = MyString{}
		node.routeLock.Unlock()
	}
	return nil
}
func (node *Node) Notify(x MyString, reply *struct{}) error {
	var tmp MyString
	node.GetPre(struct{}{}, &tmp)
	if x.Val == tmp.Val {
		return nil
	}
	if tmp.Val == "" || (tmp.Code+1 != node.id.Code && Contain(x.Code, tmp.Code+1, node.id.Code-1)) {
		node.routeLock.Lock()
		node.pre = x
		node.routeLock.Unlock()
		node.SendData(x, false)
	}
	return nil
}
func (node *Node) ClearBackup(Origin MyString, reply *struct{}) error {
	node.backuplock.Lock()
	for k, _ := range node.backup {
		if k.Origin == Origin {
			delete(node.backup, k)
		}
	}
	node.backuplock.Unlock()
	return nil
}
func (node *Node) promoteBackup() {
	node.routeLock.RLock()
	tmp := node.suc
	node.routeLock.RUnlock()
	if tmp == node.id {
		return
	}
	logrus.Info(node.id, "promote backup to", tmp)
	node.dataLock.RLock()
	for k, v := range node.data {
		p := BUpair{BUString{Key: k, Origin: node.id}, v}
		node.RemoteCall(tmp.Val, "Node.PutBackup", p, nil, false)
	}
	node.dataLock.RUnlock()
}
func (node *Node) Stabilize() {
	sn := node.LiveSuc()
	var pn MyString
	node.RemoteCall(sn.Val, "Node.GetPre", struct{}{}, &pn, false)
	if pn.Val != "" && node.id.Code+1 != sn.Code && Contain(pn.Code, node.id.Code+1, sn.Code-1) {
		node.RemoteCall(sn.Val, "Node.ClearBackup", node.id, nil, false)
		node.routeLock.Lock()
		logrus.Info(node.id, " suc from ", node.suc, " to ", pn)
		node.suc = pn
		node.routeLock.Unlock()
		node.promoteBackup()
		sn = pn
	}
	node.RemoteCall(sn.Val, "Node.Notify", node.id, nil, false)
}
func (node *Node) FingerPre(id hint, reply *MyString) error {
	node.fingerLock.RLock()
	defer node.fingerLock.RUnlock()
	*reply = node.id
	for i := m; i > 0; i-- {
		if Contain(node.finger[i-1].Code, node.id.Code, id-1) && node.ping(node.finger[i-1].Val) {
			if Contain(reply.Code, node.id.Code, node.finger[i-1].Code) {
				*reply = node.finger[i-1]
			}
			// return nil
		}
	}
	// *reply = node.id
	return nil
}
func (node *Node) GetLiveSuc(_ struct{}, reply *MyString) error {
	*reply = node.LiveSuc()
	return nil
}
func (node *Node) FindSuc(id hint, reply *MyString) error {
	if node.id.Code == id {
		*reply = node.id
		return nil
	}

	now := node.id
	suc := node.LiveSuc()
	for !Contain(id, now.Code+1, suc.Code) {
		logrus.Info(node.id, " ", id, "checked", now, ",", suc)
		tmp := now
		node.RemoteCall(tmp.Val, "Node.FingerPre", id, &now, false)
		if now == tmp {
			now = suc
		}
		node.RemoteCall(now.Val, "Node.GetLiveSuc", struct{}{}, &suc, false)

		if now == node.id {
			logrus.Info("not found")
			break
		}
	}
	*reply = suc
	return nil
}
func (node *Node) fixFinger() {
	var tmp MyString
	node.RemoteCall(node.id.Val, "Node.FindSuc", node.id.Code+(hint(1)<<node.fix_cnt), &tmp, true)
	node.fingerLock.Lock()
	defer node.fingerLock.Unlock()
	node.finger[node.fix_cnt] = tmp
	node.fix_cnt++
	if node.fix_cnt >= 8 {
		node.fix_cnt = 0
	}
}
func (node *Node) period() {
	node.ifperiod = true
	node.periodLock.Lock()
	for node.ifperiod {
		node.Stabilize()
		node.fixFinger()
		time.Sleep(ticktime)
	}
	node.periodLock.Unlock()
}
func (node *Node) Run(wg *sync.WaitGroup) {
	node.online = true
	go node.RunRPCServer(wg)
}
func (node *Node) SetSuc(x MyString, reply *struct{}) error {
	node.routeLock.Lock()
	node.suc = x
	node.routeLock.Unlock()
	return nil
}
func (node *Node) SetPre(x MyString, reply *struct{}) error {
	node.routeLock.Lock()
	node.pre = x
	node.routeLock.Unlock()
	return nil
}
func (node *Node) Create() {
	logrus.Info("Create")
	go node.period()
}

func (node *Node) Join(addr string) bool {
	logrus.Infof("Join %s", addr)
	node.routeLock.Lock()
	for {
		node.RemoteCall(addr, "Node.FindSuc", node.id.Code, &node.suc, true)
		if node.suc.Code == node.id.Code {
			node.id.Code++
		} else {
			break
		}
	}
	node.RemoteCall(node.suc.Val, "Node.GetPre", struct{}{}, &node.pre, false)
	suc := node.suc
	node.routeLock.Unlock()
	node.RemoteCall(suc.Val, "Node.Notify", node.id, nil, false)
	go node.period()
	logrus.Infof("Join finish")
	return true
}

func (node *Node) PutPair(pair Pair, _ *struct{}) error {
	node.dataLock.Lock()
	node.data[pair.Key] = pair.Value
	node.dataLock.Unlock()
	node.LiveSuc()
	node.routeLock.RLock()
	tmp := node.suc
	node.routeLock.RUnlock()
	if tmp != node.id {
		p := BUpair{BUString{Key: pair.Key, Origin: node.id}, pair.Value}
		node.RemoteCall(tmp.Val, "Node.PutBackup", p, nil, false)
	}
	return nil
}
func (node *Node) PutBackup(pair BUpair, _ *struct{}) error {
	node.backuplock.Lock()
	node.backup[pair.Key] = pair.Val
	node.backuplock.Unlock()
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
	node.routeLock.RLock()
	tmp := node.suc
	node.routeLock.RUnlock()
	if tmp != node.id {
		k := BUString{Key: key, Origin: node.id}
		node.RemoteCall(tmp.Val, "Node.DeleteBackup", k, nil, false)
	}
	return nil
}
func (node *Node) DeleteBackup(key BUString, reply *struct{}) error {
	node.backuplock.Lock()
	_, ok := node.backup[key]
	if ok {
		delete(node.backup, key)
	}
	node.backuplock.Unlock()
	return nil
}

func (node *Node) Put(key string, value string) bool {
	// logrus.Infof("Put %s %s", key, value)
	tmp := Pair{MyString{key, hashCode(key)}, value}
	var x MyString
	node.FindSuc(tmp.Key.Code, &x)
	node.RemoteCall(x.Val, "Node.PutPair", tmp, nil, false)
	return true
}

func (node *Node) Get(key string) (bool, string) {
	// logrus.Infof("Get %s", key)
	var tmp Prply
	var x MyString
	k := MyString{key, hashCode(key)}
	node.FindSuc(k.Code, &x)
	node.RemoteCall(x.Val, "Node.GetPair", k, &tmp, false)
	return tmp.Ok, tmp.Val
}

func (node *Node) Delete(key string) bool {
	// logrus.Infof("Delete %s", key)
	k := MyString{key, hashCode(key)}
	var x MyString
	node.FindSuc(k.Code, &x)
	var tmp bool
	node.RemoteCall(x.Val, "Node.DeletePair", k, &tmp, false)
	return tmp
}

func (node *Node) Quit() {
	defer node.StopRPCServer()
	logrus.Infof("Quit %s", node.id.Val)
	if !node.online {
		logrus.Infof("Already quit")
		return
	}
	suc := node.LiveSuc()
	if suc == node.id {
		return
	}
	node.ifperiod = false
	node.periodLock.Lock()
	defer node.periodLock.Unlock()
	node.SendData(suc, false)
	var pre MyString
	node.GetPre(struct{}{}, &pre)
	node.RemoteCall(suc.Val, "Node.SetPre", pre, nil, false)
	if node.ping(pre.Val) {
		node.RemoteCall(pre.Val, "Node.SetSuc", suc, nil, false)
	}
}

func (node *Node) ForceQuit() {
	logrus.Info("ForceQuit")
	node.StopRPCServer()
}

func (node *Node) Display(x int, reply *struct{}) error {
	if x > 0 {
		logrus.Info("SHOW: ", node.id, "\n", node.pre, "       ", node.suc, "\n", node.finger)
		node.RemoteCall(node.suc.Val, "Node.Display", x-1, nil, true)
	}
	return nil
}
func (node *Node) Dis(x int) {
	node.RemoteCall(node.id.Val, "Node.Display", x-1, nil, true)
}

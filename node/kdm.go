package node

//note：这份代码实现dht结点，对外提供与node.go中的实现完全一致的接口，但内部实现使用kademlia算法
import (
	"fmt"
	"net"
	"net/rpc"
	"sort"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	k     = 10
	alpha = 3
)

type PString struct {
	Val     string
	Version int
}
type Kdm struct {
	clients    map[string]*rpc.Client
	clientLock sync.RWMutex
	connLock   sync.Mutex
	online     bool
	listener   net.Listener
	server     *rpc.Server

	id       MyString
	data     map[MyString]PString
	datacnt  int
	dataLock sync.RWMutex
	//第一维i表示与自身异或值严格小于2^(i+1)范围固定为[0,m),第二维范围不固定但最大为[0,k)0号位置为桶的尾部即最近活跃节点
	bucket     [][]MyString
	bucketLock sync.RWMutex
}

//kdm methods

func (node *Kdm) RunRPCServer(wg *sync.WaitGroup) {
	node.server = rpc.NewServer()
	node.server.Register(node)
	var err error
	node.listener, err = net.Listen("tcp", node.id.Val)
	wg.Done()
	if err != nil {
		logrus.Fatal("listen error: ", err)
	}
	node.connLock.Lock()
	for node.online {
		conn, err := node.listener.Accept()
		if err != nil {
			if node.online {
				logrus.Error("accept error: ", err)
			}
			return
		}
		go func(c net.Conn) {
			go node.server.ServeConn(c)
			node.connLock.Lock()
			c.Close()
			node.connLock.Unlock()
		}(conn)
	}
}

func (node *Kdm) StopRPCServer() {
	if !node.online {
		return
	}
	node.online = false
	node.listener.Close()
	node.connLock.Unlock()
}

func (node *Kdm) getClient(addr string) (*rpc.Client, error) {
	node.clientLock.RLock()
	tmp, ok := node.clients[addr]
	node.clientLock.RUnlock()
	if ok {
		return tmp, nil
	}

	conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
	if err != nil {
		return nil, err
	}

	client := rpc.NewClient(conn)
	node.clientLock.Lock()
	node.clients[addr] = client
	node.clientLock.Unlock()
	return client, nil
}
func (node *Kdm) removeClient(addr string, bad *rpc.Client) {
	node.clientLock.Lock()
	defer node.clientLock.Unlock()

	if current, ok := node.clients[addr]; ok && current == bad {
		delete(node.clients, addr)
		current.Close()
	}
}

func (node *Kdm) RemoteCall(addr string, method string, args interface{}, reply interface{}, iflog bool) error {
	if method != "Kdm.Ping" {
		if iflog {
			logrus.Infof("[%s] RemoteCall %s %s %v", node.id.Val, addr, method, args)
		}
	}
	client, err := node.getClient(addr)
	if err != nil {
		logrus.Error("RemoteCall tcp error: ", err)
		return err
	}
	err = client.Call(method, args, reply)
	if err != nil {
		node.removeClient(addr, client)
		logrus.Error("RemoteCall error: ", err)
		return err
	}
	return nil
}

func (node *Kdm) ping(addr string) bool {
	if addr == "" {
		return false
	}
	return node.RemoteCall(addr, "Node.Ping", struct{}{}, nil, true) == nil
}

// closerTo reports whether left is closer to code than right.
func closerTo(code hint, left, right MyString) bool {
	leftDistance := left.Code ^ code
	rightDistance := right.Code ^ code
	if leftDistance != rightDistance {
		return leftDistance < rightDistance
	}
	if left.Code != right.Code {
		return left.Code < right.Code
	}
	return left.Val < right.Val
}

func sortByDistance(nodes []MyString, code hint) {
	sort.Slice(nodes, func(i, j int) bool {
		return closerTo(code, nodes[i], nodes[j])
	})
}

//获得桶中最近的alpha个节点
func (node *Kdm) getAlphaNearest(code hint, reply *[]MyString) {
	if reply == nil {
		return
	}
	*reply = (*reply)[:0]

	// If d = code ^ node.id.Code and x = candidate.Code ^ node.id.Code,
	// then code ^ candidate.Code = d ^ x. Bucket i contains exactly the
	// values x whose highest set bit is i. The following order visits the
	// resulting, disjoint distance intervals from small to large.
	d := code ^ node.id.Code

	bucketOrder := make([]int, 0, int(m))
	for i := int(m - 1); i >= 0; i-- {
		if d&(hint(1)<<i) != 0 {
			bucketOrder = append(bucketOrder, i)
		}
	}
	for i := 0; i < int(m); i++ {
		if d&(hint(1)<<i) == 0 {
			bucketOrder = append(bucketOrder, i)
		}
	}

	for _, bucketIndex := range bucketOrder {
		node.bucketLock.RLock()
		if bucketIndex >= len(node.bucket) {
			fmt.Println("unknown: len(node.bucket) too short in getAlphaNearest")
			continue
		}
		*reply = append(*reply, node.bucket[bucketIndex]...)
		node.bucketLock.RUnlock()

		if len(*reply) >= alpha {
			break
		}
	}

	sortByDistance(*reply, code)
	if len(*reply) > alpha {
		*reply = (*reply)[:alpha]
	}
}

//RPC methods

func (node *Kdm) Ping(_ struct{}, _ *struct{}) error {
	return nil
}

//DHT methods
func (node *Kdm) Init(addr string) {
	node.id.Val = addr
	node.id.Code = hashCode(addr)
	node.data = make(map[MyString]PString)
	node.datacnt = 0
	node.clients = make(map[string]*rpc.Client)
	node.bucket = make([][]MyString, m)
	for i := range node.bucket {
		node.bucket[i] = make([]MyString, 0)
	}
}
func (node *Kdm) ForceQuit() {
	logrus.Info("ForceQuit")
	node.StopRPCServer()
}

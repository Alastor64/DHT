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

type MyListEntry struct {
	value MyString
	prev  *MyListEntry
	next  *MyListEntry
}

// MyList is a fixed-capacity doubly linked list. Its entries are allocated
// once by makeMyList, so appending an entry never changes the list's
// capacity or invalidates the links between entries.
type MyList struct {
	entries []MyListEntry
	head    *MyListEntry
	tail    *MyListEntry
	size    int
}

func makeMyList(capacity int) MyList {
	return MyList{entries: make([]MyListEntry, capacity)}
}

func (bucket *MyList) append(value MyString) bool {
	if bucket.size == len(bucket.entries) {
		return false
	}

	entry := &bucket.entries[bucket.size]
	entry.value = value
	entry.prev = bucket.tail
	entry.next = nil
	if bucket.tail == nil {
		bucket.head = entry
	} else {
		bucket.tail.next = entry
	}
	bucket.tail = entry
	bucket.size++
	return true
}

func (bucket *MyList) appendValues(values []MyString) []MyString {
	for entry := bucket.head; entry != nil; entry = entry.next {
		values = append(values, entry.value)
	}
	return values
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
	// bucket[i] contains contacts whose XOR distance from this node is in
	// [2^i, 2^(i+1)). The head is the least recently seen contact.
	bucket     []MyList
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

func (node *Kdm) RemoteCall(target MyString, method string, args interface{}, reply interface{}, iflog bool) error {
	if method != "Kdm.Ping" {
		if iflog {
			logrus.Infof("[%s] RemoteCall %s %s %v", node.id.Val, target.Val, method, args)
		}
	}
	client, err := node.getClient(target.Val)
	if err != nil {
		logrus.Error("RemoteCall tcp error: ", err)
		return err
	}
	err = client.Call(method, args, reply)
	if err != nil {
		node.removeClient(target.Val, client)
		logrus.Error("RemoteCall error: ", err)
		return err
	}
	return nil
}

func (node *Kdm) ping(target MyString) bool {
	if target.Val == "" {
		return false
	}
	return node.RemoteCall(target, "Kdm.Ping", struct{}{}, nil, true) == nil
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
	return left.Code < right.Code
}

func sortByDistance(nodes []MyString, code hint) {
	sort.Slice(nodes, func(i, j int) bool {
		return closerTo(code, nodes[i], nodes[j])
	})
}

//获得桶中最近的alpha个节点
func (node *Kdm) getNearest(code hint, limit int, cap int) []MyString {
	if limit <= 0 {
		return nil
	}
	if limit+k > cap {
		cap = limit + k
	}
	reply := make([]MyString, 0, cap)

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
			fmt.Println("unknown: len(node.bucket) too short in getNearest")
			node.bucketLock.RUnlock()
			continue
		}
		reply = node.bucket[bucketIndex].appendValues(reply)
		node.bucketLock.RUnlock()

		if len(reply) >= limit {
			break
		}
	}

	sortByDistance(reply, code)
	if len(reply) > limit {
		reply = reply[:limit]
	}
	return reply
}

type findNodeResult struct {
	from  MyString
	nodes []MyString
	err   error
}

// FindNode performs an iterative Kademlia node lookup. At most alpha requests
// are in flight at once, and only the k closest known contacts are queried.
func (node *Kdm) FindNode(code hint) []MyString {
	candidates := node.getNearest(code, k, k+1)
	queried := make([]bool, len(candidates), k+1)
	inFlight := make(map[hint]struct{})

	addCandidate := func(contact MyString) {
		if contact.Val == "" || contact.Code == node.id.Code {
			return
		}

		if len(candidates) == k {
			farthest := candidates[len(candidates)-1]
			if !closerTo(code, contact, farthest) {
				return
			}
		}

		for _, candidate := range candidates {
			if candidate.Code == contact.Code {
				return
			}
		}

		insertAt := len(candidates)
		for i, candidate := range candidates {
			if closerTo(code, contact, candidate) {
				insertAt = i
				break
			}
		}

		candidates = append(candidates, MyString{})
		copy(candidates[insertAt+1:], candidates[insertAt:])
		candidates[insertAt] = contact

		queried = append(queried, false)
		copy(queried[insertAt+1:], queried[insertAt:])
		queried[insertAt] = false

		if len(candidates) > k {
			candidates = candidates[:k]
			queried = queried[:k]
		}
	}

	results := make(chan findNodeResult, alpha)
	startQueries := func() {
		for i := range candidates {
			if len(inFlight) >= alpha {
				return
			}
			if queried[i] {
				continue
			}

			contact := candidates[i]
			queried[i] = true
			inFlight[contact.Code] = struct{}{}
			go func(target MyString) {
				var nearest []MyString
				err := node.RemoteCall(target, "Kdm.FindNodeRPC", code, &nearest, false)
				results <- findNodeResult{from: target, nodes: nearest, err: err}
			}(contact)
		}
	}

	startQueries()
	for len(inFlight) > 0 {
		result := <-results
		delete(inFlight, result.from.Code)

		// Failed contacts deliberately remain in the shortlist. Since they have
		// already been marked queried, they will not be requested again.
		if result.err == nil {
			for _, contact := range result.nodes {
				addCandidate(contact)
			}
		}
		startQueries()
	}

	// An empty inFlight set is essential: contacts are marked queried when a
	// request is sent, so checking queried alone could finish before replies arrive.
	return candidates
}

//RPC methods

// FindNodeRPC is the single-hop FIND_NODE operation. It deliberately performs
// no network lookup of its own; otherwise two peers could recursively start
// full iterative lookups for each other.
func (node *Kdm) FindNodeRPC(code hint, reply *[]MyString) error {
	if reply != nil {
		*reply = node.getNearest(code, k, k+1)
	} else {
		fmt.Println("unknown: FindNodeRPC's reply is nil")
	}
	return nil
}

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
	node.bucket = make([]MyList, m)
	for i := range node.bucket {
		node.bucket[i] = makeMyList(k)
	}
}
func (node *Kdm) ForceQuit() {
	logrus.Info("ForceQuit")
	node.StopRPCServer()
}

package node

//note：这份代码实现dht结点，对外提供与node.go中的实现完全一致的接口，但内部实现使用kademlia算法
import (
	"fmt"
	"math/bits"
	"math/rand"
	"net"
	"net/rpc"
	"sort"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	k           = 10
	alpha       = 3
	kdmTicktime = 100 * time.Millisecond
	killTick    = 95
	sendTick    = 95
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
// once by makeMyList, so inserting an entry never changes the list's
// capacity or invalidates the links between entries. Entries which are not
// in the list form a singly linked free list through their next fields.
type MyList struct {
	entries  []MyListEntry
	head     *MyListEntry
	tail     *MyListEntry
	freeHead *MyListEntry
	size     int
}

func makeMyList(capacity int) MyList {
	bucket := MyList{entries: make([]MyListEntry, capacity)}
	for i := range bucket.entries {
		if i+1 < len(bucket.entries) {
			bucket.entries[i].next = &bucket.entries[i+1]
		}
	}
	if len(bucket.entries) > 0 {
		bucket.freeHead = &bucket.entries[0]
	}
	return bucket
}

func (bucket *MyList) pushFront(value MyString) (*MyListEntry, bool) {
	entry := bucket.freeHead
	if entry == nil {
		return nil, false
	}

	bucket.freeHead = entry.next
	entry.value = value
	entry.prev = nil
	entry.next = bucket.head
	if bucket.head == nil {
		bucket.tail = entry
	} else {
		bucket.head.prev = entry
	}
	bucket.head = entry
	bucket.size++
	return entry, true
}

func (bucket *MyList) moveToFront(entry *MyListEntry) {
	if entry == nil || entry == bucket.head {
		return
	}

	entry.prev.next = entry.next
	if entry.next == nil {
		bucket.tail = entry.prev
	} else {
		entry.next.prev = entry.prev
	}

	entry.prev = nil
	entry.next = bucket.head
	bucket.head.prev = entry
	bucket.head = entry
}

func (bucket *MyList) appendValues(values []MyString) []MyString {
	for entry := bucket.head; entry != nil; entry = entry.next {
		values = append(values, entry.value)
	}
	return values
}

type bucketLocation struct {
	bucketIndex int
	entry       *MyListEntry
}

type Kdm struct {
	clients       map[string]*rpc.Client
	clientLock    sync.RWMutex
	connLock      sync.Mutex
	online        bool
	updateRouting bool
	listener      net.Listener
	server        *rpc.Server

	id          MyString
	data        map[MyString]string
	dataVersion map[MyString]int
	datacnt     int
	dataLock    sync.RWMutex
	// bucket[i] contains contacts whose XOR distance from this node is in
	// [2^i, 2^(i+1)). The head is the most recently seen contact.
	bucket     []MyList
	bucketMap  map[hint]bucketLocation
	bucketLock sync.RWMutex
	ifperiod   bool
	periodLock sync.Mutex
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
	if method == "Kdm.Ping" {
		iflog = false
	}
	if iflog {
		logrus.Infof("[%s] RemoteCall %s %s %v", node.id.Val, target.Val, method, args)
	}
	client, err := node.getClient(target.Val)
	if err != nil {
		if iflog {
			logrus.Error("RemoteCall tcp error: ", err)
		}
		return err
	}
	err = client.Call(method, args, reply)
	if err != nil {
		node.removeClient(target.Val, client)
		if iflog {
			logrus.Error("RemoteCall error: ", err)
		}
		return err
	}
	if method == "Kdm.GetCode" || !node.updateRouting {
		return nil
	}

	node.updateBucket(target)
	var updateReply struct{}
	if updateErr := client.Call("Kdm.UpdateBucket", node.id, &updateReply); updateErr != nil {
		node.removeClient(target.Val, client)
		logrus.Error("UpdateBucket notification error: ", updateErr)
	}
	return nil
}

func (node *Kdm) ping(target MyString) bool {
	if target.Val == "" {
		return false
	}
	return node.RemoteCall(target, "Kdm.Ping", struct{}{}, nil, true) == nil
}

func (node *Kdm) bucketIndexFor(code hint) (int, bool) {
	distance := node.id.Code ^ code
	if distance == 0 {
		return 0, false
	}
	return bits.Len(uint(distance)) - 1, true
}

func (node *Kdm) resetBuckets() {
	node.bucketLock.Lock()
	defer node.bucketLock.Unlock()

	node.bucket = make([]MyList, m)
	node.bucketMap = make(map[hint]bucketLocation)
	for i := range node.bucket {
		node.bucket[i] = makeMyList(k)
	}
}

func (node *Kdm) killDeadContacts() {
	for i := 0; i < int(m); i++ {
		node.bucketLock.RLock()
		bucket := node.bucket[i].appendValues(make([]MyString, 0, k))
		node.bucketLock.RUnlock()
		for _, j := range bucket {
			node.ping(j)
		}
		for {
			node.bucketLock.RLock()
			if node.bucket[i].tail == nil {
				node.bucketLock.RUnlock()
				break
			}
			leastRecent := node.bucket[i].tail.value
			leastRecentLocation, mapped := node.bucketMap[leastRecent.Code]
			if !mapped {
				fmt.Println("unknown: map inconsistent in killDeadContacts")
				node.bucketLock.RUnlock()
				break
			}
			node.bucketLock.RUnlock()

			if node.ping(leastRecent) {
				break
			}

			node.bucketLock.Lock()

			currentLocation, stillPresent := node.bucketMap[leastRecent.Code]
			if !stillPresent || currentLocation != leastRecentLocation ||
				node.bucket[i].tail != leastRecentLocation.entry {
				node.bucketLock.Unlock()
				continue
			}

			node.removeBucketEntry(leastRecent.Code, leastRecentLocation.entry)
			node.bucketLock.Unlock()
		}
	}
}

func (node *Kdm) sendData() {
	node.dataLock.RLock()
	dataVersion := make(map[MyString]int, 0)
	data := make(map[MyString]string, 0)
	for t1, t2 := range node.data {
		data[t1] = t2
	}
	for t1, t2 := range node.dataVersion {
		dataVersion[t1] = t2
	}
	node.dataLock.RUnlock()
	for key, version := range dataVersion {
		var tmp VersionPair
		tmp.Val.Val, tmp.Val.Empty = data[key]
		tmp.Val.Empty = !tmp.Val.Empty
		tmp.Val.Version = version
		tmp.Key = key
		candidates := node.findNode(tmp.Key.Code)
		// index, ok := node.bucketIndexFor(tmp.Key.Code)
		// if !ok {
		// 	continue
		// }
		// candidates := make([]MyString, 0, k)
		// node.bucketLock.RLock()
		// candidates = node.bucket[index].appendValues(candidates)
		// node.bucketLock.RUnlock()
		// sortByDistance(candidates, tmp.Key.Code)
		for _, cancandidate := range candidates {
			// if !closerTo(tmp.Key.Code, cancandidate, node.id) {
			// 	break
			// }
			node.RemoteCall(cancandidate, "Kdm.PutPair", tmp, nil, false)
		}
	}
}

func (node *Kdm) period() {
	node.ifperiod = true
	node.periodLock.Lock()
	var cnt int = 0
	for node.ifperiod && node.online {
		cnt++
		if cnt%killTick == 0 {
			node.killDeadContacts()
		}
		if cnt%sendTick == 0 {
			node.sendData()
		}
		time.Sleep(kdmTicktime)
	}
	node.periodLock.Unlock()
}

// removeBucketEntry removes a routing-table entry only if code still maps to
// the expected entry. The active list, free list and bucketMap are updated
// atomically under bucketLock.
// seems right
// when calling this func the lock must be held
func (node *Kdm) removeBucketEntry(code hint, expected *MyListEntry) bool {
	if expected == nil {
		return false
	}

	location, exists := node.bucketMap[code]
	if !exists || location.entry != expected {
		return false
	}

	bucket := &node.bucket[location.bucketIndex]
	if expected.prev == nil {
		bucket.head = expected.next
	} else {
		expected.prev.next = expected.next
	}
	if expected.next == nil {
		bucket.tail = expected.prev
	} else {
		expected.next.prev = expected.prev
	}

	delete(node.bucketMap, code)
	expected.value = MyString{}
	expected.prev = nil
	expected.next = bucket.freeHead
	bucket.freeHead = expected
	bucket.size--
	return true
}

// updateBucket records target as the most recently seen contact. If its
// bucket is full, the least recently seen contact is pinged before it can be
// replaced. No network operation is performed while bucketLock is held.
func (node *Kdm) updateBucket(target MyString) {
	bucketIndex, ok := node.bucketIndexFor(target.Code)
	if !ok || target.Val == "" {
		return
	}

	for {
		node.bucketLock.Lock()
		if location, exists := node.bucketMap[target.Code]; exists {
			node.bucket[location.bucketIndex].moveToFront(location.entry)
			node.bucketLock.Unlock()
			return
		}

		bucket := &node.bucket[bucketIndex]
		if entry, inserted := bucket.pushFront(target); inserted {
			node.bucketMap[target.Code] = bucketLocation{
				bucketIndex: bucketIndex,
				entry:       entry,
			}
			node.bucketLock.Unlock()
			return
		}

		leastRecent := bucket.tail.value
		leastRecentLocation, mapped := node.bucketMap[leastRecent.Code]
		if !mapped {
			fmt.Println("unknown: map inconsistent in update bucket")
			node.bucketLock.Unlock()
			return
		}
		node.bucketLock.Unlock()

		if node.ping(leastRecent) {
			return
		}

		node.bucketLock.Lock()
		if location, exists := node.bucketMap[target.Code]; exists {
			node.bucket[location.bucketIndex].moveToFront(location.entry)
			node.bucketLock.Unlock()
			return
		}

		bucket = &node.bucket[bucketIndex]
		currentLocation, stillPresent := node.bucketMap[leastRecent.Code]
		if !stillPresent || currentLocation != leastRecentLocation ||
			bucket.tail != leastRecentLocation.entry {
			node.bucketLock.Unlock()
			continue
		}

		entry := leastRecentLocation.entry
		delete(node.bucketMap, leastRecent.Code)
		entry.value = target
		bucket.moveToFront(entry)
		node.bucketMap[target.Code] = leastRecentLocation
		node.bucketLock.Unlock()
		return
	}
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

// findNode performs an iterative Kademlia node lookup. At most alpha requests
// are in flight at once, and only the k closest known contacts are queried.
func (node *Kdm) findNode(code hint) []MyString {
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
func (node *Kdm) countLiving(contacts []MyString) int {
	cnt := 0
	for _, contact := range contacts {
		if node.ping(contact) {
			cnt++
		}
	}
	return cnt
}
func (node *Kdm) testPGD(key string) int {
	// fmt.Println("zz")
	code := hashCode(key)
	return node.countLiving(node.findNode(code))
}
func (node *Kdm) getData(key MyString) DataReply {
	candidates := node.findNode(key.Code)
	candidates = append(candidates, node.id)
	reply := DataReply{"", 0, true}
	for _, cancandidate := range candidates {
		var tmp DataReply
		if node.RemoteCall(cancandidate, "Kdm.GetPair", key, &tmp, false) != nil {
			continue
		}
		if tmp.Version == reply.Version && tmp != reply {
			fmt.Println("Version fail!!!")
		}
		if tmp.Version > reply.Version {
			reply = tmp
		}
	}

	return reply
}
func (node *Kdm) putData(key MyString, val DataReply) bool {
	candidates := node.findNode(key.Code)
	candidates = append(candidates, node.id)
	reply := DataReply{"", 0, true}
	for _, cancandidate := range candidates {
		var tmp DataReply
		if node.RemoteCall(cancandidate, "Kdm.GetPair", key, &tmp, false) != nil {
			continue
		}
		if tmp.Version == reply.Version && tmp != reply {
			fmt.Println("Version fail!!!")
		}
		if tmp.Version > reply.Version {
			reply = tmp
		}
	}
	val.Version = reply.Version + 1
	for _, cancandidate := range candidates {
		node.RemoteCall(cancandidate, "Kdm.PutPair", VersionPair{key, val}, nil, false)
	}
	if val.Empty {
		return !reply.Empty
	} else {
		return true
	}
}

//RPC methods
type DataReply struct {
	Val     string
	Version int
	Empty   bool
}
type VersionPair struct {
	Key MyString
	Val DataReply
}

func (node *Kdm) GetPair(key MyString, reply *DataReply) error {
	node.dataLock.RLock()
	defer node.dataLock.RUnlock()
	reply.Val, reply.Empty = node.data[key]
	reply.Empty = !reply.Empty
	var exist bool
	reply.Version, exist = node.dataVersion[key]
	if !exist {
		reply.Version = 0
	}
	return nil
}
func (node *Kdm) PutPair(pair VersionPair, reply *struct{}) error {
	node.dataLock.Lock()
	defer node.dataLock.Unlock()
	version, exist := node.dataVersion[pair.Key]
	if exist && version >= pair.Val.Version {
		return nil
	}
	node.dataVersion[pair.Key] = pair.Val.Version
	if pair.Val.Empty {
		if _, ok := node.data[pair.Key]; ok {
			delete(node.data, pair.Key)
		}
	} else {
		node.data[pair.Key] = pair.Val.Val
	}
	return nil
}

// FindNode exposes the iterative lookup over RPC.
func (node *Kdm) FindNode(code hint, reply *[]MyString) error {
	if reply == nil {
		return fmt.Errorf("FindNode's reply is nil")
	}

	*reply = node.findNode(code)
	return nil
}

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

// GetCode returns this node's current ID. RemoteCall deliberately does not
// update either peer's routing table for this metadata-only RPC.
func (node *Kdm) GetCode(_ struct{}, reply *hint) error {
	if reply == nil {
		return fmt.Errorf("GetCode's reply is nil")
	}
	*reply = node.id.Code
	return nil
}

// UpdateBucket lets a peer report a successful interaction. It updates only
// local routing state and deliberately sends no notification back.
func (node *Kdm) UpdateBucket(target MyString, _ *struct{}) error {
	node.updateBucket(target)
	return nil
}

//DHT methods

// Join bootstraps this node through an existing Kademlia node.
func (node *Kdm) Join(addr string) bool {
	logrus.Infof("Join %s", addr)
	if addr == "" || addr == node.id.Val {
		return false
	}
	node.updateRouting = false
	bootstrap := MyString{Val: addr}
	if err := node.RemoteCall(bootstrap, "Kdm.GetCode", struct{}{}, &bootstrap.Code, true); err != nil {
		return false
	}

	// The node ID cannot be inferred from its address. Query the bootstrap
	// node by address until linear probing finds an unused ID. FindNode may
	// return dead contacts; they still reserve their IDs and count as collisions.
	var contacts []MyString
	foundFreeID := false
	for attempts := 0; attempts < 1<<uint(m); attempts++ {
		contacts = nil
		if err := node.RemoteCall(bootstrap, "Kdm.FindNode", node.id.Code, &contacts, true); err != nil {
			return false
		}

		collision := bootstrap.Code == node.id.Code
		for _, contact := range contacts {
			if contact.Code == node.id.Code {
				collision = true
				break
			}
		}
		if !collision {
			foundFreeID = true
			break
		}
		node.id.Code++
	}
	if !foundFreeID {
		logrus.Error("Join failed: no free node ID")
		return false
	}

	node.updateRouting = true
	if !node.ping(bootstrap) {
		node.resetBuckets()
		node.updateRouting = false
		return false
	}
	for _, contact := range contacts {
		node.updateBucket(contact)
	}

	// Populate the routing table once around this node's own ID.
	for _, contact := range node.findNode(node.id.Code) {
		node.updateBucket(contact)
	}

	// A lookup can return dead contacts and leave some buckets sparse. For
	// every bucket with fewer than alpha entries, look up a random ID whose
	// XOR distance is guaranteed to fall into that bucket.
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for bucketIndex := 0; bucketIndex < int(m); bucketIndex++ {
		node.bucketLock.RLock()
		bucketSize := node.bucket[bucketIndex].size
		node.bucketLock.RUnlock()
		if bucketSize >= alpha {
			continue
		}

		lowerBits := hint(0)
		if bucketIndex > 0 {
			lowerBits = hint(rng.Intn(1 << uint(bucketIndex)))
		}
		distance := (hint(1) << uint(bucketIndex)) | lowerBits
		target := node.id.Code ^ distance
		for _, contact := range node.findNode(target) {
			node.updateBucket(contact)
		}
	}
	go node.period()
	logrus.Infof("Join finish %v", node.id)
	return true
}

func (node *Kdm) Init(addr string) {
	node.id.Val = addr
	node.id.Code = hashCode(addr)
	node.data = make(map[MyString]string)
	node.dataVersion = make(map[MyString]int)
	node.datacnt = 0
	node.clients = make(map[string]*rpc.Client)
	node.resetBuckets()
}
func (node *Kdm) Create() {
	logrus.Info("Create")
	node.updateRouting = true
	go node.period()
}
func (node *Kdm) ForceQuit() {
	logrus.Info("ForceQuit")
	node.StopRPCServer()
}

func (node *Kdm) Dis(x int) {
}

func (node *Kdm) Quit() {
	defer node.StopRPCServer()
	logrus.Infof("Quit %s", node.id.Val)
	node.ifperiod = false
}

func (node *Kdm) Delete(key string) bool {
	// logrus.Info("Delete test:", node.testPGD(key))
	return node.putData(MyString{key, hashCode(key)}, DataReply{"", 0, true})
}

func (node *Kdm) Put(key string, value string) bool {
	// logrus.Info("Put test:", node.testPGD(key))
	return node.putData(MyString{key, hashCode(key)}, DataReply{value, 0, false})
}

func (node *Kdm) Get(key string) (bool, string) {
	// fmt.Println("zz")
	// logrus.Info("Get test:", node.testPGD(key))
	reply := node.getData(MyString{key, hashCode(key)})
	if reply.Empty {
		return false, ""
	} else {
		return true, reply.Val
	}
}
func (node *Kdm) Run(wg *sync.WaitGroup) {
	node.online = true
	go node.RunRPCServer(wg)
}

package node

func NewNode(port int) DhtNode {
	node := new(Chord)
	node.Init(portToAddr(localAddress, port))
	return node
}

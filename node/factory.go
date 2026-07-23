package node

func NewNode(port int) DhtNode {
	node := new(Node)
	node.Init(portToAddr(localAddress, port))
	return node
}

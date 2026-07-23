package node

func NewNode(port int) DhtNode {
	node := new(Kdm)
	node.Init(portToAddr(localAddress, port))
	return node
}

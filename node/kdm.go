package node

import (
	"net"
	"net/rpc"
	"time"

	"github.com/sirupsen/logrus"
)

type Kdm struct {
	id MyString
}

func (node *Kdm) RemoteCall(addr string, method string, args interface{}, reply interface{}, iflog bool) error {
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

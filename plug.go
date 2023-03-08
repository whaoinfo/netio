package gateway

import (
	"github.com/whaoinfo/go-box/logger"
	"github.com/whaoinfo/go-box/nbuffer"
	"runtime/debug"
)

type PlugID string
type PlugType string
type NewPlugFunc func() IPlug

type AllocateBufferFunc func() (*nbuffer.BufferObject, error)
type ReadIOCallbackFunc func(pkt *UDPPacket)

const (
	UDPPugType         PlugType = "UDP"
	TCPIOPugType       PlugType = "TCP"
	WebsocketIOPugType PlugType = "WEB_SOCKET"
	HTTPIOPugType      PlugType = "HTTP"
	HTTPSIOPugType     PlugType = "HTTPS"
)

const (
	defIOQueueThresholdLimit = 5 // queue capacity / IOQueueThreshold
)

type IPlug interface {
	Initialize(initElem *PlugInfo) error
	GetID() PlugID
	Listen() error
	Start() error
	Stop() error
}

type PlugInfo struct {
	Type PlugType
	ID   PlugID
	Addr string

	ReadQueueWorkerNum  int
	WriteQueueWorkerNum int
	ReadQueueNum        int
	WriteQueueNum       int
	AllocateBufferFunc  AllocateBufferFunc
	ReadIOCallbackFunc  ReadIOCallbackFunc
}

var (
	registerIOPlugInfoMap = map[PlugType]NewPlugFunc{
		UDPPugType: NewUDPPlug,
	}
)

func wrapCatchException(f func() error) error {
	defer func() {
		if r := recover(); r != nil {
			logger.ErrorFmt("Catch the exception, recover: %v, stack: %v", r, string(debug.Stack()))
		}
	}()

	return f()
}

//type BaseIOPlug struct {
//	network    string
//	listenAddr string
//	listener   net.Listener
//}
//
//func (t *BaseIOPlug) Initialize(network, listenAddr string, args ...interface{}) error {
//	t.network = network
//	t.listenAddr = listenAddr
//	return nil
//}
//
//func (t *BaseIOPlug) Listen() error {
//	listener, err := net.Listen(t.network, t.listenAddr)
//	if err != nil {
//		return err
//	}
//
//	t.listener = listener
//	return nil
//}
//
//func (t *BaseIOPlug) Process() error {
//
//	return nil
//}
//
//func (t *BaseIOPlug) ReadListener() error {
//	t.listener.Accept()
//}

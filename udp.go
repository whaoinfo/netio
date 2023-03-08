package gateway

import (
	"fmt"
	"github.com/whaoinfo/go-box/ctime"
	"github.com/whaoinfo/go-box/logger"
	"github.com/whaoinfo/go-box/nbuffer"
	"net"
	"runtime/debug"
	"sync/atomic"
)

type UDPPacket struct {
	SrcAddr string
	DstAddr string
	Payload interface{}
}

func NewUDPPlug() IPlug {
	return &UDPPlug{}
}

type UDPPlug struct {
	id               PlugID
	listenAddr       *net.UDPAddr
	conn             *net.UDPConn
	pushReadQueueTp  int64
	pushWriteQueueTp int64

	readQueueWorkerNum  int
	writeQueueWorkerNum int
	readQueueNum        int
	writeQueueNum       int
	readQueueLength     int64
	writeQueueLength    int64
	readQueue           chan *UDPPacket
	writeQueue          chan *UDPPacket

	allocateReadBufferFunc AllocateBufferFunc
	readIOCallbackFunc     ReadIOCallbackFunc
}

func (t *UDPPlug) Initialize(info *PlugInfo) error {
	lAddr, err := net.ResolveUDPAddr("", info.Addr)
	if err != nil {
		return err
	}

	t.id = info.ID
	t.listenAddr = lAddr

	t.readQueueWorkerNum = info.ReadQueueWorkerNum
	t.writeQueueWorkerNum = info.WriteQueueWorkerNum
	t.readQueueNum = info.ReadQueueNum
	t.writeQueueNum = info.WriteQueueNum
	t.readQueue = make(chan *UDPPacket, info.ReadQueueNum)
	t.writeQueue = make(chan *UDPPacket, info.WriteQueueNum)

	t.allocateReadBufferFunc = info.AllocateBufferFunc
	t.readIOCallbackFunc = info.ReadIOCallbackFunc

	return nil
}

func (t *UDPPlug) GetID() PlugID {
	return t.id
}

func (t *UDPPlug) Listen() error {
	conn, err := net.ListenUDP("udp", t.listenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %v, %v", t.listenAddr.String(), err)
	}

	logger.InfoFmt("The UDP connection listening at: %v", t.listenAddr.String())
	t.conn = conn
	return nil
}

func (t *UDPPlug) Start() error {
	t.watchReader()
	t.watchWriter()

	return nil
}

func (t *UDPPlug) Stop() error {
	return nil
}

func (t *UDPPlug) watchWriter() {
	go t.startWriteQueueWorkers()
}

func (t *UDPPlug) watchReader() {
	go t.readForever()
	go t.startReadQueueWorkers()
}

func (t *UDPPlug) readForever() {
	for {
		if t.read() {
			break
		}
	}

	logger.InfoFmt("The connection has stopped to read")
}

func (t *UDPPlug) startReadQueueWorkers() {
	for i := 0; i < t.readQueueWorkerNum; i++ {
		go func(workNum int) {
			logger.AllFmt("Read queue worker has started, num: %d", workNum)
			for {
				if t.popReadQueue() {
					break
				}
				logger.AllFmt("pop message on read worker %d", workNum)
			}
			logger.AllFmt("Read queue worker has stopped, num: %d", workNum)
		}(i)
	}
}

func (t *UDPPlug) read() bool {
	defer func() {
		if r := recover(); r != nil {
			logger.ErrorFmt("Catch the exception, recover: %v, stack: %v", r, string(debug.Stack()))
		}
	}()

	buf, allocateErr := t.allocateReadBufferFunc()
	if allocateErr != nil {
		logger.WarnFmt("Failed to allocate reader buffer")
		return false
	}
	buf.Rest()

	n, srcAddr, readErr := t.conn.ReadFromUDP(buf.Bytes())
	if readErr != nil {
		logger.WarnFmt("The server connection has failed to read, %v", readErr)
		return false
	}
	buf.MoveWriteOffset(n)

	pkt := &UDPPacket{
		SrcAddr: srcAddr.String(),
		Payload: buf,
	}

	if !t.pushToReadQueue(pkt) {
		go t.readIOCallbackFunc(pkt)
	}

	return false
}

func (t *UDPPlug) pushToReadQueue(pkt *UDPPacket) bool {
	queueLen := atomic.LoadInt64(&t.readQueueLength)
	if int(queueLen/defIOQueueThresholdLimit) >= t.readQueueNum {
		return false
	}

	t.pushReadQueueTp = ctime.CurrentTimestamp()
	atomic.AddInt64(&t.readQueueLength, 1)
	t.readQueue <- pkt
	t.pushReadQueueTp = ctime.CurrentTimestamp()

	return true
}

func (t *UDPPlug) popReadQueue() bool {
	defer func() {
		if r := recover(); r != nil {
			logger.ErrorFmt("Catch the exception, recover: %v, stack: %v", r, string(debug.Stack()))
		}
	}()

	pkt, ok := <-t.readQueue
	if !ok {
		return true
	}
	atomic.AddInt64(&t.readQueueLength, -1)
	t.readIOCallbackFunc(pkt)
	return false
}

func (t *UDPPlug) pushToWriteQueue(pkt *UDPPacket) bool {
	queueLen := atomic.LoadInt64(&t.writeQueueLength)
	if int(queueLen/defIOQueueThresholdLimit) >= t.writeQueueNum {
		return false
	}

	t.pushWriteQueueTp = ctime.CurrentTimestamp()
	atomic.AddInt64(&t.writeQueueLength, 1)
	t.writeQueue <- pkt
	t.pushWriteQueueTp = ctime.CurrentTimestamp()

	return true
}

func (t *UDPPlug) startWriteQueueWorkers() {
	for i := 0; i < t.writeQueueWorkerNum; i++ {
		go func(workNum int) {
			logger.AllFmt("Write queue worker has started, num: %d", workNum)
			for {
				if t.popWriteQueue() {
					break
				}
				logger.AllFmt("pop message on write worker %d", workNum)
			}
			logger.AllFmt("Write queue worker has stopped, num: %d", workNum)
		}(i)
	}
}

func (t *UDPPlug) popWriteQueue() bool {
	defer func() {
		if r := recover(); r != nil {
			logger.ErrorFmt("Catch the exception, recover: %v, stack: %v", r, string(debug.Stack()))
		}
	}()

	pkt, ok := <-t.writeQueue
	if !ok {
		return true
	}
	atomic.AddInt64(&t.writeQueueLength, -1)
	if pkt.DstAddr == "" {
		logger.WarnFmt("The dest addr of the udp pkt is a null value")
		return false
	}
	if pkt.Payload == nil {
		logger.WarnFmt("The payload of the udp pkt is a nil value")
		return false
	}

	writer, newErr := net.Dial("udp", pkt.DstAddr)
	if newErr != nil {
		logger.WarnFmt("Failed to new udp connection, dest addr: %s, err: %v", pkt.DstAddr, newErr)
		return false
	}

	buf := pkt.Payload.(*nbuffer.BufferObject)
	_, writeErr := writer.Write(buf.Bytes())
	if writeErr != nil {
		logger.WarnFmt("The udp connection has failed to write, %v", writeErr)
	}

	return false
}

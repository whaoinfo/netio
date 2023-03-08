package netio

import (
	"github.com/whaoinfo/go-box/logger"
)

type NetIO struct {
	plugMap map[PlugID]IPlug
}

func (t *NetIO) Initialize(plugInfoList []PlugInfo, args ...interface{}) error {
	t.plugMap = make(map[PlugID]IPlug)

	for _, info := range plugInfoList {
		newPlugFunc := registerIOPlugInfoMap[info.Type]
		if newPlugFunc == nil {
			logger.WarnFmt("The %v plug dose not exists", info.Type)
			continue
		}

		plug := newPlugFunc()
		if err := plug.Initialize(&info); err != nil {
			logger.WarnFmt("Failed to initialize the %v plug, %v", info.ID, err)
			continue
		}
		t.plugMap[info.ID] = plug
	}

	return nil
}

func (t *NetIO) Start() error {
	for id, plug := range t.plugMap {
		if err := plug.Listen(); err != nil {
			logger.WarnFmt("The %v plug failed to listening, %v", id, err)
			continue
		}
		if err := plug.Start(); err != nil {
			logger.WarnFmt("The %v plug starting has failed, %v", id, err)
			continue
		}

		logger.InfoFmt("The %v plug has started", id)
	}

	return nil
}

func (t *NetIO) Stop() error {
	for id, plug := range t.plugMap {
		if err := plug.Stop(); err != nil {
			logger.WarnFmt("The AX plug failed to stopping, %v", id, err)
		}
	}

	return nil
}

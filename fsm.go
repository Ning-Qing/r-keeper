package rkeeper

import (
	"github.com/looplab/fsm"
)

const (
	// StateNomal 普通节点状态。
	StateNomal = "nomal"
	// StateMaster 主节点状态。
	StateMaster = "master"
)

const (
	// EventElectoralSuccess 选举成功。
	EventElectoralSuccess = "electoral_success"
	// EventElectoralDefeat 选举失败。
	EventElectoralDefeat = "electoral_defeat"
	// EventRenewSuccess 续约成功。
	EventRenewSuccess = "renew_success"
	// EventRenewDefeat 续约失败。
	EventRenewDefeat = "renew_defeat"
)

func loadFSM(node *Node) {
	node.fsm = fsm.NewFSM(
		StateNomal,
		fsm.Events{
			// EventElectoralSuccess 选举成功，进入主节点状态。
			{Name: EventElectoralSuccess, Src: []string{StateNomal}, Dst: StateMaster},
			// EventElectoralDefeat 选举失败，进入普通节点状态。
			{Name: EventElectoralDefeat, Src: []string{StateMaster, StateNomal}, Dst: StateNomal},
			// EventRenewSuccess 续约成功，保持主节点状态。
			{Name: EventRenewSuccess, Src: []string{StateMaster}, Dst: StateMaster},
			// EventRenewDefeat 续约失败，进入普通节点状态。
			{Name: EventRenewDefeat, Src: []string{StateMaster}, Dst: StateNomal},
		},
		fsm.Callbacks{
			"enter_master": node.enterMaster,
			"enter_nomal":  node.enterNomal,
		},
	)
}

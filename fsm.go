package rkeeper

import (
	"github.com/looplab/fsm"
)

const (
	// StateNomal represents the normal node state. // 普通节点状态
	StateNomal = "nomal"
	// StateMaster represents the master node state. // 主节点状态
	StateMaster = "master"
)

const (
	// EventElectoralSuccess represents successful election. // 选举成功
	EventElectoralSuccess = "electoral_success"
	// EventElectoralDefeat represents failed election. // 选举失败
	EventElectoralDefeat = "electoral_defeat"
	// EventRenewSuccess represents successful lock renewal. // 续约成功
	EventRenewSuccess = "renew_success"
	// EventRenewDefeat represents failed lock renewal. // 续约失败
	EventRenewDefeat = "renew_defeat"
)

func loadFSM(node *Node) {
	node.fsm = fsm.NewFSM(
		StateNomal,
		fsm.Events{
			// EventElectoralSuccess: election success, enter master state. // 选举成功，进入主节点状态
			{Name: EventElectoralSuccess, Src: []string{StateNomal}, Dst: StateMaster},
			// EventElectoralDefeat: election failure, enter normal state. // 选举失败，进入普通节点状态
			{Name: EventElectoralDefeat, Src: []string{StateMaster, StateNomal}, Dst: StateNomal},
			// EventRenewSuccess: renewal success, keep master state. // 续约成功，保持主节点状态
			{Name: EventRenewSuccess, Src: []string{StateMaster}, Dst: StateMaster},
			// EventRenewDefeat: renewal failure, enter normal state. // 续约失败，进入普通节点状态
			{Name: EventRenewDefeat, Src: []string{StateMaster}, Dst: StateNomal},
		},
		fsm.Callbacks{
			"enter_master": node.enterMaster,
			"enter_nomal":  node.enterNomal,
		},
	)
}

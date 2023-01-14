package register

import (
	"github.com/chenjiale12345/scheduler/pkg/yoda"
	"github.com/spf13/cobra"
	"k8s.io/kubernetes/cmd/kube-scheduler/app"
)

func Register() *cobra.Command {
	return app.NewSchedulerCommand(
		app.WithPlugin(yoda.Name, yoda.New),
	)
}

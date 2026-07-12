//go:generate go run pkg/codegen/cleanup/main.go
//go:generate /bin/rm -rf pkg/generated
//go:generate go run pkg/codegen/main.go
//go:generate /bin/bash scripts/generate-manifest

package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	corev1 "github.com/rancher/wrangler/v3/pkg/generated/controllers/core"
	discoveryv1 "github.com/rancher/wrangler/v3/pkg/generated/controllers/discovery"
	"github.com/rancher/wrangler/v3/pkg/kubeconfig"
	"github.com/rancher/wrangler/v3/pkg/leader"
	"github.com/rancher/wrangler/v3/pkg/signals"
	"github.com/rancher/wrangler/v3/pkg/start"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	"k8s.io/client-go/kubernetes"

	"github.com/harvester/networkfs-manager/pkg/controller/endpointslice"
	"github.com/harvester/networkfs-manager/pkg/controller/networkfilesystem"
	"github.com/harvester/networkfs-manager/pkg/controller/service"
	"github.com/harvester/networkfs-manager/pkg/controller/sharemanager"
	ntefsv1 "github.com/harvester/networkfs-manager/pkg/generated/controllers/harvesterhci.io"
	ctrllonghorn "github.com/harvester/networkfs-manager/pkg/generated/controllers/longhorn.io"
	utils "github.com/harvester/networkfs-manager/pkg/utils"
)

func main() {
	var opt utils.Option
	app := cli.NewApp()
	app.Name = "networkFS-manager"
	app.Version = utils.FriendlyVersion()
	app.Usage = "networkFS-manager help to manage network filesystem for downstream cluster or VMs"
	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:        "kubeconfig",
			EnvVars:     []string{"KUBECONFIG"},
			Destination: &opt.KubeConfig,
			Usage:       "Kube config for accessing k8s cluster",
		},
		&cli.IntFlag{
			Name:        "threadiness",
			Value:       2,
			DefaultText: "2",
			Destination: &opt.Threadiness,
		},
		&cli.BoolFlag{
			Name:        "debug",
			EnvVars:     []string{"DEBUG"},
			Usage:       "enable debug logs",
			Destination: &opt.Debug,
		},
		&cli.StringFlag{
			Name:        "namespace",
			Value:       "harvester-system",
			DefaultText: "harvester-system",
			EnvVars:     []string{"HARVESTER_NAMESPACE"},
			Destination: &opt.Namespace,
		},
	}

	app.Action = func(_ *cli.Context) error {
		initLogs(&opt)
		return run(&opt)
	}

	if err := app.Run(os.Args); err != nil {
		logrus.Fatal(err)
	}
}

func initLogs(opt *utils.Option) {
	if opt.Debug {
		logrus.SetLevel(logrus.DebugLevel)
		logrus.Debugf("Loglevel set to [%v]", logrus.DebugLevel)
	}
}

func run(opt *utils.Option) error {
	logrus.Infof("NetworkFS manager %s is starting", utils.FriendlyVersion())
	if opt.Namespace == "" {
		return errors.New("namespace cannot be empty")
	}

	ctx := signals.SetupSignalContext()
	config, err := kubeconfig.GetNonInteractiveClientConfig(opt.KubeConfig).ClientConfig()
	if err != nil {
		return fmt.Errorf("failed to find kubeconfig: %v", err)
	}

	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("error get client from kubeconfig: %s", err.Error())
	}

	clientNetfs, err := ntefsv1.NewFactoryFromConfig(config)
	if err != nil {
		return fmt.Errorf("failed to create networkFS controller: %v", err)
	}

	clientv1, err := corev1.NewFactoryFromConfig(config)
	if err != nil {
		return fmt.Errorf("failed to create core controller: %v", err)
	}

	clientDiscoveryv1, err := discoveryv1.NewFactoryFromConfig(config)
	if err != nil {
		return fmt.Errorf("failed to create endpointslices controller: %v", err)
	}

	lhClient, err := lhclientset.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("failed to create longhorn client: %v", err)
	}

	lhCtrlClient, err := ctrllonghorn.NewFactoryFromConfig(config)
	if err != nil {
		return fmt.Errorf("failed to create longhorn controller: %v", err)
	}

	endpointSlices := clientDiscoveryv1.Discovery().V1().EndpointSlice()
	services := clientv1.Core().V1().Service()
	networkFilsystems := clientNetfs.Harvesterhci().V1beta1().NetworkFilesystem()
	sharemanagers := lhCtrlClient.Longhorn().V1beta2().ShareManager()

	cb := func(ctx context.Context) {
		if err := endpointslice.Register(ctx, endpointSlices, networkFilsystems, services, opt); err != nil {
			logrus.Errorf("failed to register endpointslice controller: %v", err)
		}

		if err := service.Register(ctx, services, networkFilsystems, opt); err != nil {
			logrus.Errorf("failed to register service controller: %v", err)
		}

		if err := networkfilesystem.Register(ctx, clientv1.Core().V1(), lhClient, endpointSlices, networkFilsystems, opt); err != nil {
			logrus.Errorf("failed to register networkfilesystem controller: %v", err)
		}

		if err := sharemanager.Register(ctx, sharemanagers, networkFilsystems, opt); err != nil {
			logrus.Errorf("failed to register sharemanager controller: %v", err)
		}

		if err := start.All(ctx, opt.Threadiness, clientNetfs, clientv1, clientDiscoveryv1, lhCtrlClient); err != nil {
			logrus.Errorf("failed to start controller: %v", err)
		}

		<-ctx.Done()
	}

	leader.RunOrDie(ctx, opt.Namespace, "harvester-networkfs-manager", client, cb)

	logrus.Infof("NetworkFS manager is shutting down")
	return nil
}

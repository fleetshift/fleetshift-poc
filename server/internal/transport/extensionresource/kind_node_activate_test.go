package extensionresource_test

import (
	"context"
	"testing"

	"buf.build/go/protovalidate"

	kindaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kind"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/delivery"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/dynamicapi"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/extensionresource"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/platformresource"
)

func TestActivate_KindClusterAndNode(t *testing.T) {
	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}
	mux := dynamicapi.NewDynamicServiceMux()
	files := dynamicapi.NewDynamicFileRegistry()
	v, err := protovalidate.New()
	if err != nil {
		t.Fatal(err)
	}
	activator := &extensionresource.DynamicSchemaActivator{
		Registry:     extensionresource.NewActiveResourceRegistry(),
		GRPCMux:      mux,
		FileRegistry: files,
		Deps: extensionresource.Deps{
			Resources: application.NewExtensionResourceService(store, nil, nil, nil, nil),
			Validator: v,
		},
		PlatformDeps: platformresource.Deps{Resources: application.NewPlatformResourceService(store)},
	}
	typeSvc := application.NewExtensionResourceTypeService(store)
	mgr := application.NewAddonManager(application.AddonManagerDeps{
		Router:    delivery.NewRoutingDeliveryService(),
		TypeSvc:   typeSvc,
		Activator: activator,
	})
	ctx := context.Background()
	if err := mgr.Enable(ctx, kindaddon.Descriptor()); err != nil {
		t.Fatalf("Enable: %v", err)
	}
	if err := mgr.Connect(ctx, kindaddon.Descriptor().ID, application.ConnectInput{
		Schemas: []domain.ExtensionResourceSchema{kindaddon.Schema(), kindaddon.NodeSchema()},
	}); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	info := mux.ServiceInfo()
	names := make([]string, 0, len(info))
	for name := range info {
		names = append(names, name)
	}
	t.Logf("services: %v", names)

	if _, ok := info["kind.fleetshift.v1.ClusterService"]; !ok {
		t.Fatalf("missing ClusterService; got %v", names)
	}
	if _, ok := info["kind.fleetshift.v1.NodeService"]; !ok {
		t.Fatalf("missing NodeService; got %v", names)
	}
	if _, err := files.FindDescriptorByName("kind.fleetshift.v1.NodeService"); err != nil {
		t.Fatalf("FileRegistry NodeService: %v", err)
	}
	if _, err := files.FindDescriptorByName("kind.fleetshift.v1.Node"); err != nil {
		t.Fatalf("FileRegistry Node message: %v", err)
	}
	if _, err := files.FindDescriptorByName("kind.fleetshift.v1.ClusterService"); err != nil {
		t.Fatalf("FileRegistry ClusterService: %v", err)
	}
	if _, err := files.FindFileByPath("addons/kind/v1/kind_cluster_spec.proto"); err != nil {
		t.Fatalf("FileRegistry kind_cluster_spec.proto: %v", err)
	}
	if _, err := files.FindFileByPath("dynamic/kind/fleetshift/v1/cluster_service.proto"); err != nil {
		t.Fatalf("FileRegistry cluster_service.proto: %v", err)
	}
}

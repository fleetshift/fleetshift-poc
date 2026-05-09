package managedresource

import (
	"fmt"
	"net/http"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	v1reflectiongrpc "google.golang.org/grpc/reflection/grpc_reflection_v1"
	v1alphareflectiongrpc "google.golang.org/grpc/reflection/grpc_reflection_v1alpha"
	"google.golang.org/grpc/status"
)

// DynamicServiceMux dispatches gRPC calls for dynamically registered
// managed resource services. It is wired as the server's
// [grpc.UnknownServiceHandler], so requests to services that were not
// registered at server creation time are routed here instead of being
// rejected outright.
//
// Services can be added and removed at any time (thread-safe).
// Composite reflection exposes the dynamic services alongside statically
// registered ones.
type DynamicServiceMux struct {
	mu       sync.RWMutex
	services map[string]*dynamicEntry // keyed by gRPC full service name
}

type dynamicEntry struct {
	methods map[string]grpc.MethodDesc // keyed by method name
	desc    *grpc.ServiceDesc
	svcInfo grpc.ServiceInfo
}

// NewDynamicServiceMux creates an empty mux ready for service
// registration.
func NewDynamicServiceMux() *DynamicServiceMux {
	return &DynamicServiceMux{
		services: make(map[string]*dynamicEntry),
	}
}

// Register adds a compiled managed resource service to the mux. The
// service becomes immediately routable. Returns an error if a service
// with the same name is already registered — use [Replace] for
// atomic updates.
func (m *DynamicServiceMux) Register(svc *RegisteredService) error {
	entry := buildEntry(svc)

	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.services[svc.Desc.ServiceName]; exists {
		return fmt.Errorf("dynamic mux: service %q already registered", svc.Desc.ServiceName)
	}
	m.services[svc.Desc.ServiceName] = entry
	return nil
}

// Replace atomically swaps an existing service registration. If the
// service is not currently registered it is added (same as [Register]).
// In-flight requests that already resolved the old entry are unaffected;
// new requests route to the replacement immediately.
func (m *DynamicServiceMux) Replace(svc *RegisteredService) {
	entry := buildEntry(svc)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.services[svc.Desc.ServiceName] = entry
}

// Deregister removes a service from the mux. Subsequent calls to the
// service will receive Unimplemented. No-op if the service is not
// registered.
func (m *DynamicServiceMux) Deregister(serviceName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.services, serviceName)
}

func buildEntry(svc *RegisteredService) *dynamicEntry {
	entry := &dynamicEntry{
		methods: make(map[string]grpc.MethodDesc, len(svc.Desc.Methods)),
		desc:    svc.Desc,
		svcInfo: buildServiceInfo(svc.Desc),
	}
	for _, md := range svc.Desc.Methods {
		entry.methods[md.MethodName] = md
	}
	return entry
}

// Handle is the [grpc.StreamHandler] that dispatches to dynamic
// services. It extracts the full method from the transport stream,
// resolves the service and method, and adapts the unary handler into
// a stream-based call.
//
// Wire it as:
//
//	grpc.NewServer(grpc.UnknownServiceHandler(mux.Handle))
func (m *DynamicServiceMux) Handle(srv any, stream grpc.ServerStream) error {
	fullMethod, ok := grpc.MethodFromServerStream(stream)
	if !ok {
		return status.Error(codes.Internal, "dynamic mux: no method in stream context")
	}

	serviceName, methodName, err := parseFullMethod(fullMethod)
	if err != nil {
		return status.Errorf(codes.Unimplemented, "dynamic mux: %v", err)
	}

	m.mu.RLock()
	entry, ok := m.services[serviceName]
	m.mu.RUnlock()
	if !ok {
		return status.Errorf(codes.Unimplemented, "unknown service %s", serviceName)
	}

	md, ok := entry.methods[methodName]
	if !ok {
		return status.Errorf(codes.Unimplemented, "unknown method %s/%s", serviceName, methodName)
	}

	return unaryToStream(md)(srv, stream)
}

// ServiceInfo returns gRPC service info for all currently registered
// dynamic services. Used by composite reflection to merge with the
// statically registered services on the gRPC server.
func (m *DynamicServiceMux) ServiceInfo() map[string]grpc.ServiceInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[string]grpc.ServiceInfo, len(m.services))
	for name, entry := range m.services {
		out[name] = entry.svcInfo
	}
	return out
}

// unaryToStream wraps a grpc.MethodDesc.Handler (which has the unary
// handler signature) in a grpc.StreamHandler. The stream is used as a
// transport: recv one request message, call the unary handler, send
// the response.
func unaryToStream(md grpc.MethodDesc) grpc.StreamHandler {
	return func(srv any, stream grpc.ServerStream) error {
		resp, err := md.Handler(srv, stream.Context(), func(msg any) error {
			return stream.RecvMsg(msg)
		}, nil)
		if err != nil {
			return err
		}
		return stream.SendMsg(resp)
	}
}

// parseFullMethod splits "/service.Name/Method" into service and method
// components.
func parseFullMethod(fullMethod string) (service, method string, err error) {
	fullMethod = strings.TrimPrefix(fullMethod, "/")
	pos := strings.LastIndex(fullMethod, "/")
	if pos < 0 {
		return "", "", fmt.Errorf("malformed method %q", fullMethod)
	}
	return fullMethod[:pos], fullMethod[pos+1:], nil
}

func buildServiceInfo(desc *grpc.ServiceDesc) grpc.ServiceInfo {
	methods := make([]grpc.MethodInfo, 0, len(desc.Methods)+len(desc.Streams))
	for _, m := range desc.Methods {
		methods = append(methods, grpc.MethodInfo{
			Name:           m.MethodName,
			IsClientStream: false,
			IsServerStream: false,
		})
	}
	for _, s := range desc.Streams {
		methods = append(methods, grpc.MethodInfo{
			Name:           s.StreamName,
			IsClientStream: s.ClientStreams,
			IsServerStream: s.ServerStreams,
		})
	}
	return grpc.ServiceInfo{
		Methods:  methods,
		Metadata: desc.Metadata,
	}
}

// DynamicHTTPMux provides a thread-safe HTTP mux for dynamically
// registered managed resource services. It uses handler indirection:
// a stable dispatcher is registered once per prefix on the underlying
// [http.ServeMux], and the actual handler is swapped atomically via
// a map. This avoids the Go 1.22 panic on duplicate pattern
// registrations and provides zero-downtime replacement.
type DynamicHTTPMux struct {
	mu       sync.RWMutex
	mux      *http.ServeMux
	handlers map[string]*httpHandlerEntry // prefix → current handler + connection
}

// NewDynamicHTTPMux creates a new dynamic HTTP mux backed by the given
// [http.ServeMux]. If mux is nil, a new one is created.
func NewDynamicHTTPMux(mux *http.ServeMux) *DynamicHTTPMux {
	if mux == nil {
		mux = http.NewServeMux()
	}
	return &DynamicHTTPMux{
		mux:      mux,
		handlers: make(map[string]*httpHandlerEntry),
	}
}

// Register adds HTTP routes for a managed resource service. The
// grpcAddr is the loopback address of the gRPC server (for proxying).
// Returns an error if a service with the same plural is already
// registered — use [Replace] for atomic updates.
func (m *DynamicHTTPMux) Register(svc *RegisteredService, grpcAddr string) error {
	prefix := "/v1/" + svc.Config.CollectionID()

	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.handlers[prefix]; exists {
		return fmt.Errorf("dynamic http mux: routes for %q already registered", svc.Config.CollectionID())
	}
	return m.setHandler(svc, grpcAddr, prefix)
}

// Replace atomically swaps the HTTP handler for a service. If the
// service is not currently registered it is added (same as [Register]).
func (m *DynamicHTTPMux) Replace(svc *RegisteredService, grpcAddr string) error {
	prefix := "/v1/" + svc.Config.CollectionID()
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.setHandler(svc, grpcAddr, prefix)
}

// setHandler builds the handler and installs it. If the prefix has
// never been seen, a stable dispatcher is registered on the underlying
// mux. Closes the previous entry's gRPC connection when replacing.
// Must be called with m.mu held.
func (m *DynamicHTTPMux) setHandler(svc *RegisteredService, grpcAddr, prefix string) error {
	entry, err := buildHTTPHandler(svc, grpcAddr)
	if err != nil {
		return err
	}

	prev, dispatched := m.handlers[prefix]
	if dispatched && prev != nil && prev.conn != nil {
		go prev.conn.Close()
	}
	m.handlers[prefix] = entry

	if !dispatched {
		dispatcher := m.dispatcher(prefix)
		m.mux.HandleFunc(prefix, dispatcher)
		m.mux.HandleFunc(prefix+"/", dispatcher)
	}
	return nil
}

// Deregister removes the HTTP handler for a service. plural is the
// PascalCase plural (e.g. "KindClusters"); it is lowered to the
// lowerCamelCase collection identifier for path matching.
func (m *DynamicHTTPMux) Deregister(plural string) {
	collectionID := strings.ToLower(plural[:1]) + plural[1:]
	prefix := "/v1/" + collectionID
	m.mu.Lock()
	defer m.mu.Unlock()
	if entry, ok := m.handlers[prefix]; ok && entry != nil && entry.conn != nil {
		go entry.conn.Close()
	}
	delete(m.handlers, prefix)
}

// dispatcher returns a stable handler function for a prefix. It looks
// up the current handler on each request under a read lock. If no
// handler is registered (deregistered), it returns 404.
func (m *DynamicHTTPMux) dispatcher(prefix string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		m.mu.RLock()
		entry, ok := m.handlers[prefix]
		m.mu.RUnlock()
		if !ok || entry == nil {
			http.NotFound(w, r)
			return
		}
		entry.handler.ServeHTTP(w, r)
	}
}

// ServeMux returns the underlying [http.ServeMux] for wiring into the
// top-level HTTP server.
func (m *DynamicHTTPMux) ServeMux() *http.ServeMux {
	return m.mux
}

// CompositeServiceInfoProvider merges service info from the static
// gRPC server and the dynamic mux. This allows reflection to discover
// both statically and dynamically registered services.
type CompositeServiceInfoProvider struct {
	Server     *grpc.Server
	DynamicMux *DynamicServiceMux
}

// GetServiceInfo returns the merged service info map.
func (c *CompositeServiceInfoProvider) GetServiceInfo() map[string]grpc.ServiceInfo {
	info := c.Server.GetServiceInfo()
	for name, si := range c.DynamicMux.ServiceInfo() {
		info[name] = si
	}
	return info
}

// RegisterCompositeReflection registers gRPC reflection (both v1 and
// v1alpha) using a [CompositeServiceInfoProvider] that merges static
// server services with dynamic mux services, and a
// [CompositeDescriptorResolver] that resolves file descriptors for
// dynamically compiled services alongside statically compiled ones.
// This replaces the standard [reflection.Register] call so dynamically
// registered managed resource services are fully discoverable via
// reflection (e.g. grpcurl list, grpcurl describe).
func RegisterCompositeReflection(s *grpc.Server, mux *DynamicServiceMux, fileRegistry *DynamicFileRegistry) {
	composite := &CompositeServiceInfoProvider{Server: s, DynamicMux: mux}
	resolver := NewCompositeDescriptorResolver(fileRegistry)
	opts := reflection.ServerOptions{
		Services:           composite,
		DescriptorResolver: resolver,
	}
	v1alphareflectiongrpc.RegisterServerReflectionServer(s, reflection.NewServer(opts))
	v1reflectiongrpc.RegisterServerReflectionServer(s, reflection.NewServerV1(opts))
}


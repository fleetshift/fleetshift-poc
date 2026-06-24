package dynamicapi

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

// RegisterDesc adds a gRPC service descriptor to the mux. Returns an
// error if a service with the same name is already registered — use
// [ReplaceDesc] for atomic updates.
func (m *DynamicServiceMux) RegisterDesc(desc *grpc.ServiceDesc) error {
	entry := buildEntryFromDesc(desc)

	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.services[desc.ServiceName]; exists {
		return fmt.Errorf("dynamic mux: service %q already registered", desc.ServiceName)
	}
	m.services[desc.ServiceName] = entry
	return nil
}

// ReplaceDesc atomically swaps a gRPC service descriptor. If the
// service is not currently registered it is added.
func (m *DynamicServiceMux) ReplaceDesc(desc *grpc.ServiceDesc) {
	entry := buildEntryFromDesc(desc)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.services[desc.ServiceName] = entry
}

// Deregister removes a service from the mux. Subsequent calls to the
// service will receive Unimplemented. No-op if the service is not
// registered.
func (m *DynamicServiceMux) Deregister(serviceName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.services, serviceName)
}

func buildEntryFromDesc(desc *grpc.ServiceDesc) *dynamicEntry {
	entry := &dynamicEntry{
		methods: make(map[string]grpc.MethodDesc, len(desc.Methods)),
		desc:    desc,
		svcInfo: buildServiceInfo(desc),
	}
	for _, md := range desc.Methods {
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
//
// A single shared [grpc.ClientConn] is used for all handlers — routing
// to the correct gRPC service is determined by the fully-qualified
// method name in each RPC, not by connection identity.
type DynamicHTTPMux struct {
	mu       sync.RWMutex
	mux      *http.ServeMux
	conn     *grpc.ClientConn
	handlers map[string]http.HandlerFunc // prefix → current handler
}

// NewDynamicHTTPMux creates a new dynamic HTTP mux backed by the given
// [http.ServeMux] and gRPC client connection. If mux is nil, a new one
// is created. The conn is shared across all handlers for the lifetime
// of this mux — callers must not close it while the mux is in use.
func NewDynamicHTTPMux(mux *http.ServeMux, conn *grpc.ClientConn) *DynamicHTTPMux {
	if mux == nil {
		mux = http.NewServeMux()
	}
	return &DynamicHTTPMux{
		mux:      mux,
		conn:     conn,
		handlers: make(map[string]http.HandlerFunc),
	}
}

// RegisterPrefixHandler adds an HTTP handler at the given prefix.
func (m *DynamicHTTPMux) RegisterPrefixHandler(prefix string, handler http.HandlerFunc) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.handlers[prefix]; exists {
		return fmt.Errorf("dynamic http mux: routes for %q already registered", prefix)
	}
	m.installPrefixHandler(prefix, handler)
	return nil
}

// ReplacePrefixHandler atomically swaps the handler for a prefix. Any
// deprecatedPrefixes are removed in the same lock hold.
func (m *DynamicHTTPMux) ReplacePrefixHandler(prefix string, handler http.HandlerFunc, deprecatedPrefixes ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.installPrefixHandler(prefix, handler)
	for _, dp := range deprecatedPrefixes {
		if dp == prefix {
			continue
		}
		delete(m.handlers, dp)
	}
}

// installPrefixHandler installs a handler for a single prefix. If the
// prefix has never been seen, a stable dispatcher is registered on the
// underlying mux. Must be called with m.mu held.
func (m *DynamicHTTPMux) installPrefixHandler(prefix string, handler http.HandlerFunc) {
	_, dispatched := m.handlers[prefix]
	m.handlers[prefix] = handler

	if !dispatched {
		dispatcher := m.dispatcher(prefix)
		m.mux.HandleFunc(prefix, dispatcher)
		m.mux.HandleFunc(prefix+"/", dispatcher)
	}
}

// Conn returns the shared gRPC loopback client connection. Platform
// handlers use this for the same conn.Invoke() pattern as extension
// HTTP handlers.
func (m *DynamicHTTPMux) Conn() *grpc.ClientConn {
	return m.conn
}

// DeregisterByPrefix removes the HTTP handler registered under the
// given exact prefix string.
func (m *DynamicHTTPMux) DeregisterByPrefix(prefix string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.handlers, prefix)
}

// dispatcher returns a stable handler function for a prefix. It looks
// up the current handler on each request under a read lock. If no
// handler is registered (deregistered), it returns 404.
func (m *DynamicHTTPMux) dispatcher(prefix string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		m.mu.RLock()
		handler, ok := m.handlers[prefix]
		m.mu.RUnlock()
		if !ok {
			http.NotFound(w, r)
			return
		}
		handler.ServeHTTP(w, r)
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

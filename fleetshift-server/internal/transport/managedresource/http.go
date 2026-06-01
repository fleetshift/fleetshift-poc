package managedresource

import (
	"context"
	"io"
	"net/http"
	"strconv"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// RegisterHTTP registers REST/JSON routes for the dynamic service on the
// given HTTP mux. Routes follow the AIP HTTP binding pattern:
//
//	POST   /v1/{collection}          -> Create
//	GET    /v1/{collection}/{id}     -> Get
//	GET    /v1/{collection}          -> List
//	DELETE /v1/{collection}/{id}     -> Delete
//
// The handler connects to the gRPC server at grpcAddr to forward requests.
//
// For dynamic (hot-swappable) registration, prefer [DynamicHTTPMux] which
// uses handler indirection to support atomic replace and deregister.
func RegisterHTTP(mux *http.ServeMux, svc *RegisteredService, grpcAddr string) error {
	entry, err := buildHTTPHandler(svc, grpcAddr)
	if err != nil {
		return err
	}

	prefix := "/v1/" + svc.Config.CollectionID()

	// Register both the exact path and the subtree pattern so that
	// /v1/{collection} (list, create) and /v1/{collection}/{id} (get,
	// delete) are both routed here instead of falling through to the
	// grpc-gateway catch-all on /v1/.
	mux.HandleFunc(prefix, entry.handler)
	mux.HandleFunc(prefix+"/", entry.handler)

	return nil
}

// httpHandlerEntry pairs an HTTP handler with its gRPC client
// connection so the connection can be closed when the handler is
// replaced or deregistered.
type httpHandlerEntry struct {
	handler http.HandlerFunc
	conn    *grpc.ClientConn
}

// buildHTTPHandler creates the HTTP handler function for a managed
// resource service without registering it on any mux. Used by both
// [RegisterHTTP] (static) and [DynamicHTTPMux] (dynamic).
func buildHTTPHandler(svc *RegisteredService, grpcAddr string) (*httpHandlerEntry, error) {
	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	prefix := "/v1/" + svc.Config.CollectionID()

	handler := func(w http.ResponseWriter, r *http.Request) {
		rest := strings.TrimPrefix(r.URL.Path, prefix)

		switch {
		case r.Method == http.MethodPost && (rest == "" || rest == "/"):
			handleHTTPCreate(w, r, conn, svc)
		case r.Method == http.MethodPost && strings.HasSuffix(rest, ":resume"):
			id := strings.TrimPrefix(rest, "/")
			id = strings.TrimSuffix(id, ":resume")
			handleHTTPResume(w, r, conn, svc, id)
		case r.Method == http.MethodGet && (rest == "" || rest == "/"):
			handleHTTPList(w, r, conn, svc)
		case r.Method == http.MethodGet && len(rest) > 1:
			id := strings.TrimPrefix(rest, "/")
			handleHTTPGet(w, r, conn, svc, id)
		case r.Method == http.MethodDelete && len(rest) > 1:
			id := strings.TrimPrefix(rest, "/")
			handleHTTPDelete(w, r, conn, svc, id)
		default:
			http.NotFound(w, r)
		}
	}
	return &httpHandlerEntry{handler: handler, conn: conn}, nil
}

func handleHTTPCreate(w http.ResponseWriter, r *http.Request, conn *grpc.ClientConn, svc *RegisteredService) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		httpError(w, codes.InvalidArgument, "read body: "+err.Error())
		return
	}

	// Parse the request body as the resource message (spec + optional fields).
	resource := dynamicpb.NewMessage(svc.Descriptors.Resource)
	if err := protojson.Unmarshal(body, resource); err != nil {
		httpError(w, codes.InvalidArgument, "parse body: "+err.Error())
		return
	}

	// Extract the ID from the query parameter (AIP pattern: ?{singular}_id=xxx)
	lower := strings.ToLower(svc.Config.Singular[:1]) + svc.Config.Singular[1:]
	id := r.URL.Query().Get(lower + "_id")
	if id == "" {
		httpError(w, codes.InvalidArgument, lower+"_id query parameter is required")
		return
	}

	createReq := dynamicpb.NewMessage(svc.Descriptors.CreateRequest)
	idField := svc.Descriptors.CreateRequest.Fields().ByNumber(1)
	resourceField := svc.Descriptors.CreateRequest.Fields().ByNumber(2)
	createReq.Set(idField, stringValue(id))
	createReq.Set(resourceField, messageValue(resource))

	resp := dynamicpb.NewMessage(svc.Descriptors.Resource)
	method := "/" + svc.Config.ServiceName() + "/Create" + svc.Config.Singular
	if err := conn.Invoke(grpcContext(r), method, createReq, resp); err != nil {
		grpcHTTPError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, resp)
}

func handleHTTPGet(w http.ResponseWriter, r *http.Request, conn *grpc.ClientConn, svc *RegisteredService, id string) {
	getReq := dynamicpb.NewMessage(svc.Descriptors.GetRequest)
	nameField := svc.Descriptors.GetRequest.Fields().ByName("name")
	getReq.Set(nameField, stringValue(svc.Config.Collection()+id))

	resp := dynamicpb.NewMessage(svc.Descriptors.Resource)
	method := "/" + svc.Config.ServiceName() + "/Get" + svc.Config.Singular
	if err := conn.Invoke(grpcContext(r), method, getReq, resp); err != nil {
		grpcHTTPError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, resp)
}

func handleHTTPList(w http.ResponseWriter, r *http.Request, conn *grpc.ClientConn, svc *RegisteredService) {
	listReq := dynamicpb.NewMessage(svc.Descriptors.ListRequest)

	if v := r.URL.Query().Get("page_size"); v != "" {
		if field := svc.Descriptors.ListRequest.Fields().ByName("page_size"); field != nil {
			n, err := strconv.ParseInt(v, 10, 32)
			if err != nil {
				httpError(w, codes.InvalidArgument, "invalid page_size: "+err.Error())
				return
			}
			listReq.Set(field, protoreflect.ValueOfInt32(int32(n)))
		}
	}
	if v := r.URL.Query().Get("page_token"); v != "" {
		if field := svc.Descriptors.ListRequest.Fields().ByName("page_token"); field != nil {
			listReq.Set(field, protoreflect.ValueOfString(v))
		}
	}

	resp := dynamicpb.NewMessage(svc.Descriptors.ListResponse)
	method := "/" + svc.Config.ServiceName() + "/List" + svc.Config.Plural
	if err := conn.Invoke(grpcContext(r), method, listReq, resp); err != nil {
		grpcHTTPError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, resp)
}

func handleHTTPDelete(w http.ResponseWriter, r *http.Request, conn *grpc.ClientConn, svc *RegisteredService, id string) {
	deleteReq := dynamicpb.NewMessage(svc.Descriptors.DeleteRequest)
	nameField := svc.Descriptors.DeleteRequest.Fields().ByName("name")
	deleteReq.Set(nameField, stringValue(svc.Config.Collection()+id))

	resp := dynamicpb.NewMessage(svc.Descriptors.Resource)
	method := "/" + svc.Config.ServiceName() + "/Delete" + svc.Config.Singular
	if err := conn.Invoke(grpcContext(r), method, deleteReq, resp); err != nil {
		grpcHTTPError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, resp)
}

func handleHTTPResume(w http.ResponseWriter, r *http.Request, conn *grpc.ClientConn, svc *RegisteredService, id string) {
	resumeReq := dynamicpb.NewMessage(svc.Descriptors.ResumeRequest)
	nameField := svc.Descriptors.ResumeRequest.Fields().ByName("name")
	resumeReq.Set(nameField, stringValue(svc.Config.Collection()+id))

	// Parse optional request body for user_signature / valid_until.
	body, err := io.ReadAll(r.Body)
	if err != nil {
		httpError(w, codes.InvalidArgument, "read body: "+err.Error())
		return
	}
	if len(body) > 0 {
		if err := protojson.Unmarshal(body, resumeReq); err != nil {
			httpError(w, codes.InvalidArgument, "parse body: "+err.Error())
			return
		}
		// Re-set name since body unmarshal might have cleared it.
		resumeReq.Set(nameField, stringValue(svc.Config.Collection()+id))
	}

	resp := dynamicpb.NewMessage(svc.Descriptors.Resource)
	method := "/" + svc.Config.ServiceName() + "/Resume" + svc.Config.Singular
	if err := conn.Invoke(grpcContext(r), method, resumeReq, resp); err != nil {
		grpcHTTPError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, resp)
}

// grpcContext returns a context that forwards the HTTP Authorization
// header as outgoing gRPC metadata so that the server-side authn
// interceptor can authenticate the caller.
func grpcContext(r *http.Request) context.Context {
	auth := r.Header.Get("Authorization")
	if auth == "" {
		return r.Context()
	}
	return metadata.AppendToOutgoingContext(r.Context(), "authorization", auth)
}

func stringValue(s string) protoreflect.Value {
	return protoreflect.ValueOfString(s)
}

func messageValue(m *dynamicpb.Message) protoreflect.Value {
	return protoreflect.ValueOfMessage(m)
}

func writeJSON(w http.ResponseWriter, code int, msg *dynamicpb.Message) {
	b, err := protojson.Marshal(msg)
	if err != nil {
		http.Error(w, "marshal response: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(b)
}

func httpError(w http.ResponseWriter, code codes.Code, msg string) {
	httpCode := http.StatusInternalServerError
	switch code {
	case codes.InvalidArgument:
		httpCode = http.StatusBadRequest
	case codes.NotFound:
		httpCode = http.StatusNotFound
	case codes.AlreadyExists:
		httpCode = http.StatusConflict
	}
	http.Error(w, msg, httpCode)
}

func grpcHTTPError(w http.ResponseWriter, err error) {
	st, ok := status.FromError(err)
	if !ok {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	httpError(w, st.Code(), st.Message())
}

package managedresource

import (
	"io"
	"net/http"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// buildPlatformHTTPHandler creates the HTTP handler function for a
// platform resource service. It follows the same conn.Invoke() gRPC-
// loopback pattern as the extension HTTP handler ([buildHTTPHandler]).
func buildPlatformHTTPHandler(platSvc *RegisteredPlatformService, conn *grpc.ClientConn, prefix string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rest := strings.TrimPrefix(r.URL.Path, prefix)

		switch {
		case r.Method == http.MethodPost && (rest == "" || rest == "/"):
			handlePlatformHTTPCreate(w, r, conn, platSvc)
		case r.Method == http.MethodGet && (rest == "" || rest == "/"):
			handlePlatformHTTPList(w, r, conn, platSvc)
		case r.Method == http.MethodGet && len(rest) > 1:
			id := strings.TrimPrefix(rest, "/")
			handlePlatformHTTPGet(w, r, conn, platSvc, id)
		case r.Method == http.MethodDelete && len(rest) > 1:
			id := strings.TrimPrefix(rest, "/")
			handlePlatformHTTPDelete(w, r, conn, platSvc, id)
		default:
			http.NotFound(w, r)
		}
	}
}

func handlePlatformHTTPCreate(w http.ResponseWriter, r *http.Request, conn *grpc.ClientConn, svc *RegisteredPlatformService) {
	lower := strings.ToLower(svc.Config.Singular[:1]) + svc.Config.Singular[1:]
	id := r.URL.Query().Get(lower + "_id")
	if id == "" {
		httpError(w, codes.InvalidArgument, lower+"_id query parameter is required")
		return
	}

	createReq := dynamicpb.NewMessage(svc.Descriptors.CreateRequest)
	idField := svc.Descriptors.CreateRequest.Fields().ByNumber(1)
	createReq.Set(idField, protoreflect.ValueOfString(id))

	body, err := io.ReadAll(r.Body)
	if err != nil {
		httpError(w, codes.InvalidArgument, "read body: "+err.Error())
		return
	}
	if len(body) > 0 {
		resourceMsg := dynamicpb.NewMessage(svc.Descriptors.Resource)
		if err := protojson.Unmarshal(body, resourceMsg); err != nil {
			httpError(w, codes.InvalidArgument, "parse body: "+err.Error())
			return
		}
		resourceField := svc.Descriptors.CreateRequest.Fields().ByNumber(2)
		createReq.Set(resourceField, protoreflect.ValueOfMessage(resourceMsg))
	}

	resp := dynamicpb.NewMessage(svc.Descriptors.Resource)
	method := "/" + svc.Config.GRPCServiceName() + "/CreatePlatform" + svc.Config.Singular
	if err := conn.Invoke(grpcContext(r), method, createReq, resp); err != nil {
		grpcHTTPError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, resp)
}

func handlePlatformHTTPGet(w http.ResponseWriter, r *http.Request, conn *grpc.ClientConn, svc *RegisteredPlatformService, id string) {
	getReq := dynamicpb.NewMessage(svc.Descriptors.GetRequest)
	nameField := svc.Descriptors.GetRequest.Fields().ByName("name")
	getReq.Set(nameField, protoreflect.ValueOfString(svc.Config.Collection()+id))

	resp := dynamicpb.NewMessage(svc.Descriptors.Resource)
	method := "/" + svc.Config.GRPCServiceName() + "/GetPlatform" + svc.Config.Singular
	if err := conn.Invoke(grpcContext(r), method, getReq, resp); err != nil {
		grpcHTTPError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, resp)
}

func handlePlatformHTTPList(w http.ResponseWriter, r *http.Request, conn *grpc.ClientConn, svc *RegisteredPlatformService) {
	listReq := dynamicpb.NewMessage(svc.Descriptors.ListRequest)

	resp := dynamicpb.NewMessage(svc.Descriptors.ListResponse)
	method := "/" + svc.Config.GRPCServiceName() + "/ListPlatform" + svc.Config.Plural
	if err := conn.Invoke(grpcContext(r), method, listReq, resp); err != nil {
		grpcHTTPError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, resp)
}

func handlePlatformHTTPDelete(w http.ResponseWriter, r *http.Request, conn *grpc.ClientConn, svc *RegisteredPlatformService, id string) {
	deleteReq := dynamicpb.NewMessage(svc.Descriptors.DeleteRequest)
	nameField := svc.Descriptors.DeleteRequest.Fields().ByName("name")
	deleteReq.Set(nameField, protoreflect.ValueOfString(svc.Config.Collection()+id))

	resp := dynamicpb.NewMessage(svc.Descriptors.Resource)
	method := "/" + svc.Config.GRPCServiceName() + "/DeletePlatform" + svc.Config.Singular
	if err := conn.Invoke(grpcContext(r), method, deleteReq, resp); err != nil {
		grpcHTTPError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, resp)
}

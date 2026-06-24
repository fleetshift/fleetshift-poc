package platformresource

import (
	"io"
	"net/http"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/dynamicapi"
)

// BuildHTTPHandler creates the HTTP handler function for a platform
// resource service. It follows the same conn.Invoke() gRPC-loopback
// pattern as the extension HTTP handler.
func BuildHTTPHandler(platSvc *RegisteredService, conn *grpc.ClientConn, prefix string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rest := strings.TrimPrefix(r.URL.Path, prefix)

		switch {
		case r.Method == http.MethodPost && (rest == "" || rest == "/"):
			handleHTTPCreate(w, r, conn, platSvc)
		case r.Method == http.MethodGet && (rest == "" || rest == "/"):
			handleHTTPList(w, r, conn, platSvc)
		case r.Method == http.MethodGet && len(rest) > 1:
			id := strings.TrimPrefix(rest, "/")
			handleHTTPGet(w, r, conn, platSvc, id)
		default:
			http.NotFound(w, r)
		}
	}
}

func handleHTTPCreate(w http.ResponseWriter, r *http.Request, conn *grpc.ClientConn, svc *RegisteredService) {
	lower := strings.ToLower(svc.Config.Singular[:1]) + svc.Config.Singular[1:]
	id := r.URL.Query().Get(lower + "_id")
	if id == "" {
		dynamicapi.HTTPError(w, codes.InvalidArgument, lower+"_id query parameter is required")
		return
	}

	createReq := dynamicpb.NewMessage(svc.Descriptors.CreateRequest)
	idField := svc.Descriptors.CreateRequest.Fields().ByNumber(1)
	createReq.Set(idField, protoreflect.ValueOfString(id))

	body, err := io.ReadAll(r.Body)
	if err != nil {
		dynamicapi.HTTPError(w, codes.InvalidArgument, "read body: "+err.Error())
		return
	}
	if len(body) > 0 {
		resourceMsg := dynamicpb.NewMessage(svc.Descriptors.Resource)
		if err := protojson.Unmarshal(body, resourceMsg); err != nil {
			dynamicapi.HTTPError(w, codes.InvalidArgument, "parse body: "+err.Error())
			return
		}
		resourceField := svc.Descriptors.CreateRequest.Fields().ByNumber(2)
		createReq.Set(resourceField, protoreflect.ValueOfMessage(resourceMsg))
	}

	resp := dynamicpb.NewMessage(svc.Descriptors.Resource)
	method := "/" + svc.Config.GRPCServiceName() + "/CreatePlatform" + svc.Config.Singular
	if err := conn.Invoke(dynamicapi.GRPCContext(r), method, createReq, resp); err != nil {
		dynamicapi.GRPCHTTPError(w, err)
		return
	}

	dynamicapi.WriteJSON(w, http.StatusOK, resp)
}

func handleHTTPGet(w http.ResponseWriter, r *http.Request, conn *grpc.ClientConn, svc *RegisteredService, id string) {
	getReq := dynamicpb.NewMessage(svc.Descriptors.GetRequest)
	nameField := svc.Descriptors.GetRequest.Fields().ByName("name")
	getReq.Set(nameField, protoreflect.ValueOfString(svc.Config.Collection()+id))

	resp := dynamicpb.NewMessage(svc.Descriptors.Resource)
	method := "/" + svc.Config.GRPCServiceName() + "/GetPlatform" + svc.Config.Singular
	if err := conn.Invoke(dynamicapi.GRPCContext(r), method, getReq, resp); err != nil {
		dynamicapi.GRPCHTTPError(w, err)
		return
	}

	dynamicapi.WriteJSON(w, http.StatusOK, resp)
}

func handleHTTPList(w http.ResponseWriter, r *http.Request, conn *grpc.ClientConn, svc *RegisteredService) {
	listReq := dynamicpb.NewMessage(svc.Descriptors.ListRequest)

	resp := dynamicpb.NewMessage(svc.Descriptors.ListResponse)
	method := "/" + svc.Config.GRPCServiceName() + "/ListPlatform" + svc.Config.Plural
	if err := conn.Invoke(dynamicapi.GRPCContext(r), method, listReq, resp); err != nil {
		dynamicapi.GRPCHTTPError(w, err)
		return
	}

	dynamicapi.WriteJSON(w, http.StatusOK, resp)
}

package managedresource

import (
	"io"
	"net/http"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
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
func RegisterHTTP(mux *http.ServeMux, svc *RegisteredService, grpcAddr string) error {
	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}

	prefix := "/v1/" + svc.Config.Plural

	mux.HandleFunc(prefix, func(w http.ResponseWriter, r *http.Request) {
		// Route: POST /v1/clusters -> Create
		// Route: GET  /v1/clusters -> List
		rest := strings.TrimPrefix(r.URL.Path, prefix)

		switch {
		case r.Method == http.MethodPost && (rest == "" || rest == "/"):
			handleHTTPCreate(w, r, conn, svc)
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
	})

	return nil
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

	// Build the create request.
	createReq := dynamicpb.NewMessage(svc.Descriptors.CreateRequest)
	idField := svc.Descriptors.CreateRequest.Fields().ByNumber(1)
	resourceField := svc.Descriptors.CreateRequest.Fields().ByNumber(2)
	createReq.Set(idField, resource.NewField(idField))
	createReq.Set(idField, resource.ProtoReflect().NewField(idField))

	// Actually set fields correctly
	createReq = dynamicpb.NewMessage(svc.Descriptors.CreateRequest)
	createReq.Set(svc.Descriptors.CreateRequest.Fields().ByNumber(1),
		resource.ProtoReflect().NewField(svc.Descriptors.CreateRequest.Fields().ByNumber(1)))
	// Simpler: just set string and message directly
	createReq2 := dynamicpb.NewMessage(svc.Descriptors.CreateRequest)
	createReq2.Set(idField, stringValue(id))
	createReq2.Set(resourceField, messageValue(resource))

	resp := dynamicpb.NewMessage(svc.Descriptors.Resource)
	method := "/" + svc.Config.ServiceName() + "/Create" + svc.Config.Singular
	if err := conn.Invoke(r.Context(), method, createReq2, resp); err != nil {
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
	if err := conn.Invoke(r.Context(), method, getReq, resp); err != nil {
		grpcHTTPError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, resp)
}

func handleHTTPList(w http.ResponseWriter, r *http.Request, conn *grpc.ClientConn, svc *RegisteredService) {
	listReq := dynamicpb.NewMessage(svc.Descriptors.ListRequest)

	resp := dynamicpb.NewMessage(svc.Descriptors.ListResponse)
	titlePlural := strings.ToUpper(svc.Config.Plural[:1]) + svc.Config.Plural[1:]
	method := "/" + svc.Config.ServiceName() + "/List" + titlePlural
	if err := conn.Invoke(r.Context(), method, listReq, resp); err != nil {
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
	if err := conn.Invoke(r.Context(), method, deleteReq, resp); err != nil {
		grpcHTTPError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, resp)
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

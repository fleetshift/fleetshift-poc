package dynamicapi

import (
	"context"
	"errors"
	"net/http"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// ToStatusError maps well-known domain errors to gRPC status codes.
// It also preserves context cancellation/deadline semantics and passes
// through errors that are already gRPC status errors.
func ToStatusError(err error) error {
	switch {
	case errors.Is(err, domain.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, domain.ErrAlreadyExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, domain.ErrStaleGeneration):
		return status.Error(codes.Aborted, err.Error())
	case errors.Is(err, domain.ErrInvalidArgument):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	}
	if st, ok := status.FromError(err); ok {
		return st.Err()
	}
	return status.Error(codes.Internal, "internal error")
}

// GRPCContext returns a context that forwards the HTTP Authorization
// header as outgoing gRPC metadata so that the server-side authn
// interceptor can authenticate the caller.
func GRPCContext(r *http.Request) context.Context {
	auth := r.Header.Get("Authorization")
	if auth == "" {
		return r.Context()
	}
	return metadata.AppendToOutgoingContext(r.Context(), "authorization", auth)
}

// WriteJSON marshals a dynamic proto message as JSON and writes it to
// the HTTP response.
func WriteJSON(w http.ResponseWriter, code int, msg *dynamicpb.Message) {
	b, err := protojson.Marshal(msg)
	if err != nil {
		http.Error(w, "marshal response: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(b)
}

// HTTPError writes an HTTP error response using a gRPC status code for
// code mapping.
func HTTPError(w http.ResponseWriter, code codes.Code, msg string) {
	httpCode := http.StatusInternalServerError
	switch code {
	case codes.InvalidArgument:
		httpCode = http.StatusBadRequest
	case codes.NotFound:
		httpCode = http.StatusNotFound
	case codes.AlreadyExists, codes.Aborted:
		httpCode = http.StatusConflict
	case codes.PermissionDenied:
		httpCode = http.StatusForbidden
	}
	http.Error(w, msg, httpCode)
}

// GRPCHTTPError extracts a gRPC status from err and writes the
// corresponding HTTP error.
func GRPCHTTPError(w http.ResponseWriter, err error) {
	st, ok := status.FromError(err)
	if !ok {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	HTTPError(w, st.Code(), st.Message())
}

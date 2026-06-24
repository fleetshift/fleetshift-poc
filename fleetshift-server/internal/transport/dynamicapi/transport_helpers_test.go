package dynamicapi

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func TestHTTPError_AbortedMapsToConflict(t *testing.T) {
	w := httptest.NewRecorder()
	HTTPError(w, codes.Aborted, "stale generation")

	if w.Code != http.StatusConflict {
		t.Errorf("codes.Aborted: got HTTP %d, want %d", w.Code, http.StatusConflict)
	}
}

func TestHTTPError_AllCodes(t *testing.T) {
	tests := []struct {
		code     codes.Code
		wantHTTP int
	}{
		{codes.InvalidArgument, http.StatusBadRequest},
		{codes.NotFound, http.StatusNotFound},
		{codes.AlreadyExists, http.StatusConflict},
		{codes.Aborted, http.StatusConflict},
		// codes.OK is not a real error path; asserts the defensive default for unhandled codes.
		{codes.OK, http.StatusInternalServerError},
		{codes.PermissionDenied, http.StatusForbidden},
	}

	for _, tt := range tests {
		t.Run(tt.code.String(), func(t *testing.T) {
			w := httptest.NewRecorder()
			HTTPError(w, tt.code, "msg")
			if w.Code != tt.wantHTTP {
				t.Errorf("got %d, want %d", w.Code, tt.wantHTTP)
			}
		})
	}
}

func TestToStatusError_ContextCanceled(t *testing.T) {
	err := ToStatusError(context.Canceled)
	st, ok := status.FromError(err)
	if !ok {
		t.Fatal("expected gRPC status error")
	}
	if st.Code() != codes.Canceled {
		t.Errorf("got %v, want Canceled", st.Code())
	}
}

func TestToStatusError_ContextDeadlineExceeded(t *testing.T) {
	err := ToStatusError(context.DeadlineExceeded)
	st, ok := status.FromError(err)
	if !ok {
		t.Fatal("expected gRPC status error")
	}
	if st.Code() != codes.DeadlineExceeded {
		t.Errorf("got %v, want DeadlineExceeded", st.Code())
	}
}

func TestToStatusError_WrappedContextErrors(t *testing.T) {
	wrapped := fmt.Errorf("db query: %w", context.DeadlineExceeded)
	err := ToStatusError(wrapped)
	st, ok := status.FromError(err)
	if !ok {
		t.Fatal("expected gRPC status error")
	}
	if st.Code() != codes.DeadlineExceeded {
		t.Errorf("got %v, want DeadlineExceeded", st.Code())
	}
}

func TestToStatusError_ExistingStatusError(t *testing.T) {
	orig := status.Error(codes.Unauthenticated, "bad token")
	err := ToStatusError(orig)
	st, ok := status.FromError(err)
	if !ok {
		t.Fatal("expected gRPC status error")
	}
	if st.Code() != codes.Unauthenticated {
		t.Errorf("got %v, want Unauthenticated", st.Code())
	}
	if st.Message() != "bad token" {
		t.Errorf("got message %q, want %q", st.Message(), "bad token")
	}
}

func TestToStatusError_DomainErrors(t *testing.T) {
	tests := []struct {
		err      error
		wantCode codes.Code
	}{
		{domain.ErrNotFound, codes.NotFound},
		{domain.ErrAlreadyExists, codes.AlreadyExists},
		{domain.ErrStaleGeneration, codes.Aborted},
		{domain.ErrInvalidArgument, codes.InvalidArgument},
		{errors.New("unknown"), codes.Internal},
	}

	for _, tt := range tests {
		t.Run(tt.err.Error(), func(t *testing.T) {
			err := ToStatusError(tt.err)
			st, ok := status.FromError(err)
			if !ok {
				t.Fatal("expected gRPC status error")
			}
			if st.Code() != tt.wantCode {
				t.Errorf("got %v, want %v", st.Code(), tt.wantCode)
			}
		})
	}
}

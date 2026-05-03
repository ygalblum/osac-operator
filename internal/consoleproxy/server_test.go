package consoleproxy

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAPIMux(t *testing.T) {
	s := &Server{
		logger:         discardLogger,
		configResolver: fakeConfigResolver{err: errors.New("no backend")},
	}
	mux := s.newAPIMux()

	tests := []struct {
		name     string
		method   string
		path     string
		wantCode int
		wantKind string
	}{
		{
			name:     "routes api group list",
			method:   http.MethodGet,
			path:     "/apis",
			wantCode: http.StatusOK,
			wantKind: "APIGroupList",
		},
		{
			name:     "routes api group",
			method:   http.MethodGet,
			path:     "/apis/" + apiGroup,
			wantCode: http.StatusOK,
			wantKind: "APIGroup",
		},
		{
			name:     "routes api resource list",
			method:   http.MethodGet,
			path:     "/apis/" + apiGroup + "/" + apiVersion,
			wantCode: http.StatusOK,
			wantKind: "APIResourceList",
		},
		{
			name:   "console path routes to handler",
			method: http.MethodGet,
			path:   "/apis/" + apiGroup + "/" + apiVersion + "/namespaces/ns/computeinstances/ci/console",
			// Handler is invoked but will fail because Server has no hubClient/configResolver.
			// The important thing is it does NOT return 404 (route matched).
		},
		{
			name:     "unknown path returns 404",
			method:   http.MethodGet,
			path:     "/apis/unknown.group/v1",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "wrong method returns 405",
			method:   http.MethodPost,
			path:     "/apis",
			wantCode: http.StatusMethodNotAllowed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(tt.method, tt.path, nil)
			req.Header.Set("Accept", "application/json")

			mux.ServeHTTP(rec, req)

			if tt.wantCode != 0 && rec.Code != tt.wantCode {
				t.Fatalf("status code = %d, want %d", rec.Code, tt.wantCode)
			}

			// For the console path, just verify it didn't 404 (route matched).
			if tt.name == "console path routes to handler" && rec.Code == http.StatusNotFound {
				t.Fatal("console path returned 404, expected handler to be invoked")
			}

			if tt.wantKind != "" {
				var resp struct {
					Kind string `json:"kind"`
				}
				if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
					t.Fatalf("failed to decode response: %v", err)
				}
				if resp.Kind != tt.wantKind {
					t.Fatalf("kind = %q, want %q", resp.Kind, tt.wantKind)
				}
			}
		})
	}
}

package consoleproxy

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestProbeMux(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		prepare func(*probeState)
		want    int
	}{
		{
			name: "healthz is always healthy",
			path: "/healthz",
			want: http.StatusOK,
		},
		{
			name: "livez is always healthy",
			path: "/livez",
			want: http.StatusOK,
		},
		{
			name: "readyz is unavailable before ready",
			path: "/readyz",
			want: http.StatusServiceUnavailable,
		},
		{
			name: "readyz is healthy after ready",
			path: "/readyz",
			prepare: func(probes *probeState) {
				probes.MarkReady()
			},
			want: http.StatusOK,
		},
		{
			name: "readyz is unavailable while shutting down",
			path: "/readyz",
			prepare: func(probes *probeState) {
				probes.MarkReady()
				probes.MarkShuttingDown()
			},
			want: http.StatusServiceUnavailable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			probes := newProbeState()
			if tt.prepare != nil {
				tt.prepare(probes)
			}

			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)

			newProbeMux(probes).ServeHTTP(rec, req)

			if rec.Code != tt.want {
				t.Fatalf("%s status code = %d, want %d", tt.path, rec.Code, tt.want)
			}
		})
	}
}

func TestAPIMuxDoesNotServeProbeRoutes(t *testing.T) {
	handler := (&Server{}).newAPIMux()

	for _, path := range []string{"/healthz", "/livez", "/readyz"} {
		t.Run(path, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, path, nil)

			handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusNotFound {
				t.Fatalf("status code = %d, want %d", rec.Code, http.StatusNotFound)
			}
		})
	}
}

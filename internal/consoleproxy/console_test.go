package consoleproxy

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	osacv1alpha1 "github.com/osac-project/osac-operator/api/v1alpha1"
)

func TestConsoleURLFromConfig(t *testing.T) {
	tests := []struct {
		name      string
		host      string
		namespace string
		vmName    string
		wantURL   string
		wantErr   string
	}{
		{
			name:      "preserves API path prefix",
			host:      "https://gateway.example/cluster-a",
			namespace: "tenant-a",
			vmName:    "vm-a",
			wantURL:   "wss://gateway.example/cluster-a/apis/subresources.kubevirt.io/v1/namespaces/tenant-a/virtualmachineinstances/vm-a/console",
		},
		{
			name:      "converts http to ws",
			host:      "http://gateway.example/base/",
			namespace: "tenant-a",
			vmName:    "vm-a",
			wantURL:   "ws://gateway.example/base/apis/subresources.kubevirt.io/v1/namespaces/tenant-a/virtualmachineinstances/vm-a/console",
		},
		{
			name:      "rejects missing scheme",
			host:      "gateway.example/cluster-a",
			namespace: "tenant-a",
			vmName:    "vm-a",
			wantErr:   `unsupported remote host protocol ""`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, err := consoleURLFromConfig(&rest.Config{Host: tt.host}, tt.namespace, tt.vmName)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if err.Error() != tt.wantErr {
					t.Fatalf("error = %q, want %q", err.Error(), tt.wantErr)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got := u.String(); got != tt.wantURL {
				t.Fatalf("url = %q, want %q", got, tt.wantURL)
			}
		})
	}
}

func TestHandleConsole_Errors(t *testing.T) {
	tests := []struct {
		name     string
		objects  []client.Object
		resolver func(t *testing.T) (ConfigResolver, func())
		wantCode int
	}{
		{
			name: "resolver failure returns service unavailable",
			resolver: func(t *testing.T) (ConfigResolver, func()) {
				return fakeConfigResolver{err: errors.New("bad kubeconfig")}, func() {}
			},
			wantCode: http.StatusServiceUnavailable,
		},
		{
			name: "missing compute instance returns not found",
			resolver: func(t *testing.T) (ConfigResolver, func()) {
				return fakeConfigResolver{
					config: &rest.Config{Host: "https://fake:6443"},
					source: "test",
				}, func() {}
			},
			wantCode: http.StatusNotFound,
		},
		{
			name: "missing VM reference returns service unavailable",
			objects: []client.Object{
				&osacv1alpha1.ComputeInstance{
					ObjectMeta: metav1.ObjectMeta{Name: "vm-a", Namespace: "tenant-a"},
				},
			},
			resolver: func(t *testing.T) (ConfigResolver, func()) {
				return fakeConfigResolver{
					config: &rest.Config{Host: "https://fake:6443"},
					source: "test",
				}, func() {}
			},
			wantCode: http.StatusServiceUnavailable,
		},
		{
			name:    "dial failure returns service unavailable",
			objects: []client.Object{newComputeInstance("tenant-a", "vm-a", "vm-ns", "kubevirt-vm")},
			resolver: func(t *testing.T) (ConfigResolver, func()) {
				return fakeConfigResolver{
					config: closedPortConfig(t),
					source: "test",
				}, func() {}
			},
			wantCode: http.StatusServiceUnavailable,
		},
		{
			name:    "upstream error forwarded",
			objects: []client.Object{newComputeInstance("tenant-a", "vm-a", "vm-ns", "kubevirt-vm")},
			resolver: func(t *testing.T) (ConfigResolver, func()) {
				upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusConflict)
					_, _ = w.Write([]byte(`{"message":"vm busy"}`))
				}))
				return fakeConfigResolver{config: configForTLSServer(t, upstream), source: "test"}, upstream.Close
			},
			wantCode: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resolver, cleanup := tt.resolver(t)
			defer cleanup()
			server := newTestServer(t, tt.objects, resolver)

			req := httptest.NewRequest(http.MethodGet, consolePath, nil)
			req.SetPathValue("namespace", "tenant-a")
			req.SetPathValue("name", "vm-a")

			rec := httptest.NewRecorder()
			server.handleConsole(rec, req)

			if rec.Code != tt.wantCode {
				t.Fatalf("status code = %d, want %d", rec.Code, tt.wantCode)
			}
		})
	}
}

func TestResolveVMReference(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = osacv1alpha1.AddToScheme(scheme)

	tests := []struct {
		name      string
		objects   []client.Object
		wantCode  int
		wantNS    string
		wantName  string
		wantCheck func(*testing.T, metav1.Status)
	}{
		{
			name:     "missing compute instance returns not found",
			wantCode: http.StatusNotFound,
			wantCheck: func(t *testing.T, status metav1.Status) {
				if status.Reason != metav1.StatusReasonNotFound {
					t.Fatalf("reason = %q, want %q", status.Reason, metav1.StatusReasonNotFound)
				}
			},
		},
		{
			name: "missing VM reference returns service unavailable",
			objects: []client.Object{
				&osacv1alpha1.ComputeInstance{
					ObjectMeta: metav1.ObjectMeta{Name: "vm-a", Namespace: "tenant-a"},
				},
			},
			wantCode: http.StatusServiceUnavailable,
			wantCheck: func(t *testing.T, status metav1.Status) {
				if status.Reason != metav1.StatusReasonServiceUnavailable {
					t.Fatalf("reason = %q, want %q", status.Reason, metav1.StatusReasonServiceUnavailable)
				}
				if !strings.Contains(status.Message, "virtual machine reference is not available yet") {
					t.Fatalf("message = %q, want VM reference unavailable", status.Message)
				}
			},
		},
		{
			name: "incomplete VM reference returns service unavailable",
			objects: []client.Object{
				&osacv1alpha1.ComputeInstance{
					ObjectMeta: metav1.ObjectMeta{Name: "vm-a", Namespace: "tenant-a"},
					Status: osacv1alpha1.ComputeInstanceStatus{
						VirtualMachineReference: &osacv1alpha1.VirtualMachineReferenceType{
							Namespace: "tenant-a",
						},
					},
				},
			},
			wantCode: http.StatusServiceUnavailable,
			wantCheck: func(t *testing.T, status metav1.Status) {
				if status.Reason != metav1.StatusReasonServiceUnavailable {
					t.Fatalf("reason = %q, want %q", status.Reason, metav1.StatusReasonServiceUnavailable)
				}
				if !strings.Contains(status.Message, "virtual machine reference is incomplete") {
					t.Fatalf("message = %q, want incomplete VM reference unavailable", status.Message)
				}
			},
		},
		{
			name:     "returns resolved VM reference",
			objects:  []client.Object{newComputeInstance("tenant-a", "vm-a", "vm-ns", "kubevirt-vm")},
			wantNS:   "vm-ns",
			wantName: "kubevirt-vm",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := &Server{
				hubClient: fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(tt.objects...).
					Build(),
			}

			vmNS, vmName, err := server.resolveVMReference(context.Background(), "tenant-a", "vm-a")

			if tt.wantNS != "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if vmNS != tt.wantNS {
					t.Fatalf("vmNamespace = %q, want %q", vmNS, tt.wantNS)
				}
				if vmName != tt.wantName {
					t.Fatalf("vmName = %q, want %q", vmName, tt.wantName)
				}
				return
			}

			if err == nil {
				t.Fatal("expected error, got nil")
			}
			status := statusFromError(t, err)
			if int(status.Code) != tt.wantCode {
				t.Fatalf("code = %d, want %d", status.Code, tt.wantCode)
			}
			tt.wantCheck(t, status)
		})
	}
}

func TestForwardUpstreamResponse(t *testing.T) {
	tests := []struct {
		name            string
		statusCode      int
		contentType     string
		body            string
		wantCode        int
		wantContentType string
		wantBody        string
	}{
		{
			name:            "forwards JSON status body",
			statusCode:      http.StatusConflict,
			contentType:     "application/json",
			body:            `{"kind":"Status","apiVersion":"v1","status":"Failure","message":"virtual machine instance is not ready","reason":"Conflict","code":409}`,
			wantCode:        http.StatusConflict,
			wantContentType: "application/json",
			wantBody:        `{"kind":"Status","apiVersion":"v1","status":"Failure","message":"virtual machine instance is not ready","reason":"Conflict","code":409}`,
		},
		{
			name:            "forwards plain text body",
			statusCode:      http.StatusServiceUnavailable,
			contentType:     "text/plain",
			body:            "Active VNC connection. Request denied.",
			wantCode:        http.StatusServiceUnavailable,
			wantContentType: "text/plain",
			wantBody:        "Active VNC connection. Request denied.",
		},
		{
			name:       "handles missing content type",
			statusCode: http.StatusBadGateway,
			body:       "bad gateway",
			wantCode:   http.StatusBadGateway,
			wantBody:   "bad gateway",
		},
		{
			name:       "handles nil body",
			statusCode: http.StatusInternalServerError,
			wantCode:   http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := &http.Response{
				StatusCode: tt.statusCode,
				Header:     http.Header{},
			}
			if tt.contentType != "" {
				resp.Header.Set("Content-Type", tt.contentType)
			}
			if tt.body != "" {
				resp.Body = io.NopCloser(strings.NewReader(tt.body))
			}

			rec := httptest.NewRecorder()
			forwardUpstreamResponse(rec, resp)

			if rec.Code != tt.wantCode {
				t.Fatalf("status code = %d, want %d", rec.Code, tt.wantCode)
			}
			if tt.wantContentType != "" {
				if got := rec.Header().Get("Content-Type"); got != tt.wantContentType {
					t.Fatalf("Content-Type = %q, want %q", got, tt.wantContentType)
				}
			}
			if got := rec.Body.String(); got != tt.wantBody {
				t.Fatalf("body = %q, want %q", got, tt.wantBody)
			}
		})
	}
}

func TestNewConsoleConnectStatusError(t *testing.T) {
	resp := &http.Response{
		StatusCode: http.StatusConflict,
		Status:     "409 Conflict",
	}

	tests := []struct {
		name  string
		resp  *http.Response
		check func(*testing.T, error)
	}{
		{
			name: "returns upstream error when response is non-nil",
			resp: resp,
			check: func(t *testing.T, err error) {
				var ue *upstreamError
				if !errors.As(err, &ue) {
					t.Fatalf("expected *upstreamError, got %T", err)
				}
				if ue.resp != resp {
					t.Fatal("upstreamError should wrap the original response")
				}
			},
		},
		{
			name: "returns service unavailable when response is nil",
			check: func(t *testing.T, err error) {
				if !apierrors.IsServiceUnavailable(err) {
					t.Fatalf("expected ServiceUnavailable, got %T: %v", err, err)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := newConsoleConnectStatusError(errors.New("dial failed"), tt.resp)
			tt.check(t, err)
		})
	}
}

func TestDialConsole(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(t *testing.T) (*rest.Config, func())
		wantErr bool
	}{
		{
			name: "success",
			setup: func(t *testing.T) (*rest.Config, func()) {
				ts, cfg := startWSEchoServer(t)
				return cfg, ts.Close
			},
		},
		{
			name: "invalid host",
			setup: func(t *testing.T) (*rest.Config, func()) {
				return &rest.Config{Host: "://bad"}, func() {}
			},
			wantErr: true,
		},
		{
			name: "connection refused",
			setup: func(t *testing.T) (*rest.Config, func()) {
				return closedPortConfig(t), func() {}
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, cleanup := tt.setup(t)
			defer cleanup()

			server := &Server{logger: discardLogger}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			conn, err := server.dialConsole(ctx, cfg, "test-ns", "test-vm")
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			_ = conn.CloseNow()
		})
	}
}

func TestHandleConsole_SuccessfulWebSocketProxy(t *testing.T) {
	echoServer, echoCfg := startWSEchoServer(t)
	defer echoServer.Close()

	ci := newComputeInstance("tenant-a", "vm-a", "test-ns", "test-vm")
	server := newTestServer(t, []client.Object{ci}, fakeConfigResolver{config: echoCfg, source: "test"})

	httpServer := httptest.NewServer(server.newAPIMux())
	defer httpServer.Close()

	consoleURL := strings.Replace(httpServer.URL, "http://", "ws://", 1) +
		"/apis/" + apiGroup + "/" + apiVersion + "/namespaces/tenant-a/computeinstances/vm-a/console"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, consoleURL, nil)
	if err != nil {
		t.Fatalf("failed to dial console: %v", err)
	}
	defer func() { _ = conn.CloseNow() }()

	testMessage := []byte("hello console")
	if err := conn.Write(ctx, websocket.MessageBinary, testMessage); err != nil {
		t.Fatalf("failed to write message: %v", err)
	}

	msgType, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("failed to read echo: %v", err)
	}
	if msgType != websocket.MessageBinary {
		t.Fatalf("message type = %v, want Binary", msgType)
	}
	if string(data) != string(testMessage) {
		t.Fatalf("echo = %q, want %q", data, testMessage)
	}

	_ = conn.Close(websocket.StatusNormalClosure, "")
}

// --- Test helpers ---

type fakeConfigResolver struct {
	config *rest.Config
	source string
	err    error
}

func (f fakeConfigResolver) ResolveConfig(_ context.Context, _ string) (*rest.Config, string, error) {
	return f.config, f.source, f.err
}

func newTestServer(t *testing.T, objects []client.Object, resolver ConfigResolver) *Server {
	t.Helper()

	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = osacv1alpha1.AddToScheme(scheme)

	builder := fake.NewClientBuilder().WithScheme(scheme)
	if len(objects) > 0 {
		builder = builder.WithObjects(objects...)
	}

	return &Server{
		logger:         discardLogger,
		hubClient:      builder.Build(),
		configResolver: resolver,
		probes:         newProbeState(),
	}
}

func newComputeInstance(namespace, name, vmNS, vmName string) *osacv1alpha1.ComputeInstance {
	return &osacv1alpha1.ComputeInstance{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Status: osacv1alpha1.ComputeInstanceStatus{
			VirtualMachineReference: &osacv1alpha1.VirtualMachineReferenceType{
				Namespace:                  vmNS,
				KubeVirtVirtualMachineName: vmName,
			},
		},
	}
}

func startWSEchoServer(t *testing.T) (*httptest.Server, *rest.Config) {
	t.Helper()

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			InsecureSkipVerify: true,
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer func() { _ = conn.CloseNow() }()

		for {
			msgType, data, err := conn.Read(r.Context())
			if err != nil {
				return
			}
			if err := conn.Write(r.Context(), msgType, data); err != nil {
				return
			}
		}
	})

	ts := httptest.NewTLSServer(mux)
	cfg := configForTLSServer(t, ts)
	return ts, cfg
}

func configForTLSServer(t *testing.T, ts *httptest.Server) *rest.Config {
	t.Helper()

	cert := ts.TLS.Certificates[0]
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatalf("failed to parse test server certificate: %v", err)
	}
	caData := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leaf.Raw})

	return &rest.Config{
		Host: ts.URL,
		TLSClientConfig: rest.TLSClientConfig{
			CAData: caData,
		},
	}
}

func closedPortConfig(t *testing.T) *rest.Config {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()

	return &rest.Config{
		Host:            fmt.Sprintf("https://127.0.0.1:%d", port),
		TLSClientConfig: rest.TLSClientConfig{Insecure: true},
	}
}

func statusFromError(t *testing.T, err error) metav1.Status {
	t.Helper()

	statusErr, ok := err.(interface{ Status() metav1.Status })
	if !ok {
		t.Fatalf("error does not expose Kubernetes status: %T", err)
	}
	return statusErr.Status()
}

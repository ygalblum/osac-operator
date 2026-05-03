package consoleproxy

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func validKubeconfig() []byte {
	return []byte(`apiVersion: v1
kind: Config
clusters:
- cluster:
    server: https://remote-cluster:6443
  name: remote
contexts:
- context:
    cluster: remote
    user: test
  name: remote
current-context: remote
users:
- name: test
  user:
    token: test-token
`)
}

func labeledSecret(namespace, name string, data map[string][]byte) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
			Labels:    map[string]string{RemoteKubeconfigLabel: "true"},
		},
		Data: data,
	}
}

var discardLogger = slog.New(slog.NewTextHandler(&discardWriter{}, nil))

type discardWriter struct{}

func (d *discardWriter) Write(p []byte) (int, error) { return len(p), nil }

func TestRemoteConfigResolver(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	tests := []struct {
		name      string
		namespace string
		objects   []runtime.Object
		wantHost  string
		wantErr   string
	}{
		{
			name:      "success",
			namespace: "osac-dev",
			objects: []runtime.Object{
				labeledSecret("osac-dev", "remote-kubeconfig", map[string][]byte{
					remoteKubeconfigKey: validKubeconfig(),
				}),
			},
			wantHost: "https://remote-cluster:6443",
		},
		{
			name:      "no labeled secret",
			namespace: "osac-dev",
			objects:   []runtime.Object{},
			wantErr:   "no remote kubeconfig secret found",
		},
		{
			name:      "secret without kubeconfig key",
			namespace: "osac-dev",
			objects: []runtime.Object{
				labeledSecret("osac-dev", "bad-secret", map[string][]byte{
					"other-key": []byte("data"),
				}),
			},
			wantErr: `has no "kubeconfig" key`,
		},
		{
			name:      "invalid kubeconfig data",
			namespace: "osac-dev",
			objects: []runtime.Object{
				labeledSecret("osac-dev", "bad-kubeconfig", map[string][]byte{
					remoteKubeconfigKey: []byte("not valid yaml {{{"),
				}),
			},
			wantErr: "parsing kubeconfig",
		},
		{
			name:      "multiple secrets picks first without error",
			namespace: "osac-dev",
			objects: []runtime.Object{
				labeledSecret("osac-dev", "first", map[string][]byte{
					remoteKubeconfigKey: validKubeconfig(),
				}),
				labeledSecret("osac-dev", "second", map[string][]byte{
					remoteKubeconfigKey: validKubeconfig(),
				}),
			},
			wantHost: "https://remote-cluster:6443",
		},
		{
			name:      "secret in wrong namespace is not found",
			namespace: "osac-dev",
			objects: []runtime.Object{
				labeledSecret("other-namespace", "remote-kubeconfig", map[string][]byte{
					remoteKubeconfigKey: validKubeconfig(),
				}),
			},
			wantErr: "no remote kubeconfig secret found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := fake.NewClientBuilder().
				WithScheme(scheme).
				WithRuntimeObjects(tt.objects...).
				Build()

			resolver := NewRemoteConfigResolver(c, discardLogger)
			config, _, err := resolver.ResolveConfig(context.Background(), tt.namespace)

			if tt.wantErr != "" {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error %q does not contain %q", err.Error(), tt.wantErr)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if config.Host != tt.wantHost {
				t.Fatalf("host = %q, want %q", config.Host, tt.wantHost)
			}
		})
	}
}

func TestRemoteConfigResolver_ReturnsSourceWithSecretName(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithRuntimeObjects(labeledSecret("ns", "my-remote-kc", map[string][]byte{
			remoteKubeconfigKey: validKubeconfig(),
		})).
		Build()

	resolver := NewRemoteConfigResolver(c, discardLogger)
	_, source, err := resolver.ResolveConfig(context.Background(), "ns")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(source, "my-remote-kc") {
		t.Fatalf("source %q should contain secret name", source)
	}
}

func TestLocalConfigResolver(t *testing.T) {
	original := &rest.Config{Host: "https://local-cluster:6443"}
	resolver := NewLocalConfigResolver(original, discardLogger)

	config, source, err := resolver.ResolveConfig(context.Background(), "any-namespace")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if config.Host != original.Host {
		t.Fatalf("host = %q, want %q", config.Host, original.Host)
	}
	if !strings.Contains(source, "local") {
		t.Fatalf("source %q should indicate local", source)
	}
	if config == original {
		t.Fatal("should return a copy, not the same pointer")
	}
}

func TestAutoConfigResolver_UsesRemoteWhenAvailable(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithRuntimeObjects(labeledSecret("ns", "remote-kc", map[string][]byte{
			remoteKubeconfigKey: validKubeconfig(),
		})).
		Build()

	remote := NewRemoteConfigResolver(c, discardLogger)
	local := NewLocalConfigResolver(&rest.Config{Host: "https://local:6443"}, discardLogger)
	resolver := NewAutoConfigResolver(remote, local, discardLogger)

	config, source, err := resolver.ResolveConfig(context.Background(), "ns")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if config.Host != "https://remote-cluster:6443" {
		t.Fatalf("expected remote host, got %q", config.Host)
	}
	if !strings.Contains(source, "remote") {
		t.Fatalf("source %q should indicate remote", source)
	}
}

func TestAutoConfigResolver_FallsBackToLocalWhenNoSecret(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		Build()

	remote := NewRemoteConfigResolver(c, discardLogger)
	local := NewLocalConfigResolver(&rest.Config{Host: "https://local:6443"}, discardLogger)
	resolver := NewAutoConfigResolver(remote, local, discardLogger)

	config, source, err := resolver.ResolveConfig(context.Background(), "ns")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if config.Host != "https://local:6443" {
		t.Fatalf("expected local host, got %q", config.Host)
	}
	if !strings.Contains(source, "local") {
		t.Fatalf("source %q should indicate local fallback", source)
	}
}

func TestAutoConfigResolver_DoesNotFallBackOnParseError(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithRuntimeObjects(labeledSecret("ns", "bad", map[string][]byte{
			remoteKubeconfigKey: []byte("not valid {{{"),
		})).
		Build()

	remote := NewRemoteConfigResolver(c, discardLogger)
	local := NewLocalConfigResolver(&rest.Config{Host: "https://local:6443"}, discardLogger)
	resolver := NewAutoConfigResolver(remote, local, discardLogger)

	_, _, err := resolver.ResolveConfig(context.Background(), "ns")
	if err == nil {
		t.Fatal("expected error for malformed kubeconfig, got nil")
	}
	if !strings.Contains(err.Error(), "no fallback") {
		t.Fatalf("error %q should mention no fallback", err.Error())
	}
}

func TestAutoConfigResolver_DoesNotFallBackOnMissingKey(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithRuntimeObjects(labeledSecret("ns", "no-key", map[string][]byte{
			"wrong-key": validKubeconfig(),
		})).
		Build()

	remote := NewRemoteConfigResolver(c, discardLogger)
	local := NewLocalConfigResolver(&rest.Config{Host: "https://local:6443"}, discardLogger)
	resolver := NewAutoConfigResolver(remote, local, discardLogger)

	_, _, err := resolver.ResolveConfig(context.Background(), "ns")
	if err == nil {
		t.Fatal("expected error for missing kubeconfig key, got nil")
	}
	if !strings.Contains(err.Error(), "no fallback") {
		t.Fatalf("error %q should mention no fallback", err.Error())
	}
}

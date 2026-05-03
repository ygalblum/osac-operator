package main

import (
	"io"
	"log/slog"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	osacv1alpha1 "github.com/osac-project/osac-operator/api/v1alpha1"
)

var discardLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

func TestBuildConfigResolver(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = osacv1alpha1.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	hubConfig := &rest.Config{Host: "https://fake:6443"}

	tests := []struct {
		name    string
		mode    string
		wantErr bool
	}{
		{
			name:    "invalid mode returns error",
			mode:    "bogus",
			wantErr: true,
		},
		{
			name: "local mode succeeds",
			mode: "local",
		},
		{
			name: "remote mode succeeds",
			mode: "remote",
		},
		{
			name: "auto mode succeeds",
			mode: "auto",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resolver, err := buildConfigResolver(tt.mode, fakeClient, hubConfig, discardLogger)

			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if resolver == nil {
				t.Fatal("resolver should not be nil")
			}
		})
	}
}

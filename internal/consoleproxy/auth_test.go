package consoleproxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	genericrequest "k8s.io/apiserver/pkg/endpoints/request"
)

const consolePath = "/apis/console.osac.openshift.io/v1alpha1/namespaces/my-ns/computeinstances/my-ci/console"

func allowAllAuthn() authenticator.Request {
	return authenticator.RequestFunc(func(_ *http.Request) (*authenticator.Response, bool, error) {
		return &authenticator.Response{
			User: &user.DefaultInfo{Name: "test-user", Groups: []string{"system:authenticated"}},
		}, true, nil
	})
}

func rejectAuthn() authenticator.Request {
	return authenticator.RequestFunc(func(_ *http.Request) (*authenticator.Response, bool, error) {
		return nil, false, nil
	})
}

func errorAuthn() authenticator.Request {
	return authenticator.RequestFunc(func(_ *http.Request) (*authenticator.Response, bool, error) {
		return nil, false, http.ErrAbortHandler
	})
}

func allowAllAuthz() authorizer.Authorizer {
	return authorizer.AuthorizerFunc(func(_ context.Context, _ authorizer.Attributes) (authorizer.Decision, string, error) {
		return authorizer.DecisionAllow, "", nil
	})
}

func denyAuthz() authorizer.Authorizer {
	return authorizer.AuthorizerFunc(func(_ context.Context, _ authorizer.Attributes) (authorizer.Decision, string, error) {
		return authorizer.DecisionDeny, "forbidden", nil
	})
}

func TestWrapWithAuthFilters(t *testing.T) {
	tests := []struct {
		name            string
		path            string
		authn           authenticator.Request
		authz           authorizer.Authorizer
		wantCode        int
		wantInnerCalled bool
		wantUser        string
	}{
		{
			name:            "authenticated and authorized",
			path:            consolePath,
			authn:           allowAllAuthn(),
			authz:           allowAllAuthz(),
			wantCode:        http.StatusOK,
			wantInnerCalled: true,
			wantUser:        "test-user",
		},
		{
			name:            "unauthenticated",
			path:            consolePath,
			authn:           rejectAuthn(),
			authz:           allowAllAuthz(),
			wantCode:        http.StatusUnauthorized,
			wantInnerCalled: false,
		},
		{
			name:            "authentication error",
			path:            consolePath,
			authn:           errorAuthn(),
			authz:           allowAllAuthz(),
			wantCode:        http.StatusUnauthorized,
			wantInnerCalled: false,
		},
		{
			name:            "authenticated but unauthorized",
			path:            consolePath,
			authn:           allowAllAuthn(),
			authz:           denyAuthz(),
			wantCode:        http.StatusForbidden,
			wantInnerCalled: false,
		},
		{
			name:            "api discovery passes through auth chain",
			path:            "/apis/console.osac.openshift.io",
			authn:           allowAllAuthn(),
			authz:           allowAllAuthz(),
			wantCode:        http.StatusOK,
			wantInnerCalled: true,
			wantUser:        "test-user",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			innerCalled := false
			var capturedUser string
			inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				innerCalled = true
				if u, ok := genericrequest.UserFrom(r.Context()); ok {
					capturedUser = u.GetName()
				}
				w.WriteHeader(http.StatusOK)
			})

			handler := wrapWithAuthFilters(inner, tt.authn, tt.authz, nil)
			req := httptest.NewRequest("GET", tt.path, nil)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != tt.wantCode {
				t.Errorf("status code = %d, want %d", rec.Code, tt.wantCode)
			}
			if innerCalled != tt.wantInnerCalled {
				t.Errorf("inner called = %v, want %v", innerCalled, tt.wantInnerCalled)
			}
			if tt.wantUser != "" && capturedUser != tt.wantUser {
				t.Errorf("user = %q, want %q", capturedUser, tt.wantUser)
			}
		})
	}
}

func TestWrapWithAuthFilters_RequestInfoParsing(t *testing.T) {
	tests := []struct {
		name            string
		path            string
		wantResourceReq bool
		wantAPIGroup    string
		wantAPIVersion  string
		wantResource    string
		wantSubresource string
		wantNamespace   string
		wantName        string
		wantVerb        string
		wantPath        string
	}{
		{
			name:            "console subresource",
			path:            consolePath,
			wantResourceReq: true,
			wantAPIGroup:    "console.osac.openshift.io",
			wantAPIVersion:  "v1alpha1",
			wantResource:    "computeinstances",
			wantSubresource: "console",
			wantNamespace:   "my-ns",
			wantName:        "my-ci",
			wantVerb:        "get",
		},
		{
			name:            "healthz is non-resource",
			path:            "/healthz",
			wantResourceReq: false,
			wantPath:        "/healthz",
		},
		{
			name:            "api group discovery",
			path:            "/apis/console.osac.openshift.io",
			wantResourceReq: false,
			wantPath:        "/apis/console.osac.openshift.io",
		},
		{
			name:            "api resource list",
			path:            "/apis/console.osac.openshift.io/v1alpha1",
			wantResourceReq: false,
			wantPath:        "/apis/console.osac.openshift.io/v1alpha1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var captured authorizer.Attributes
			capturingAuthz := authorizer.AuthorizerFunc(func(_ context.Context, a authorizer.Attributes) (authorizer.Decision, string, error) {
				captured = a
				return authorizer.DecisionAllow, "", nil
			})

			inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
			})

			handler := wrapWithAuthFilters(inner, allowAllAuthn(), capturingAuthz, nil)
			req := httptest.NewRequest("GET", tt.path, nil)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("status code = %d, want 200", rec.Code)
			}
			if captured == nil {
				t.Fatal("authorizer was not called")
			}
			if captured.IsResourceRequest() != tt.wantResourceReq {
				t.Errorf("IsResourceRequest = %v, want %v", captured.IsResourceRequest(), tt.wantResourceReq)
			}

			if tt.wantResourceReq {
				if got := captured.GetAPIGroup(); got != tt.wantAPIGroup {
					t.Errorf("APIGroup = %q, want %q", got, tt.wantAPIGroup)
				}
				if got := captured.GetAPIVersion(); got != tt.wantAPIVersion {
					t.Errorf("APIVersion = %q, want %q", got, tt.wantAPIVersion)
				}
				if got := captured.GetResource(); got != tt.wantResource {
					t.Errorf("Resource = %q, want %q", got, tt.wantResource)
				}
				if got := captured.GetSubresource(); got != tt.wantSubresource {
					t.Errorf("Subresource = %q, want %q", got, tt.wantSubresource)
				}
				if got := captured.GetNamespace(); got != tt.wantNamespace {
					t.Errorf("Namespace = %q, want %q", got, tt.wantNamespace)
				}
				if got := captured.GetName(); got != tt.wantName {
					t.Errorf("Name = %q, want %q", got, tt.wantName)
				}
				if got := captured.GetVerb(); got != tt.wantVerb {
					t.Errorf("Verb = %q, want %q", got, tt.wantVerb)
				}
			} else if tt.wantPath != "" {
				if got := captured.GetPath(); got != tt.wantPath {
					t.Errorf("Path = %q, want %q", got, tt.wantPath)
				}
			}
		})
	}
}

package consoleproxy

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestHandleAPIGroupList(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/apis", nil)
	req.Header.Set("Accept", "application/json")

	handleAPIGroupList(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", rec.Code, http.StatusOK)
	}

	var list metav1.APIGroupList
	if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if len(list.Groups) != 1 {
		t.Fatalf("groups = %d, want 1", len(list.Groups))
	}
	group := list.Groups[0]
	if group.Name != apiGroup {
		t.Fatalf("group name = %q, want %q", group.Name, apiGroup)
	}
	if group.PreferredVersion.Version != apiVersion {
		t.Fatalf("preferred version = %q, want %q", group.PreferredVersion.Version, apiVersion)
	}
}

func TestHandleAPIGroup(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/apis/"+apiGroup, nil)
	req.Header.Set("Accept", "application/json")

	handleAPIGroup(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", rec.Code, http.StatusOK)
	}

	var group metav1.APIGroup
	if err := json.Unmarshal(rec.Body.Bytes(), &group); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if group.Name != apiGroup {
		t.Fatalf("name = %q, want %q", group.Name, apiGroup)
	}
	if len(group.Versions) != 1 {
		t.Fatalf("versions = %d, want 1", len(group.Versions))
	}
	if group.Versions[0].Version != apiVersion {
		t.Fatalf("version = %q, want %q", group.Versions[0].Version, apiVersion)
	}
}

func TestHandleAPIResourceList(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/apis/"+apiGroup+"/"+apiVersion, nil)
	req.Header.Set("Accept", "application/json")

	handleAPIResourceList(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", rec.Code, http.StatusOK)
	}

	var list metav1.APIResourceList
	if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if list.GroupVersion != apiGroup+"/"+apiVersion {
		t.Fatalf("groupVersion = %q, want %q", list.GroupVersion, apiGroup+"/"+apiVersion)
	}
	if len(list.APIResources) != 1 {
		t.Fatalf("resources = %d, want 1", len(list.APIResources))
	}
	res := list.APIResources[0]
	if res.Name != "computeinstances/console" {
		t.Fatalf("resource name = %q, want %q", res.Name, "computeinstances/console")
	}
	if !res.Namespaced {
		t.Fatal("resource should be namespaced")
	}
	if len(res.Verbs) != 1 || res.Verbs[0] != "get" {
		t.Fatalf("verbs = %v, want [get]", res.Verbs)
	}
}

package proxy

import (
	"encoding/json"
	"net/url"
	"testing"
)

func TestMergeListingBodiesJSON(t *testing.T) {
	t.Parallel()

	query := url.Values{"format": []string{"json"}}
	ceph := []byte(`[{"name":"alpha"},{"name":"beta"}]`)
	swift := []byte(`[{"name":"beta"},{"name":"gamma"}]`)

	body, contentType, err := mergeListingBodies(query, ceph, swift)
	if err != nil {
		t.Fatalf("merge json listing: %v", err)
	}
	if contentType != "application/json; charset=utf-8" {
		t.Fatalf("unexpected content type: %s", contentType)
	}

	var got []map[string]any
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("decode merged json: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("unexpected merged item count: %d", len(got))
	}
}

func TestMergeListingBodiesPlain(t *testing.T) {
	t.Parallel()

	query := url.Values{}
	body, contentType, err := mergeListingBodies(query, []byte("alpha\nbeta\n"), []byte("beta\ngamma\n"))
	if err != nil {
		t.Fatalf("merge plain listing: %v", err)
	}
	if contentType != "text/plain; charset=utf-8" {
		t.Fatalf("unexpected content type: %s", contentType)
	}
	if string(body) != "alpha\nbeta\ngamma\n" {
		t.Fatalf("unexpected merged body: %q", string(body))
	}
}

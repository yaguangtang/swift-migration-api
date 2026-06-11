package proxy

import "testing"

func TestParseSwiftPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		path    string
		want    SwiftPath
		wantErr bool
	}{
		{
			name: "account",
			path: "/v1/AUTH_demo",
			want: SwiftPath{Account: "AUTH_demo", Kind: ResourceAccount, BackendPath: "/AUTH_demo"},
		},
		{
			name: "container",
			path: "/v1/AUTH_demo/images",
			want: SwiftPath{Account: "AUTH_demo", Container: "images", Kind: ResourceContainer, BackendPath: "/AUTH_demo/images"},
		},
		{
			name: "object",
			path: "/v1/AUTH_demo/images/ubuntu/latest.qcow2",
			want: SwiftPath{Account: "AUTH_demo", Container: "images", Object: "ubuntu/latest.qcow2", Kind: ResourceObject, BackendPath: "/AUTH_demo/images/ubuntu/latest.qcow2"},
		},
		{
			name:    "invalid prefix",
			path:    "/v2/AUTH_demo",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := ParseSwiftPath(tt.path)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("parse path: %v", err)
			}
			if got != tt.want {
				t.Fatalf("unexpected parsed path:\nwant: %#v\ngot:  %#v", tt.want, got)
			}
		})
	}
}

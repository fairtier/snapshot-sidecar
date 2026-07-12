package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRequireBearer(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(requireBearer("s3cret", next))
	defer srv.Close()

	tests := []struct {
		name   string
		header string
		want   int
	}{
		{"no header", "", http.StatusUnauthorized},
		{"wrong scheme", "Basic s3cret", http.StatusUnauthorized},
		{"wrong token", "Bearer nope", http.StatusUnauthorized},
		{"token prefix", "Bearer s3cre", http.StatusUnauthorized},
		{"correct", "Bearer s3cret", http.StatusOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodPost, srv.URL+"/snapshot.v1.SnapshotService/TriggerSnapshot", nil)
			if err != nil {
				t.Fatal(err)
			}
			if tt.header != "" {
				req.Header.Set("Authorization", tt.header)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != tt.want {
				t.Errorf("status = %d, want %d", resp.StatusCode, tt.want)
			}
		})
	}
}

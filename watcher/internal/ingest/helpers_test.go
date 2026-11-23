package ingest

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func newHTTPServer(t *testing.T, h http.Handler) string {
	t.Helper()
	ts := httptest.NewServer(h)
	t.Cleanup(ts.Close)
	return ts.URL
}

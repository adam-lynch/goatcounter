package title

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestTitle(t *testing.T) {
	tests := []struct {
		html, title string
	}{
		{`<!DOCTYPE html><html><head><title>Test! </title></head></html>`, `Test!`},
		{`<html><head><title>Test! </title></head></html>`, `Test!`},
		{`<title>Test! </title><body><p>ads</p>`, `Test!`},
		{`<title>&lt;p&gt;asd&amp;</title><body><p>ads</p>`, `<p>asd&`},
		{`<title>€</title><body><p>ads</p>`, `€`},
		{`<title attr="val">€</title><body><p>ads</p>`, `€`},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprint(w, tt.html)
			}))
			defer srv.Close()
			defer cache.Flush()

			title, err := Get(srv.URL)
			if err != nil {
				t.Fatal(err)
			}
			if title != tt.title {
				t.Errorf("title wrong: %q", title)
			}
		})
	}
}

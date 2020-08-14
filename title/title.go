// Package title fetches a HTML page's <title>
package title

import (
	"net/http"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	"zgo.at/errors"
	"zgo.at/zcache"
)

var (
	client = http.Client{Timeout: 5 * time.Second}
	cache  = zcache.New(12*time.Hour, 15*time.Minute)
)

// Get the text contents of a page's <title> element.
//
// Note this won't run any JavaScript; so if a title is changed in JS based on
// the URL fragment then the result will be wrong.
func Get(url string) (string, error) {
	if t, ok := cache.Get(url); ok {
		return t.(string), nil
	}

	r, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", errors.Wrap(err, "title.Get")
	}
	r.Header.Set("User-Agent", "GoatCounter/1.0 titlebot")

	resp, err := client.Do(r)
	if err != nil {
		return "", errors.Wrap(err, "title.Get")
	}
	defer resp.Body.Close()

	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return "", errors.Wrap(err, "title.Get")
	}
	title := doc.Find("head title")
	if title == nil {
		return "", nil
	}
	title = title.First()

	text := strings.TrimSpace(title.Text())
	cache.SetDefault(url, text)
	return text, nil
}

// Copyright © 2019 Martin Tournoij <martin@arp242.net>
// This file is part of GoatCounter and published under the terms of the EUPL
// v1.2, which can be found in the LICENSE file or at http://eupl12.zgo.at

package logscan

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"testing"

	"zgo.at/ztest"
)

func TestErrors(t *testing.T) {
	_, err := New(strings.NewReader(""), "log:$xxx", "", "", "")
	if !ztest.ErrorContains(err, "unknown format specifier: $xxx") {
		t.Error(err)
	}

	_, err = New(strings.NewReader(""), "xxx", "", "", "")
	if !ztest.ErrorContains(err, "unknown format: xxx") {
		t.Error(err)
	}
}

func TestNew(t *testing.T) {
	files, err := ioutil.ReadDir("./testdata")
	if err != nil {
		t.Fatal(err)
	}

	for _, f := range files {
		t.Run(f.Name(), func(t *testing.T) {
			fp, err := os.Open("./testdata/" + f.Name())
			if err != nil {
				t.Fatal(err)
			}

			scan, err := New(fp, f.Name(), "", "", "")
			if err != nil {
				t.Fatal(err)
			}

			for {
				data, err := scan.Line()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatal(err)
				}

				keys := make([]string, 0, len(data))
				for k := range data {
					keys = append(keys, k)
				}
				sort.Strings(keys)

				for _, k := range keys {
					fmt.Println("  ", k, "\t", data[k])
				}
				fmt.Println()
			}
		})
	}
}

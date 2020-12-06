// Copyright © 2019 Martin Tournoij <martin@arp242.net>
// This file is part of GoatCounter and published under the terms of the EUPL
// v1.2, which can be found in the LICENSE file or at http://eupl12.zgo.at

package goatcounter_test

import (
	"reflect"
	"strings"
	"testing"

	. "zgo.at/goatcounter"
	"zgo.at/goatcounter/gctest"
	"zgo.at/isbot"
	"zgo.at/zdb"
	"zgo.at/zstd/ztest"
)

func TestUserAgentGetOrInsert(t *testing.T) {
	ctx, clean := gctest.DB(t)
	defer clean()

	test := func(gotUA, wantUA UserAgent, want string) {
		if !reflect.DeepEqual(gotUA, wantUA) {
			t.Fatalf("wrong ua\ngot:  %#v\nwant: %#v", gotUA, wantUA)
		}

		want = strings.ReplaceAll(strings.TrimSpace(strings.ReplaceAll(want, "\t", "")), "@", " ")
		out := zdb.DumpString(ctx, `select * from view_user_agents`)
		if d := ztest.Diff(out, want); d != "" {
			t.Errorf(d)
		}
	}

	{
		ua := UserAgent{UserAgent: "Mozilla/5.0 (X11; Linux x86_64; rv:79.0) Gecko/20100101 Firefox/79.0"}
		err := ua.GetOrInsert(ctx)
		if err != nil {
			t.Fatal(err)
		}
		test(ua, UserAgent{UserAgent: ua.UserAgent, ID: 1, BrowserID: 1, SystemID: 1, Bot: isbot.NoBotNoMatch}, `
			id  bid  sid  bot  browser     system  ua
			1   1    1    1    Firefox 79  Linux   ~Z (X11; ~L x86_64; rv:79.0) ~g20100101 ~f79.0
		`)
	}

	{
		ua := UserAgent{UserAgent: "Mozilla/5.0 (X11; Linux x86_64; rv:79.0) Gecko/20100101 Firefox/79.0"}
		err := ua.GetOrInsert(ctx)
		if err != nil {
			t.Fatal(err)
		}
		test(ua, UserAgent{UserAgent: ua.UserAgent, ID: 1, BrowserID: 1, SystemID: 1, Bot: isbot.NoBotNoMatch}, `
			id  bid  sid  bot  browser     system  ua
			1   1    1    1    Firefox 79  Linux   ~Z (X11; ~L x86_64; rv:79.0) ~g20100101 ~f79.0
		`)
	}

	{
		ua := UserAgent{UserAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:79.0) Gecko/20100101 Firefox/79.0"}
		err := ua.GetOrInsert(ctx)
		if err != nil {
			t.Fatal(err)
		}
		test(ua, UserAgent{UserAgent: ua.UserAgent, ID: 2, BrowserID: 1, SystemID: 2, Bot: isbot.NoBotNoMatch}, `
			id  bid  sid  bot  browser     system      ua
			1   1    1    1    Firefox 79  Linux       ~Z (X11; ~L x86_64; rv:79.0) ~g20100101 ~f79.0
			2   2    1    1    Firefox 79  Windows 10  ~Z (~W NT 10.0; Win64; x64; rv:79.0) ~g20100101 ~f79.0
		`)
	}

	{
		ua := UserAgent{UserAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:71.0) Gecko/20100101 Firefox/71.0"}
		err := ua.GetOrInsert(ctx)
		if err != nil {
			t.Fatal(err)
		}
		test(ua, UserAgent{UserAgent: ua.UserAgent, ID: 3, BrowserID: 2, SystemID: 2, Bot: isbot.NoBotNoMatch}, `
			id  bid  sid  bot  browser     system      ua
			1   1    1    1    Firefox 79  Linux       ~Z (X11; ~L x86_64; rv:79.0) ~g20100101 ~f79.0
			2   2    1    1    Firefox 79  Windows 10  ~Z (~W NT 10.0; Win64; x64; rv:79.0) ~g20100101 ~f79.0
			3   2    2    1    Firefox 71  Windows 10  ~Z (~W NT 10.0; Win64; x64; rv:71.0) ~g20100101 ~f71.0
		`)
	}
}

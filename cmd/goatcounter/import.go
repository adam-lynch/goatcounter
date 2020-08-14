// Copyright © 2019 Martin Tournoij <martin@arp242.net>
// This file is part of GoatCounter and published under the terms of the EUPL
// v1.2, which can be found in the LICENSE file or at http://eupl12.zgo.at

package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"zgo.at/errors"
	"zgo.at/goatcounter"
	"zgo.at/goatcounter/cfg"
	"zgo.at/goatcounter/handlers"
	"zgo.at/goatcounter/logscan"
	"zgo.at/json"
	"zgo.at/zdb"
	"zgo.at/zhttp"
	"zgo.at/zli"
	"zgo.at/zlog"
	"zgo.at/zstd/zint"
	"zgo.at/zstd/zstring"
)

const usageImport = `
Import pageviews from an export or logfile.

Overview:

    You must give one filename to import; use - to read from stdin:

        $ goatcounter import export.csv.gz

    Or to read from a log file:

        $ goatcounter import -follow /var/log/nginx/access.log

    This requires a running GoatCounter instance; it's a front-end for the API
    rather than a tool to modify the database directly. If you add an ID or site
    code as the -site flag an API key can be generated automatically, but this
    requires access to the database.

    Alternatively, use an URL in -site if you want to send data to a remote
    instance:

        $ export GOATCOUNTER_API_KEY=..
        $ goatcounter import -site https://stats.example.com

Flags:

  -db          Database connection: "sqlite://<file>" or "postgres://<connect>"
               See "goatcounter help db" for detailed documentation. Default:
               sqlite://db/goatcounter.sqlite3?_busy_timeout=200&_journal_mode=wal&cache=shared

  -debug       Modules to debug, comma-separated or 'all' for all modules.

  -silent      Don't show progress information.

  -site        Site to import to, can be passed as an ID ("1") or site code
               ("example") if you have access to the database. Can be omitted if there's only
               one site in the db.

               Use an URL ("https://stats.example.com") to send data to a remote
               instance; this requires setting GOATCOUNTER_API_KEY.

  -follow      Watch a file for new lines and import them. Existing lines are
               not processed.

  -format      Log format; currently accepted values:

                   csv         GoatCounter CSV export (default)
                   combined    NCSA Combined Log
                   combined-v  NCSA Combined Log with virtual host
                   common      Common Log Format (CLF)
                   common-v    Common Log Format (CLF) with virtual host
                   log:[fmt]   Custom log format; see "goatcounter help logfile"
                               for details on the format.

  -date, -time, -datetime
               Format of date and time for log imports; set automatically when
               using one of the predefined log formats and only needs to be set
               when using a custom log:[..]".
               This follows Go's time format; see "goatcounter help logfile" for
               an overview on how this works.

Environment:

  GOATCOUNTER_API_KEY   API key to use if you're connecting to a remote API;
                        must have "count" permission.
`

// w3c         W3C
// squid       Squid native log format
// aws-cf      AWS Amazon CloudFront (Download Distribution)
// aws-el      AWS Elastic Load Balancing
// aws-s3      AWS Amazon Simple Storage Service (S3)
// gcs         Google Cloud Storage
// virtualmin  Virtualmin Log Format with Virtual Host
// k8s-nginx   Kubernetes Nginx Ingress Log Format

const helpLogfile = `
Format specifiers are given as $name.

List of format specifiers:

    ignore         Ignore zero or more characters.

    time           Time according to the -time value.
    date           Date according to -date value.
    datetime       Date and time according to -datetime value.

    remote_addr    Client remote address; IPv4 or IPv6 address (DNS names are
                   not supported here).
    xff            Client remote address from X-Forwarded-For header field. The
                   remote address will be set to the last non-private IP
                   address.

    method         Request method.
    status         Status code sent to the client.
    http           HTTP request protocol (i.e. HTTP/1.1).
    path           URL path; this may contain the query string.
    query          Query string; only needed if not included in $path.
    referrer       "Referrer" request header.
    user_agent     User-Agent request header.

Some format specifiers that are not (yet) used anywhere:

    host           Server name of the server serving the request.
    timing_sec     Time to serve the request in seconds, with possible decimal.
    timing_milli   Time to serve the request in milliseconds.
    timing_micro   Time to serve the request in microseconds.
    size           Size of the object returned to the client.

Date and time parsing:

    Parsing the date and time is done with Go's time package; instead of

Mon Jan 2 15:04:05 MST 2006

        2006
        1, 01
        2, 02
        3, 03, 15
        4, 04
        5, 05
        .000000000
        MST, -0700

    Some special values:

        ANSIC       = "Mon Jan _2 15:04:05 2006"
        UnixDate    = "Mon Jan _2 15:04:05 MST 2006"
        RFC822      = "02 Jan 06 15:04 MST"
        RFC822Z     = "02 Jan 06 15:04 -0700"
        RFC850      = "Monday, 02-Jan-06 15:04:05 MST"
        RFC1123     = "Mon, 02 Jan 2006 15:04:05 MST"
        RFC1123Z    = "Mon, 02 Jan 2006 15:04:05 -0700"
        RFC3339     = "2006-01-02T15:04:05Z07:00"
        RFC3339Nano = "2006-01-02T15:04:05.999999999Z07:00"

the full documentation is available at https://pkg.go.dev/time
`

func importCmd() (int, error) {
	// So it uses https URLs in site.URL()
	// TODO: should fix it to always use https even on dev and get rid of the
	// exceptions.
	cfg.Prod = true

	dbConnect := flagDB()
	debug := flagDebug()

	var format, siteFlag, date, time, datetime string
	var silent, follow bool
	CommandLine.StringVar(&siteFlag, "site", "", "")
	CommandLine.StringVar(&format, "format", "csv", "")
	CommandLine.BoolVar(&silent, "silent", false, "")
	CommandLine.BoolVar(&follow, "follow", false, "")
	CommandLine.StringVar(&date, "date", "", "")
	CommandLine.StringVar(&time, "time", "", "")
	CommandLine.StringVar(&datetime, "datetime", "", "")
	err := CommandLine.Parse(os.Args[2:])
	if err != nil {
		return 1, err
	}

	files := CommandLine.Args()
	if len(files) == 0 {
		return 1, fmt.Errorf("need a filename")
	}
	if len(files) > 1 {
		return 1, fmt.Errorf("can only specify one filename")
	}

	fp, err := zli.InputOrFile(files[0], silent)
	if err != nil {
		return 1, err
	}
	defer fp.Close()

	zlog.Config.SetDebug(*debug)

	url, key, clean, err := findSite(siteFlag, *dbConnect)
	if err != nil {
		return 1, err
	}
	if clean != nil {
		defer clean()
	}

	// Import from CSV.
	if format == "csv" {
		if follow {
			return 1, fmt.Errorf("cannot use -follow with -format=csv")
		}
		n, err := importCSV(fp, url, key, silent)
		if err != nil {
			var gErr *errors.Group
			if errors.As(err, &gErr) {
				return 1, fmt.Errorf("%d errors", gErr.Len())
			}
			return 1, err
		}

		if !silent {
			zli.EraseLine()
			fmt.Printf("Done! Imported %d rows\n", n)
		}
		return 0, nil
	}

	// Assume log file for everything else.
	var scan *logscan.Scanner
	if follow && files[0] != "-" {
		fp.Close()
		scan, err = logscan.NewFollow(files[0], format, date, time, datetime)
	} else {
		scan, err = logscan.New(fp, format, date, time, datetime)
	}
	if err != nil {
		return 1, err
	}

	hits := make([]handlers.APICountRequestHit, 0, 100)
	for {
		data, err := scan.Line()
		if err == io.EOF {
			break
		}
		if err != nil {
			return 1, err
		}

		hit := handlers.APICountRequestHit{
			Path:      data.Path(),
			Ref:       data.Referrer(),
			Query:     data.Query(),
			UserAgent: data.UserAgent(),
		}

		hit.CreatedAt, err = data.Datetime()
		if err != nil {
			zlog.Error(err)
			continue
		}

		if data.XForwardedFor() != "" {
			xffSplit := strings.Split(data.XForwardedFor(), ",")
			for i := len(xffSplit) - 1; i >= 0; i-- {
				if !zhttp.PrivateIP(xffSplit[i]) {
					hit.IP = zhttp.RemovePort(strings.TrimSpace(xffSplit[i]))
					break
				}
			}
		}
		if hit.IP == "" {
			hit.IP = zhttp.RemovePort(data.RemoteAddr())
		}

		hits = append(hits, hit)

		if len(hits) == 100 {
			// TODO: limit goroutines here
			go func(hits []handlers.APICountRequestHit) {
				defer zlog.Recover()

				err := importSend(url, key, silent, hits)
				if err != nil {
					zlog.Error(err)
				}
			}(hits)

			hits = make([]handlers.APICountRequestHit, 0, 100)
		}
	}

	if len(hits) > 0 {
		err := importSend(url, key, silent, hits)
		if err != nil {
			zlog.Error(err)
		}
	}

	return 0, nil
}

var (
	importClient = http.Client{Timeout: 5 * time.Second}
	nSent        int
)

func newRequest(method, url, key string, body io.Reader) (*http.Request, error) {
	r, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", "Bearer "+key)
	return r, nil
}

func importSend(url, key string, silent bool, hits []handlers.APICountRequestHit) error {
	body, err := json.Marshal(handlers.APICountRequest{Hits: hits})
	if err != nil {
		return err
	}

	if !silent {
		zli.ReplaceLinef("Sending %d hits to %s…", len(hits), url)
	}

	r, err := newRequest("POST", url, key, bytes.NewReader(body))
	if err != nil {
		return err
	}

	resp, err := importClient.Do(r)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	// All okay!
	case 202:
		if !silent {
			fmt.Print(" Okay!")
		}

	// Rate limit
	case 429:
		s, err := strconv.Atoi(resp.Header.Get("X-Rate-Limit-Reset"))
		if err != nil {
			return err
		}

		if !silent {
			zli.ReplaceLinef("Rate limited; sleeping for %d seconds\n", s)
		}
		time.Sleep(time.Duration(s) * time.Second)

	// Other error
	default:
		b, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("%s: %s: %s", url, resp.Status, zstring.ElideLeft(string(b), 200))
	}

	nSent += len(hits)

	// Sleep until next Memstore cycle.
	if nSent > 2000 {
		last, err := time.Parse(time.RFC3339Nano, resp.Header.Get("X-Goatcounter-Memstore"))
		if err != nil {
			return err
		}
		s := time.Until(last.Add(10_000 * time.Millisecond))

		if !silent {
			zli.ReplaceLinef("Sent 2,000 pageviews; Waiting %s", s.Round(time.Millisecond))
		}
		time.Sleep(s)

		nSent = 0
	}
	return nil
}

func importCSV(fp io.Reader, url, key string, silent bool) (int, error) {
	c := csv.NewReader(fp)
	header, err := c.Read()
	if err != nil {
		return 0, err
	}
	if len(header) == 0 || !strings.HasPrefix(header[0], goatcounter.ExportVersion) {
		return 0, errors.Errorf(
			"wrong version of CSV database: %s (expected: %s)",
			header[0][:1], goatcounter.ExportVersion)
	}

	var (
		n        = 0
		sessions = make(map[zint.Uint128]zint.Uint128)
		errs     = errors.NewGroup(50)
		hits     = make([]handlers.APICountRequestHit, 0, 100)
	)
	for {
		line, err := c.Read()
		if err == io.EOF {
			break
		}
		if errs.Append(err) {
			if !silent {
				zli.Errorf(err)
			}
			continue
		}

		var row goatcounter.ExportRow
		if errs.Append(row.Read(line)) {
			if !silent {
				zli.Errorf(err)
			}
			continue
		}

		hit, err := row.Hit(0)
		if errs.Append(row.Read(line)) {
			if !silent {
				zli.Errorf(err)
			}
			continue
		}

		// Map session IDs to new session IDs.
		s, ok := sessions[hit.Session]
		if !ok {
			sessions[hit.Session] = goatcounter.Memstore.SessionID()
		}
		hit.Session = s

		hits = append(hits, handlers.APICountRequestHit{
			Path:      hit.Path,
			Title:     hit.Title,
			Event:     hit.Event,
			Ref:       hit.Ref,
			Size:      hit.Size,
			Bot:       hit.Bot,
			UserAgent: hit.Browser,
			Location:  hit.Location,
			CreatedAt: hit.CreatedAt,
			Session:   hit.Session.String(),
		})
		if len(hits) >= 100 {
			if errs.Append(importSend(url, key, silent, hits)) {
				if !silent {
					zli.Errorf(err)
				}
			}
			hits = make([]handlers.APICountRequestHit, 0, 100)
		}
		n++
	}
	if len(hits) > 0 {
		errs.Append(importSend(url, key, silent, hits))
	}

	return n, errs.ErrorOrNil()
}

func findSite(siteFlag, dbConnect string) (string, string, func(), error) {
	var (
		url, key string
		clean    func()
	)
	switch {
	case strings.HasPrefix(siteFlag, "http://") || strings.HasPrefix(siteFlag, "https://"):
		url = strings.TrimRight(siteFlag, "/")
		url = strings.TrimSuffix(url, "/api/v0/count")
		if !strings.HasPrefix(url, "http") {
			url = "https://" + url
		}

		key = os.Getenv("GOATCOUNTER_API_KEY")
		if key == "" {
			return "", "", nil, errors.New("GOATCOUNTER_API_KEY must be set")
		}

	default:
		db, err := connectDB(dbConnect, nil, false)
		if err != nil {
			return "", "", nil, err
		}
		defer db.Close()
		ctx := zdb.With(context.Background(), db)

		var site goatcounter.Site
		siteID, intErr := strconv.ParseInt(siteFlag, 10, 64)
		switch {
		default:
			err = site.ByCode(ctx, siteFlag)
		case intErr != nil && siteID > 0:
			err = site.ByID(ctx, siteID)
		case siteFlag == "":
			var sites goatcounter.Sites
			err := sites.UnscopedList(ctx)
			if err != nil {
				return "", "", nil, err
			}

			switch len(sites) {
			case 0:
				return "", "", nil, fmt.Errorf("there are no sites in the database")
			case 1:
				site = sites[0]
			default:
				return "", "", nil, fmt.Errorf("more than one site: use -site to specify which site to import")
			}
		}
		if err != nil {
			return "", "", nil, err
		}
		ctx = goatcounter.WithSite(ctx, &site)

		var user goatcounter.User
		err = user.BySite(ctx, site.ID)
		if err != nil {
			return "", "", nil, err
		}
		ctx = goatcounter.WithUser(ctx, &user)

		token := goatcounter.APIToken{
			SiteID:      site.ID,
			Name:        "goatcounter import",
			Permissions: goatcounter.APITokenPermissions{Count: true},
		}
		err = token.Insert(ctx)
		if err != nil {
			return "", "", nil, err
		}

		url = site.URL() + "/api/v0/count"
		key = token.Token
		clean = func() { token.Delete(ctx) }
	}

	// Verify that the site is live and that we've got the correct permissions.
	r, err := newRequest("GET", url+"/api/v0/me", key, nil)
	if err != nil {
		return "", "", nil, err
	}

	resp, err := importClient.Do(r)
	if err != nil {
		return "", "", nil, err
	}
	defer resp.Body.Close()
	b, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return "", "", nil, fmt.Errorf("%s: %s: %s", url+"/api/v0/me",
			resp.Status, zstring.ElideLeft(string(b), 200))
	}

	var perm struct {
		Token goatcounter.APIToken `json:"token"`
	}
	err = json.Unmarshal(b, &perm)
	if err != nil {
		return "", "", nil, err
	}
	if !perm.Token.Permissions.Count {
		return "", "", nil, fmt.Errorf("the API toke %q is missing the 'count' permission", perm.Token.Name)
	}

	return url + "/api/v0/count", key, clean, nil
}

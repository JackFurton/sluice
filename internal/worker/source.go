/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package worker

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/JackFurton/sluice/internal/runspec"
)

// maxResponseBytes bounds a single response body. An upstream that starts
// returning a multi-gigabyte page should fail the run, not the node.
const maxResponseBytes = 256 << 20

// maxAttempts is how many times one page is requested before the run fails.
const maxAttempts = 4

// Credentials are projected into the run pod from a Secret and read from the
// environment. They never appear in the RunConfig.
type Credentials struct {
	Token    string
	Username string
	Password string
	// Headers maps a header name to a value read from the environment.
	Headers map[string]string
}

// Fetcher walks an HTTP source, handing each page of records to a callback.
type Fetcher struct {
	cfg    runspec.SourceConfig
	creds  Credentials
	client *http.Client

	// requests counts upstream calls, including retries, so the reported
	// number matches what the vendor's rate limit actually saw.
	requests int32
	lastCall time.Time
}

// NewFetcher builds a Fetcher for one run.
func NewFetcher(cfg runspec.SourceConfig, creds Credentials) *Fetcher {
	timeout := time.Duration(cfg.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &Fetcher{
		cfg:    cfg,
		creds:  creds,
		client: &http.Client{Timeout: timeout},
	}
}

// Requests reports how many upstream calls were made.
func (f *Fetcher) Requests() int32 { return f.requests }

// PageFunc receives one page of decoded records.
type PageFunc func(records []any) error

// Fetch walks the source and calls fn once per page. The watermark bounds
// narrow the request when the source supports it; they are advisory, and the
// caller still filters records it has already seen.
func (f *Fetcher) Fetch(ctx context.Context, bounds WatermarkBounds, fn PageFunc) error {
	page := f.cfg.Pagination
	maxPages := page.MaxPages
	if maxPages <= 0 {
		maxPages = 1000
	}
	if page.Type == "" || page.Type == runspec.PaginationNone {
		maxPages = 1
	}

	cursor := ""
	nextURL := ""
	offset := page.StartPage
	if page.Type == runspec.PaginationPageNumber && page.StartPage == 0 {
		// Page numbering is conventionally 1-based; offsets are 0-based.
		offset = 1
	}

	for i := int32(0); i < maxPages; i++ {
		requestURL, err := f.buildURL(bounds, cursor, offset, nextURL)
		if err != nil {
			return err
		}

		doc, header, err := f.request(ctx, requestURL)
		if err != nil {
			return err
		}

		records, err := ExtractRecords(doc, f.cfg.RecordsPath)
		if err != nil {
			return err
		}
		if err := fn(records); err != nil {
			return err
		}

		switch page.Type {
		case "", runspec.PaginationNone:
			return nil
		case runspec.PaginationCursor:
			next, ok := ExtractString(doc, page.NextCursorPath)
			if !ok || next == "" || next == cursor {
				return nil
			}
			cursor = next
		case runspec.PaginationPageNumber:
			if isLastPage(records, page.PageSize) {
				return nil
			}
			offset++
		case runspec.PaginationOffset:
			if isLastPage(records, page.PageSize) {
				return nil
			}
			offset += page.PageSize
		case runspec.PaginationLinkHeader:
			next := nextLink(header.Get("Link"))
			if next == "" {
				return nil
			}
			nextURL = next
		default:
			return fmt.Errorf("unsupported pagination type %q", page.Type)
		}
	}

	// Running out of pages is not success. A source that never signals the end
	// of its result set would otherwise look like a clean partial load.
	return fmt.Errorf("stopped after maxPages=%d without reaching the end of the result set", maxPages)
}

// isLastPage reports whether a short page means the result set is exhausted.
// An empty page always ends the walk; a short page ends it only when a page
// size was requested, since without one the server chooses and a small page
// says nothing.
func isLastPage(records []any, pageSize int32) bool {
	if len(records) == 0 {
		return true
	}
	return pageSize > 0 && int32(len(records)) < pageSize
}

func (f *Fetcher) buildURL(bounds WatermarkBounds, cursor string, offset int32, nextURL string) (string, error) {
	if nextURL != "" {
		return nextURL, nil
	}

	u, err := url.Parse(f.cfg.URL)
	if err != nil {
		return "", fmt.Errorf("parse url: %w", err)
	}
	q := u.Query()
	for k, v := range f.cfg.Query {
		q.Set(k, v)
	}
	if bounds.Param != "" {
		if bounds.From != "" {
			q.Set(bounds.Param, bounds.From)
		}
		if bounds.To != "" && bounds.ToParam != "" {
			q.Set(bounds.ToParam, bounds.To)
		}
	}

	page := f.cfg.Pagination
	switch page.Type {
	case runspec.PaginationCursor:
		if cursor != "" && page.CursorParam != "" {
			q.Set(page.CursorParam, cursor)
		}
	case runspec.PaginationPageNumber, runspec.PaginationOffset:
		if page.PageParam != "" {
			q.Set(page.PageParam, strconv.Itoa(int(offset)))
		}
	}
	if page.SizeParam != "" && page.PageSize > 0 {
		q.Set(page.SizeParam, strconv.Itoa(int(page.PageSize)))
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// request performs one page request, retrying on the status codes that mean
// "later, not never".
func (f *Fetcher) request(ctx context.Context, requestURL string) (any, http.Header, error) {
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if attempt > 1 {
			delay := backoff(attempt, lastErr)
			select {
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			case <-time.After(delay):
			}
		}
		f.throttle(ctx)

		doc, header, err := f.do(ctx, requestURL)
		if err == nil {
			return doc, header, nil
		}
		lastErr = err
		var retryable *retryableError
		if !asRetryable(err, &retryable) {
			return nil, nil, err
		}
	}
	return nil, nil, fmt.Errorf("giving up after %d attempts: %w", maxAttempts, lastErr)
}

func (f *Fetcher) do(ctx context.Context, requestURL string) (any, http.Header, error) {
	var body io.Reader
	if f.cfg.Method == http.MethodPost && f.cfg.Body != "" {
		body = strings.NewReader(f.cfg.Body)
	}
	method := f.cfg.Method
	if method == "" {
		method = http.MethodGet
	}

	req, err := http.NewRequestWithContext(ctx, method, requestURL, body)
	if err != nil {
		return nil, nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for _, h := range f.cfg.Headers {
		value := h.Value
		if h.FromSecret {
			value = f.creds.Headers[h.Name]
		}
		if value != "" {
			req.Header.Set(h.Name, value)
		}
	}
	f.applyAuth(req)

	f.requests++
	resp, err := f.client.Do(req)
	if err != nil {
		// Transport errors are transient far more often than not.
		return nil, nil, &retryableError{err: err}
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
		snippet := readSnippet(resp.Body)
		return nil, nil, &retryableError{
			err:        fmt.Errorf("upstream returned %s: %s", resp.Status, snippet),
			retryAfter: parseRetryAfter(resp.Header.Get("Retry-After")),
		}
	}
	if resp.StatusCode >= 300 {
		return nil, nil, fmt.Errorf("upstream returned %s: %s", resp.Status, readSnippet(resp.Body))
	}

	var doc any
	dec := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes))
	if err := dec.Decode(&doc); err != nil {
		return nil, nil, fmt.Errorf("decode response: %w", err)
	}
	return doc, resp.Header, nil
}

func (f *Fetcher) applyAuth(req *http.Request) {
	switch f.cfg.AuthType {
	case runspec.AuthBearer:
		if f.creds.Token != "" {
			req.Header.Set("Authorization", "Bearer "+f.creds.Token)
		}
	case runspec.AuthHeader:
		if f.cfg.AuthHeaderName != "" && f.creds.Token != "" {
			req.Header.Set(f.cfg.AuthHeaderName, f.creds.Token)
		}
	case runspec.AuthBasic:
		if f.creds.Username != "" || f.creds.Password != "" {
			pair := f.creds.Username + ":" + f.creds.Password
			req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(pair)))
		}
	}
}

// throttle spaces requests out to honor maxRequestsPerSecond.
func (f *Fetcher) throttle(ctx context.Context) {
	if f.cfg.MaxRequestsPerSecond <= 0 {
		f.lastCall = time.Now()
		return
	}
	interval := time.Second / time.Duration(f.cfg.MaxRequestsPerSecond)
	if wait := interval - time.Since(f.lastCall); wait > 0 && !f.lastCall.IsZero() {
		select {
		case <-ctx.Done():
		case <-time.After(wait):
		}
	}
	f.lastCall = time.Now()
}

// retryableError marks a failure worth another attempt.
type retryableError struct {
	err        error
	retryAfter time.Duration
}

func (e *retryableError) Error() string { return e.err.Error() }
func (e *retryableError) Unwrap() error { return e.err }

func asRetryable(err error, target **retryableError) bool {
	r, ok := err.(*retryableError)
	if ok {
		*target = r
	}
	return ok
}

// backoff honors an upstream Retry-After when one was sent, and otherwise
// doubles from one second.
func backoff(attempt int, err error) time.Duration {
	var r *retryableError
	if asRetryable(err, &r) && r.retryAfter > 0 {
		return r.retryAfter
	}
	return min(time.Second<<(attempt-2), 30*time.Second)
}

func parseRetryAfter(value string) time.Duration {
	if value == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(value); err == nil {
		if d := time.Until(when); d > 0 {
			return d
		}
	}
	return 0
}

func readSnippet(body io.Reader) string {
	b, _ := io.ReadAll(io.LimitReader(body, 512))
	return strings.TrimSpace(string(b))
}

// nextLink pulls the rel="next" target out of an RFC 8288 Link header.
func nextLink(header string) string {
	for part := range strings.SplitSeq(header, ",") {
		segments := strings.Split(strings.TrimSpace(part), ";")
		if len(segments) < 2 {
			continue
		}
		target := strings.TrimSpace(segments[0])
		if !strings.HasPrefix(target, "<") || !strings.HasSuffix(target, ">") {
			continue
		}
		for _, param := range segments[1:] {
			p := strings.ReplaceAll(strings.TrimSpace(param), `"`, "")
			if p == "rel=next" {
				return strings.Trim(target, "<>")
			}
		}
	}
	return ""
}

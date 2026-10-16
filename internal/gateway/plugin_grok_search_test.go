package gateway

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/yetone/magpie/internal/provider"
)

// searchArgs are the arguments of a Grok model's call to Codex's tool
// search: the integers it writes with a fraction of zeros, beside a string
// and a real fraction.
const searchArgs = `{"query":"calendar create","limit":8.0,"max_results":20.0,"depth":2.5}`

// searchArgsWant is what Codex reads of searchArgs: the zero fractions
// gone, the real fraction as written.
var searchArgsWant = map[string]string{"query": `"calendar create"`, "limit": "8", "max_results": "20", "depth": "2.5"}

// searchArgsRead is the arguments of the tool_search_call a reply carries,
// by where Codex reads it: a stream's output_item.done and completed, or a
// whole reply's output. They are kept as the JSON they were written in: a
// number's literal is what Codex parses, and a float64 gives 8 back for
// 8.0.
func searchArgsRead(t *testing.T, body []byte, stream bool) map[string]string {
	t.Helper()
	read := map[string]string{}
	keep := func(where string, items []json.RawMessage) {
		for _, it := range items {
			var im struct {
				Type      string          `json:"type"`
				Arguments json.RawMessage `json:"arguments"`
			}
			if json.Unmarshal(it, &im) != nil || im.Type != "tool_search_call" {
				continue
			}
			if _, dup := read[where]; dup {
				t.Fatalf("%s: two search calls: %s", where, body)
			}
			if len(im.Arguments) == 0 {
				t.Fatalf("%s: no arguments: %s", where, it)
			}
			read[where] = string(im.Arguments)
		}
	}
	if !stream {
		var res struct {
			Output []json.RawMessage `json:"output"`
		}
		if err := json.Unmarshal(body, &res); err != nil {
			t.Fatalf("%v: %s", err, body)
		}
		keep("output", res.Output)
		return read
	}
	for _, data := range dataLines(t, string(body)) {
		var ev itemEvent
		if err := json.Unmarshal([]byte(data), &ev); err != nil {
			continue
		}
		switch ev.Type {
		case "response.output_item.done":
			keep("output_item.done", []json.RawMessage{rawJSONOf(t, ev.Item)})
		case "response.completed":
			var res struct {
				Response struct {
					Output []json.RawMessage `json:"output"`
				} `json:"response"`
			}
			if json.Unmarshal([]byte(data), &res) == nil {
				keep("completed", res.Response.Output)
			}
		}
	}
	return read
}

// rawJSONOf is an item's own JSON, as it was written.
func rawJSONOf(t *testing.T, item map[string]json.RawMessage) json.RawMessage {
	t.Helper()
	b, err := marshalPlain(item)
	if err != nil {
		t.Fatalf("%v", err)
	}
	return b
}

// checkSearchArgs checks every place Codex reads the search call's
// arguments, and that the model's real fraction survived.
func checkSearchArgs(t *testing.T, body []byte, stream bool) {
	t.Helper()
	read := searchArgsRead(t, body, stream)
	if len(read) == 0 {
		t.Fatalf("stream %v: no search call read: %s", stream, body)
	}
	for where, got := range read {
		fields := fieldsOf(t, got)
		if len(fields) != len(searchArgsWant) {
			t.Fatalf("stream %v: %s: arguments %s, want %v", stream, where, got, searchArgsWant)
		}
		for k, v := range searchArgsWant {
			if fields[k] != v {
				t.Fatalf("stream %v: %s: %s is %s in %s, want %s", stream, where, k, fields[k], got, v)
			}
		}
	}
}

// searchCallItem is the function_call to the search the plugin's model
// writes: its arguments a string, as a Responses upstream's are.
func searchCallItem() string {
	return `{"type":"function_call","id":"fc_1","call_id":"s1","name":"tool_search","arguments":` + quoted(searchArgs) + `,"status":"completed"}`
}

// searchStream is a relayed stream answering with one call to the search,
// its arguments streamed in the pieces and given whole in its done event,
// so what the deltas add up to and what the done events say are both read.
func searchStream(pieces ...string) string {
	lines := []string{
		`event: response.created` + "\n" + `data: {"type":"response.created","response":{"id":"r1","object":"response","status":"in_progress","output":[]}}`,
		`event: response.output_item.added` + "\n" + `data: {"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","id":"fc_1","call_id":"s1","name":"tool_search","arguments":"","status":"in_progress"}}`,
	}
	for _, p := range pieces {
		lines = append(lines, `event: response.function_call_arguments.delta`+"\n"+`data: {"type":"response.function_call_arguments.delta","item_id":"fc_1","output_index":0,"delta":`+quoted(p)+`}`)
	}
	return sse(append(lines,
		`event: response.function_call_arguments.done`+"\n"+`data: {"type":"response.function_call_arguments.done","item_id":"fc_1","output_index":0,"arguments":`+quoted(searchArgs)+`}`,
		`event: response.output_item.done`+"\n"+`data: {"type":"response.output_item.done","output_index":0,"item":`+searchCallItem()+`}`,
		`event: response.completed`+"\n"+`data: {"type":"response.completed","response":{"id":"r1","object":"response","status":"completed","model":"grok-4.7","output":[`+searchCallItem()+`],"usage":{"input_tokens":5,"output_tokens":3}}}`)...)
}

// A Grok model the Grok plugin serves writes the search's integers with a
// fraction of zeros too ("limit":8.0), which Codex reads strictly and turns
// the search away over ("failed to parse tool_search arguments: invalid
// number"), so spawn_agent and wait_agent, which Codex defers behind the
// search, are never found. The plugin's reply is relayed through the same
// chain as a relay's (searchTidy, then intTidy), streamed in deltas or
// gathered whole for an account that only streams (gathers, plugins#112),
// and Codex reads integers in both.
func TestGrokPluginSearchGivesCodexIntegers(t *testing.T) {
	up := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, searchStream(`{"query":"calendar crea`, `te","limit":8.0,"depth":2.5}`))
	})
	for _, stream := range []bool{true, false} {
		pid := besideFake(t, "grok", up)
		body := `{"model":"` + pid + `/grok-4.7","stream":` + strconv.FormatBool(stream) + `,` + searchTools + `,` + searchInput + `}`
		checkSearchArgs(t, askGrok(t, "/v1/responses", body), stream)
	}
}

// A Grok model the plugin serves writes the search's arguments as an
// object, as the ChatGPT backend does; Codex reads the integers in it too.
func TestGrokPluginSearchObjectGivesCodexIntegers(t *testing.T) {
	item := `{"type":"function_call","id":"fc_1","call_id":"s1","name":"tool_search","arguments":{"query":"mail","limit":15.0},"status":"completed"}`
	up := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, sse(
			`event: response.created`+"\n"+`data: {"type":"response.created","response":{"id":"r1","object":"response","status":"in_progress","output":[]}}`,
			`event: response.output_item.done`+"\n"+`data: {"type":"response.output_item.done","output_index":0,"item":`+item+`}`,
			`event: response.completed`+"\n"+`data: {"type":"response.completed","response":{"id":"r1","object":"response","status":"completed","model":"grok-4.7","output":[`+item+`],"usage":{"input_tokens":5,"output_tokens":3}}}`))
	})
	for _, stream := range []bool{true, false} {
		pid := besideFake(t, "grok", up)
		body := `{"model":"` + pid + `/grok-4.7","stream":` + strconv.FormatBool(stream) + `,` + searchTools + `,` + searchInput + `}`
		read := searchArgsRead(t, askGrok(t, "/v1/responses", body), stream)
		if len(read) == 0 {
			t.Fatalf("stream %v: no search call read", stream)
		}
		for where, got := range read {
			fields := fieldsOf(t, got)
			if fields["query"] != `"mail"` || fields["limit"] != "15" {
				t.Fatalf("stream %v: %s: %s", stream, where, got)
			}
		}
	}
}

// The same search from a model that is not Grok's goes as it came, its
// 8.0 untouched.
func TestNonGrokPluginSearchKeepsItsFractions(t *testing.T) {
	up := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, searchStream())
	})
	// fake-resp is the fake plugin's own Responses model, no Grok in its
	// name (internal/plugin/testdata/fake/index.js, $FAKE_RESPONSES)
	pid := besideFake(t, "openai", up)
	body := `{"model":"` + pid + `/fake-resp","stream":true,` + searchTools + `,` + searchInput + `}`
	read := searchArgsRead(t, askGrok(t, "/v1/responses", body), true)
	if len(read) == 0 {
		t.Fatalf("no search call read")
	}
	for where, got := range read {
		fields := fieldsOf(t, got)
		if fields["limit"] != "8.0" || fields["max_results"] != "20.0" || fields["depth"] != "2.5" {
			t.Fatalf("%s: another model's arguments changed: %s", where, got)
		}
	}
}

// A search a relay gives Codex whole, with no deltas at all, still has its
// integers: a whole reply is held and gone through at the end.
func TestGrokRelaySearchWholeGivesCodexIntegers(t *testing.T) {
	item := `{"type":"function_call","id":"fc_1","call_id":"s1","name":"tool_search","arguments":` + quoted(`{"query":"mail","limit":8.0}`) + `,"status":"completed"}`
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, sse(
			`event: response.created`+"\n"+`data: {"type":"response.created","response":{"id":"r1","object":"response","status":"in_progress","output":[]}}`,
			`event: response.output_item.done`+"\n"+`data: {"type":"response.output_item.done","output_index":0,"item":`+item+`}`,
			`event: response.completed`+"\n"+`data: {"type":"response.completed","response":{"id":"r1","object":"response","status":"completed","model":"grok-4.7","output":[`+item+`],"usage":{"input_tokens":5,"output_tokens":3}}}`))
	}))
	defer up.Close()
	fresh(t)
	if err := provider.Save(provider.Provider{ID: "relay", Name: "Relay", Key: "k", Models: []string{"grok-4.7"}, Responses: up.URL + "/v1"}); err != nil {
		t.Fatal(err)
	}
	read := searchArgsRead(t, askGrok(t, "/v1/responses", `{"model":"relay/grok-4.7","stream":true,`+searchTools+`,`+searchInput+`}`), true)
	if len(read) == 0 {
		t.Fatal("no search call read")
	}
	for where, got := range read {
		fields := fieldsOf(t, got)
		if fields["query"] != `"mail"` || fields["limit"] != "8" {
			t.Fatalf("%s: %s", where, got)
		}
	}
}

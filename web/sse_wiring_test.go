package web

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// An SSE event name lives in three places that the compiler does not connect:
//
//  1. the emit site — either a raw string handed to api/rest.Server.OnEvent, or
//     a web.SSEEvent literal broadcast directly;
//  2. the EventType const block plus AllEventTypes in web/sse.go;
//  3. the eventTypes listener array in web/static/js/sse.js.
//
// The REST bridge in cmd/sage-gui/node.go adopts whatever string the handler
// passed (web.EventTypeFromREST), so the const block never constrains the emit
// sites, and EventSource only delivers named events to listeners that were
// explicitly registered, so the browser silently drops anything missing from the
// JavaScript array. An event can therefore be emitted correctly, marshalled
// correctly, written to the wire correctly — and observed by nobody. Every Go
// test passes and scripts/check-static-js.mjs passes, because it only parses
// modules.
//
// That is not hypothetical. Before this test existed, "cocommit", "search",
// "hybrid", "pipeline_send", "pipeline_complete", "reinstate" and "redeploy"
// were all emitted by production code and absent from both the const block and
// the JavaScript array.
//
// DESIGN CHOICE: this test asserts FULL consistency and the accompanying change
// closed those seven gaps, rather than asserting the invariant for an allow-list
// of events that "should" be observable. An allow-list would have been a fourth
// hand-maintained copy of the same list, reproducing the exact failure mode it
// was meant to prevent: add an event, forget the allow-list, nothing fails.
// Full consistency has no list to forget — a new emit site fails the test until
// the event is registered in Go and subscribed to in JavaScript.
func TestSSEEventWiring(t *testing.T) {
	scan := scanSSESources(t)
	registry := eventTypeStrings(AllEventTypes)

	t.Run("registry is well formed", func(t *testing.T) {
		assertNoDuplicates(t, registry, "web.AllEventTypes")
		for _, name := range registry {
			assert.NotEmpty(t, name, "web.AllEventTypes must not contain an empty event name")
		}
	})

	// The const block is the vocabulary; AllEventTypes is the registry the other
	// two checks are stated against. A const declared but never registered is
	// invisible to those checks, so pin them to each other first.
	t.Run("const block matches AllEventTypes", func(t *testing.T) {
		declared := make([]string, 0, len(scan.consts))
		for _, value := range scan.consts {
			declared = append(declared, value)
		}
		assertSameEventSet(t, registry, declared,
			"web.AllEventTypes", "the EventType const block in web/sse.go", "")
	})

	t.Run("every emitted event is registered and every registered event is emitted", func(t *testing.T) {
		require.Empty(t, scan.unresolved,
			"every SSE emit site must name its event with a string literal or an EventType const so the wiring is checkable")
		require.NotEmpty(t, scan.emits, "found no SSE emit sites at all — the source scan is broken")

		emitted := make([]string, 0, len(scan.emits))
		for _, site := range scan.emits {
			emitted = append(emitted, site.name)
		}
		assertSameEventSet(t, registry, emitted,
			"web.AllEventTypes", "the SSE event names emitted by Go code",
			"emit sites found:\n"+scan.sitesByName())
	})

	t.Run("dashboard JavaScript subscribes to exactly the registered events", func(t *testing.T) {
		// Comment-stripped for the same reason the loop check below strips: a
		// commented-out entry is the single most common way anyone removes a
		// name from a JS array, and the literal harvester cannot tell a live
		// element from one inside a comment. Reading the raw source here would
		// let `// 'redeploy',` satisfy the registry while the browser
		// subscribes to nothing — the exact defect this file exists to catch,
		// reintroduced by the assertion meant to catch it.
		js := stripJSComments(readDashboardSSEScript(t))
		listened := jsListenedEventTypes(t, js)
		assertNoDuplicates(t, listened, "the eventTypes array in web/static/js/sse.js")
		assertSameEventSet(t, registry, listened,
			"web.AllEventTypes", "the eventTypes array in web/static/js/sse.js", "")
	})

	// The array only matters because it drives addEventListener. Pin that too,
	// or the array could be preserved as decoration while the browser stops
	// subscribing.
	t.Run("the eventTypes array actually registers the listeners", func(t *testing.T) {
		// Assert against COMMENT-STRIPPED source. A raw substring check passes
		// happily on `// for (const type of eventTypes) {` — commenting the loop
		// out would leave the array intact as decoration, the browser
		// subscribing to nothing, and this test green. That is the precise
		// failure mode the whole file exists to prevent, so it must not be
		// reintroduced by the assertion itself.
		js := stripJSComments(readDashboardSSEScript(t))
		assert.Contains(t, js, "for (const type of eventTypes) {",
			"eventTypes must be the LIVE loop that registers listeners (commented-out code does not count)")
		assert.Contains(t, js, "this.es.addEventListener(type, (e) => {",
			"each event type in the array must be subscribed on the EventSource")
	})

	// The scanner grants exactly two call-sites a pass: the REST bridge adopts a
	// name checked at its own OnEvent site, and the contentless retrieval
	// wrapper forwards OnEvent through a func parameter the scan cannot follow.
	// Both are sound ONLY while they stay where they are. An unbounded pass is
	// how an unregistered event would reach the stream unnoticed, so the set of
	// callers is pinned: adding one is a deliberate act that fails this test and
	// has to be justified rather than absorbed.
	t.Run("the scanner's sanctioned bypasses stay bounded", func(t *testing.T) {
		assert.Equal(t,
			[]string{"cmd/sage-gui/node.go"},
			callerFilesOf(t, "EventTypeFromREST"),
			"EventTypeFromREST is the single REST->SSE bridge; a new caller can launder an unregistered name past the scan")
		assert.Equal(t,
			[]string{"api/rest/memory_handler.go"},
			callerFilesOf(t, "emitContentlessRetrievalActivity"),
			"the contentless retrieval wrapper is scanned by name; a new caller elsewhere is invisible to the wiring pin")

		// Pinning the two helper NAMES does not bound the bypass, because the
		// thing that launders a name is the callback ESCAPING as a value:
		//
		//	emit := s.OnEvent
		//	emit("brand_new_event", "", "", "", nil)
		//
		// produces no emit site and no unresolved entry — the scan simply does
		// not see it, and every other subtest here stays green. Any new wrapper
		// taking an api/rest.EventCallback has the same effect without touching
		// either pinned name. So pin the SHAPE: OnEvent may be called, handed to
		// the one sanctioned wrapper, nil-checked, or assigned to. Reading it as
		// a value anywhere else has to be a deliberate act that fails here.
		assert.Empty(t, scan.escapes,
			"the OnEvent callback must not escape as a value: a func value carries the event name past the AST scan, so a new indirection would emit unregistered events with every wiring assertion still green")
	})
}

func readDashboardSSEScript(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile("static/js/sse.js")
	require.NoError(t, err)
	return string(raw)
}

var (
	jsEventTypesArrayRe = regexp.MustCompile(`(?s)const\s+eventTypes\s*=\s*\[(.*?)]`)
	// Anchored at BOTH ends, so an element must be entirely a quoted string:
	// `void 'x'`, `f('x')` and `c ? 'x' : 'y'` fail to match rather than
	// yielding their inner text. The older unanchored form it replaced is
	// deliberately gone rather than kept around: an unanchored harvester is
	// exactly the false negative this file just closed, and leaving it in
	// scope invites the next edit to reach for it.
	jsBareStringLiteralRe = regexp.MustCompile(`^(?:'([^']*)'|"([^"]*)")$`)
)

// jsListenedEventTypes extracts the event names the dashboard client subscribes
// to from the single eventTypes array literal in static/js/sse.js.
func jsListenedEventTypes(t *testing.T, js string) []string {
	t.Helper()
	matches := jsEventTypesArrayRe.FindAllStringSubmatch(js, -1)
	require.Len(t, matches, 1,
		"static/js/sse.js must declare exactly one eventTypes array; a second one would shadow the checked list")

	// Validate the SHAPE of every element rather than harvesting any quoted
	// string out of the array body. Harvesting was trivially defeatable by an
	// element that CONTAINS a string literal but does not EVALUATE to one:
	// `void 'redeploy'` is valid JavaScript, evaluates to undefined, and made
	// the browser subscribe to "undefined" while this function still reported
	// "redeploy" as listened — a false negative in the one direction that
	// matters, since the event then reaches nobody.
	//
	// Anything that is not a bare quoted literal is refused loudly rather than
	// silently reinterpreted. A call, an operator, a template literal, a
	// conditional, a spread or an identifier each evaluate to something this
	// test cannot predict, and a listener list is exactly where "probably
	// fine" is not good enough.
	names := make([]string, 0)
	for _, element := range strings.Split(matches[0][1], ",") {
		element = strings.TrimSpace(element)
		if element == "" {
			continue
		}
		literal := jsBareStringLiteralRe.FindStringSubmatch(element)
		require.NotNil(t, literal,
			"every element of the eventTypes array in web/static/js/sse.js must be a bare quoted string literal; %q is not, so what the browser actually subscribes to cannot be determined from the source", element)
		name := literal[1]
		if name == "" {
			name = literal[2]
		}
		names = append(names, name)
	}
	return names
}

// sseEmitSite is one place in the repository that names an SSE event.
type sseEmitSite struct {
	name string
	pos  string
}

// sseSourceScan is the result of reading every non-test Go file in the module
// for SSE event vocabulary and SSE emit sites.
type sseSourceScan struct {
	consts map[string]string // EventType const identifier -> event name
	// stringConsts holds plain untyped string consts, which is how a package
	// that cannot import web (api/rest, by design — see EventTypeFromREST)
	// names an event it emits. They are a strictly weaker source than consts:
	// the map is keyed by bare identifier across the whole module, so any name
	// declared twice with different values is recorded in stringConstConflicts
	// and refused at resolve time rather than silently resolving to whichever
	// file the walk happened to reach last.
	// Keyed by "<package dir>\x00<identifier>". Keying by bare identifier
	// across the module made this rail 394 names wide and one-directional: a
	// const in an unrelated package could resolve an emit site here, and the
	// conflict set could not see it because it only compares const against
	// const — a package-level VAR of the same name is invisible to it while
	// still shadowing at the call site. Same-package scoping removes the whole
	// class instead of widening the conflict detector to chase it.
	stringConsts         map[string]string
	stringConstConflicts map[string]bool
	// fileDirs resolves an AST file back to its package directory, so a lookup
	// can be restricted to consts declared alongside the emit site.
	fileDirs map[*ast.File]string
	// fileImports records, per file, the identifiers that are genuinely package
	// qualifiers. Without it a selector cannot be told apart from a field access
	// on a local value, and every EventType const is impersonable by any struct
	// field sharing its name.
	fileImports map[*ast.File]map[string]bool
	emits       []sseEmitSite
	unresolved  []string
	// escapes records every place the OnEvent callback is read as a VALUE
	// rather than called. That is the shape that actually launders an event
	// name past this scan: `emit := s.OnEvent; emit("anything", ...)` produces
	// no emit site and no unresolved entry, so pinning the callers of two
	// named helper functions does not bound the bypass — the escaping callback
	// does.
	escapes []string
}

// sitesByName renders the emit sites grouped by event name so a failure names
// the offending file and line rather than only the offending string.
func (s sseSourceScan) sitesByName() string {
	grouped := map[string][]string{}
	for _, site := range s.emits {
		grouped[site.name] = append(grouped[site.name], site.pos)
	}
	names := make([]string, 0, len(grouped))
	for name := range grouped {
		names = append(names, name)
	}
	sort.Strings(names)

	var b strings.Builder
	for _, name := range names {
		b.WriteString("    ")
		b.WriteString(name)
		b.WriteString(": ")
		b.WriteString(strings.Join(grouped[name], ", "))
		b.WriteString("\n")
	}
	return b.String()
}

var sseScanSkipDirs = map[string]bool{
	"third_party":  true,
	"vendor":       true,
	"node_modules": true,
	"testdata":     true,
	".git":         true,
}

// scanSSESources parses every non-test Go file in the module and collects both
// the EventType vocabulary and every SSE emit site. It walks the whole module
// rather than a fixed package list so an emit site added in a new package is
// covered the day it is written.
func scanSSESources(t *testing.T) sseSourceScan {
	t.Helper()

	root := moduleRoot(t)
	fset := token.NewFileSet()
	type parsedFile struct {
		path string
		file *ast.File
	}
	var parsed []parsedFile

	walkErr := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if path != root && (sseScanSkipDirs[entry.Name()] || strings.HasPrefix(entry.Name(), ".")) {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, parseErr := parser.ParseFile(fset, path, nil, 0)
		if parseErr != nil {
			return parseErr
		}
		parsed = append(parsed, parsedFile{path: path, file: file})
		return nil
	})
	require.NoError(t, walkErr)
	require.NotEmpty(t, parsed, "found no Go sources to scan")

	scan := sseSourceScan{
		consts:               map[string]string{},
		stringConsts:         map[string]string{},
		stringConstConflicts: map[string]bool{},
		fileDirs:             map[*ast.File]string{},
		fileImports:          map[*ast.File]map[string]bool{},
	}

	// Pass 1: the EventType vocabulary, so pass 2 can resolve identifiers.
	for _, pf := range parsed {
		scan.fileDirs[pf.file] = filepath.ToSlash(filepath.Dir(pf.path))
		imports := map[string]bool{}
		for _, spec := range pf.file.Imports {
			name := ""
			if spec.Name != nil {
				name = spec.Name.Name
			} else if path, err := strconv.Unquote(spec.Path.Value); err == nil {
				name = path
				if idx := strings.LastIndex(name, "/"); idx >= 0 {
					name = name[idx+1:]
				}
			}
			if name != "" && name != "_" && name != "." {
				imports[name] = true
			}
		}
		scan.fileImports[pf.file] = imports
		for _, decl := range pf.file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.CONST {
				continue
			}
			for _, spec := range gen.Specs {
				value, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				typeIdent, isIdent := value.Type.(*ast.Ident)
				if !isIdent || typeIdent.Name != "EventType" {
					// Not EventType. Still record plain `const x = "..."` string
					// values, so an emit site in a package that cannot import
					// web stays resolvable instead of unscannable.
					if value.Type == nil {
						for i, name := range value.Names {
							if i >= len(value.Values) {
								continue
							}
							lit, isLit := value.Values[i].(*ast.BasicLit)
							if !isLit || lit.Kind != token.STRING {
								continue
							}
							unquoted, unquoteErr := strconv.Unquote(lit.Value)
							if unquoteErr != nil {
								continue
							}
							key := filepath.ToSlash(filepath.Dir(pf.path)) + "\x00" + name.Name
							if prior, seen := scan.stringConsts[key]; seen && prior != unquoted {
								scan.stringConstConflicts[key] = true
								continue
							}
							scan.stringConsts[key] = unquoted
						}
					}
					continue
				}
				for i, name := range value.Names {
					require.Less(t, i, len(value.Values),
						"EventType const %s has no value at %s", name.Name, relPos(root, fset, name.Pos()))
					lit, ok := value.Values[i].(*ast.BasicLit)
					require.True(t, ok && lit.Kind == token.STRING,
						"EventType const %s must be a string literal at %s", name.Name, relPos(root, fset, name.Pos()))
					unquoted, unquoteErr := strconv.Unquote(lit.Value)
					require.NoError(t, unquoteErr)
					scan.consts[name.Name] = unquoted
				}
			}
		}
	}
	require.NotEmpty(t, scan.consts, "found no EventType constants — the source scan is broken")

	// Pass 2: the emit sites.
	for _, pf := range parsed {
		pf := pf
		record := func(expr ast.Expr, what string) {
			pos := relPos(root, fset, expr.Pos())
			names, ok := scan.resolve(pf.file, expr, 0)
			if !ok {
				scan.unresolved = append(scan.unresolved, what+" at "+pos)
				return
			}
			for _, name := range names {
				scan.emits = append(scan.emits, sseEmitSite{name: name, pos: pos})
			}
		}

		// Positions of OnEvent references that are legitimate: called directly,
		// handed to the one sanctioned wrapper, compared against nil, or
		// assigned TO (which installs the callback rather than reading it).
		sanctioned := map[token.Pos]bool{}
		ast.Inspect(pf.file, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.CallExpr:
				if sel, ok := n.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "OnEvent" {
					sanctioned[sel.Pos()] = true
				}
				if ident, ok := n.Fun.(*ast.Ident); ok && ident.Name == "emitContentlessRetrievalActivity" {
					for _, arg := range n.Args {
						if sel, isSel := arg.(*ast.SelectorExpr); isSel && sel.Sel.Name == "OnEvent" {
							sanctioned[sel.Pos()] = true
						}
					}
				}
			case *ast.BinaryExpr:
				for _, side := range []ast.Expr{n.X, n.Y} {
					if sel, ok := side.(*ast.SelectorExpr); ok && sel.Sel.Name == "OnEvent" {
						sanctioned[sel.Pos()] = true
					}
				}
			case *ast.AssignStmt:
				for _, lhs := range n.Lhs {
					if sel, ok := lhs.(*ast.SelectorExpr); ok && sel.Sel.Name == "OnEvent" {
						sanctioned[sel.Pos()] = true
					}
				}
			}
			return true
		})
		ast.Inspect(pf.file, func(node ast.Node) bool {
			sel, ok := node.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "OnEvent" || sanctioned[sel.Pos()] {
				return true
			}
			scan.escapes = append(scan.escapes, relPos(root, fset, sel.Pos()))
			return true
		})

		// An SSEEvent can be built INCREMENTALLY rather than as a composite
		// literal:
		//	var event SSEEvent
		//	event.Type = EventType(name)
		//	h.SSE.Broadcast(event)
		// That shape has no composite literal to inspect and no OnEvent call,
		// so it passed the scan entirely. Track every variable declared as an
		// SSEEvent, resolve its .Type assignment as an emit site, and report
		// any that never receives one.
		// Keyed by the DECLARATION's own position, never by the bare variable
		// name. A file-global name map let two functions that each declare
		// `var event SSEEvent` collide: the first function's safe
		// `event.Type = EventRemember` deleted the shared entry, and the second
		// function's `event.Type = EventType(runtime)` then had nothing left to
		// report. Declaration identity cannot collide, so the shape is closed
		// rather than narrowed.
		builtEvents := map[token.Pos]builtEventDecl{}
		ast.Inspect(pf.file, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.ValueSpec:
				if !isSSEEventType(n.Type) {
					return true
				}
				for _, name := range n.Names {
					if name.Name != "_" {
						builtEvents[name.Pos()] = builtEventDecl{
							name:  name.Name,
							scope: declScopeOf(pf.file, name.Pos()),
							pos:   name.Pos(),
						}
					}
				}
			case *ast.AssignStmt:
				for _, lhs := range n.Lhs {
					if lit, ok := lhs.(*ast.Ident); ok {
						if len(n.Rhs) == 1 {
							if comp, isComp := n.Rhs[0].(*ast.CompositeLit); isComp &&
								isSSEEventType(comp.Type) && len(comp.Elts) == 0 && lit.Name != "_" {
								builtEvents[lit.Pos()] = builtEventDecl{
									name:  lit.Name,
									scope: declScopeOf(pf.file, lit.Pos()),
									pos:   lit.Pos(),
								}
							}
						}
					}
				}
			}
			return true
		})
		ast.Inspect(pf.file, func(node ast.Node) bool {
			assign, ok := node.(*ast.AssignStmt)
			if !ok {
				return true
			}
			for i, lhs := range assign.Lhs {
				sel, isSel := lhs.(*ast.SelectorExpr)
				if !isSel || sel.Sel.Name != "Type" || i >= len(assign.Rhs) {
					continue
				}
				base, isIdent := sel.X.(*ast.Ident)
				if !isIdent {
					continue
				}
				// Match the assignment to the declaration it actually refers to:
				// same name AND the assignment sits inside that declaration's
				// scope. Matching on name alone is what let one function clear
				// another function's tracking.
				matched := token.NoPos
				for pos, decl := range builtEvents {
					if decl.name != base.Name {
						continue
					}
					if decl.scope == nil ||
						base.Pos() < decl.scope.Pos() || base.Pos() > decl.scope.End() {
						continue
					}
					if matched == token.NoPos || pos > matched {
						matched = pos
					}
				}
				if matched == token.NoPos {
					continue
				}
				delete(builtEvents, matched)
				record(assign.Rhs[i], "SSEEvent Type assignment")
			}
			return true
		})
		for _, decl := range builtEvents {
			scan.unresolved = append(scan.unresolved,
				"SSEEvent variable "+decl.name+" never receives a checkable Type at "+relPos(root, fset, decl.pos))
		}

		ast.Inspect(pf.file, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.CallExpr:
				// The contentless retrieval choke point forwards OnEvent as a
				// func VALUE, so the OnEvent call inside it names its event
				// through a parameter the scan cannot follow. Recording the
				// wrapper's own call sites keeps recall/search/hybrid visible
				// to the wiring pin instead of silently dropping out of it —
				// which is exactly what happened when that helper was
				// introduced. Its caller set stays pinned by the bounded-bypass
				// subtest, so this cannot become an open door.
				if ident, isIdent := n.Fun.(*ast.Ident); isIdent &&
					ident.Name == "emitContentlessRetrievalActivity" && len(n.Args) >= 2 {
					record(n.Args[1], "emitContentlessRetrievalActivity event name")
					return true
				}
				sel, ok := n.Fun.(*ast.SelectorExpr)
				if !ok || sel.Sel.Name != "OnEvent" || len(n.Args) == 0 {
					return true
				}
				record(n.Args[0], "OnEvent event name")
			case *ast.CompositeLit:
				if !isSSEEventType(n.Type) {
					return true
				}
				for _, elt := range n.Elts {
					kv, ok := elt.(*ast.KeyValueExpr)
					if !ok {
						continue
					}
					key, ok := kv.Key.(*ast.Ident)
					if !ok || key.Name != "Type" {
						continue
					}
					record(kv.Value, "SSEEvent Type field")
					return true
				}
				// A zero literal is not reported here: it is tracked above as an
				// incrementally-built event, so it is resolved when a later
				// ev.Type = ... assignment supplies a checkable name and
				// reported when nothing ever does. Reporting it here as well
				// would refuse the legitimate incremental shape outright.
				if len(n.Elts) > 0 {
					scan.unresolved = append(scan.unresolved,
						"SSEEvent literal without a Type field at "+relPos(root, fset, n.Pos()))
				}
			}
			return true
		})
	}

	return scan
}

// isSSEEventType reports whether a composite literal type is web.SSEEvent,
// written either from inside package web or through the package qualifier.
func isSSEEventType(expr ast.Expr) bool {
	switch t := expr.(type) {
	case *ast.Ident:
		return t.Name == "SSEEvent"
	case *ast.SelectorExpr:
		return t.Sel.Name == "SSEEvent"
	}
	return false
}

// resolve statically evaluates an expression that names an SSE event. It returns
// every value the expression can take, and false when the expression cannot be
// evaluated at all — which the test treats as a defect, because an unreadable
// emit site is exactly the blind spot this file exists to close.
func (s *sseSourceScan) resolve(file *ast.File, expr ast.Expr, depth int) ([]string, bool) {
	if depth > 4 {
		return nil, false
	}
	switch e := expr.(type) {
	case *ast.BasicLit:
		if e.Kind != token.STRING {
			return nil, false
		}
		unquoted, err := strconv.Unquote(e.Value)
		if err != nil {
			return nil, false
		}
		return []string{unquoted}, true
	case *ast.Ident:
		// A LOCAL BINDING SHADOWS A PACKAGE CONST, so it must be consulted
		// FIRST. Reading s.consts up front meant
		//	EventRemember := EventType(r.Header.Get("X-Whatever"))
		// resolved through the GLOBAL const map to "remember" while the site
		// actually emitted arbitrary runtime input. That is the same
		// certified-with-a-value-it-does-not-have failure as the file-scoped
		// resolution below, reached by shadowing instead of by scope.
		if names, ok, bound := s.resolveLocalVar(file, e, depth); bound {
			// Bound but unresolvable is a REFUSAL, never a fallthrough to the
			// const map — falling through is precisely the shadowing bug.
			if !ok {
				return nil, false
			}
			return names, true
		}
		if value, ok := s.consts[e.Name]; ok {
			return []string{value}, true
		}
		// Weakest source, checked last and never when ambiguous: a plain
		// untyped string const. Refusing a conflicted name keeps a wrong
		// resolution from masquerading as a checked one.
		if dir, known := s.fileDirs[file]; known {
			key := dir + "\x00" + e.Name
			if value, ok := s.stringConsts[key]; ok && !s.stringConstConflicts[key] {
				return []string{value}, true
			}
		}
		return nil, false
	case *ast.SelectorExpr:
		// A selector resolves ONLY when its qualifier is genuinely a package
		// this file imports. Matching on Sel.Name alone meant ANY expression
		// whose final identifier happened to equal a const name resolved to
		// that const's value:
		//	eventNames := probeEventNames{EventRedeploy: EventType(req.Operation)}
		//	h.SSE.Broadcast(SSEEvent{Type: eventNames.EventRedeploy, ...})
		// certified as "redeploy" while emitting arbitrary runtime input. A
		// struct field, a method value, or any receiver at all could borrow a
		// const's identity that way.
		pkg, isIdent := e.X.(*ast.Ident)
		if !isIdent || !s.fileImports[file][pkg.Name] {
			return nil, false
		}
		if value, ok := s.consts[e.Sel.Name]; ok {
			return []string{value}, true
		}
		return nil, false
	case *ast.CallExpr:
		if len(e.Args) != 1 {
			return nil, false
		}
		// web.EventTypeFromREST is the single sanctioned bridge: it adopts the
		// raw name a REST handler passed to OnEvent, and that name is checked at
		// the OnEvent call site itself, so contributing nothing here is correct.
		// A bare EventType(x) conversion gets no such pass — only its argument
		// can excuse it — because that is precisely how an unregistered event
		// would otherwise reach the stream unnoticed.
		if isCalled(e.Fun, "EventTypeFromREST") {
			// A caller that hands the bridge a name it knows statically is not
			// bridging anything, so that name is held to the registry too.
			if names, ok := s.resolve(file, e.Args[0], depth+1); ok {
				return names, true
			}
			return nil, true
		}
		// string(x) is a conversion, not a choice: it cannot change the value,
		// so the argument is exactly as checkable as a bare name would be. This
		// is the shape api/rest must use, since it holds its event names as
		// untyped consts rather than web.EventType.
		if isCalled(e.Fun, "string") {
			return s.resolve(file, e.Args[0], depth+1)
		}
		if !isCalled(e.Fun, "EventType") {
			return nil, false
		}
		return s.resolve(file, e.Args[0], depth+1)
	}
	return nil, false
}

// resolveLocalVar resolves an identifier that is not an EventType const by
// collecting every value assigned to that name in the same file — the shape used
// where a handler picks between two event types before broadcasting.
// The third result reports whether the name is BOUND in the enclosing function
// (by a parameter, a := or a var). A bound name shadows any package const of
// the same name, so the caller must refuse rather than fall through.
func (s *sseSourceScan) resolveLocalVar(file *ast.File, ident *ast.Ident, depth int) ([]string, bool, bool) {
	name := ident.Name

	// Walk the enclosing scope chain OUTWARD, innermost first, the way Go's
	// lexical scoping resolves a name. Stopping at the innermost function was
	// itself a false negative: a shadowing declaration in an OUTER function
	// with the emit inside a nested closure fell outside the innermost scope,
	// so the name looked unbound and resolution fell through to the package
	// const map — certifying the site with the const's value while Go bound it
	// to the outer local. The first scope that binds the name wins; only a
	// name bound by NO enclosing scope may reach the const map.
	//
	// Scope the search to the function the identifier actually appears in.
	// Collecting assignments from anywhere in the FILE resolves one function's
	// identifier from an unrelated function's local variable — and because this
	// returns found=true, the site is not merely missed, it is CERTIFIED with a
	// value it does not have. web/handler.go is 5,000 lines and already declares
	// `eventType :=`, so any new handler there taking an EventType parameter
	// would inherit that false clearance.
	scopes := enclosingFuncChain(file, ident.Pos())
	if len(scopes) == 0 {
		return nil, false, false
	}

	var enclosing ast.Node
	for _, scope := range scopes {
		// A parameter, receiver or named result is supplied by the CALLER. Its
		// value is not knowable here, so refuse it outright rather than
		// resolving it from an assignment that happens to share the name.
		if isFuncScopedName(scope, name) {
			return nil, false, true
		}
		if scopeBindsName(scope, name) {
			enclosing = scope
			break
		}
	}
	if enclosing == nil {
		return nil, false, false
	}

	var values []string
	found := false
	resolvedAll := true

	collect := func(lhs []ast.Expr, rhs []ast.Expr) {
		for i, target := range lhs {
			ident, ok := target.(*ast.Ident)
			if !ok || ident.Name != name || i >= len(rhs) {
				continue
			}
			found = true
			resolved, ok := s.resolve(file, rhs[i], depth+1)
			if !ok {
				resolvedAll = false
				continue
			}
			values = append(values, resolved...)
		}
	}

	ast.Inspect(enclosing, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.AssignStmt:
			collect(n.Lhs, n.Rhs)
		case *ast.ValueSpec:
			names := make([]ast.Expr, 0, len(n.Names))
			for _, ident := range n.Names {
				names = append(names, ident)
			}
			collect(names, n.Values)
		}
		return true
	})

	if !found {
		return nil, false, false
	}
	return values, resolvedAll, true
}

// builtEventDecl identifies ONE declaration of an SSEEvent variable. It carries
// the declaring scope so an assignment can be matched to the declaration it
// really refers to rather than to any same-named variable elsewhere in the file.
type builtEventDecl struct {
	name  string
	scope ast.Node
	pos   token.Pos
}

// declScopeOf returns the innermost function containing pos, which is the scope
// a local declaration belongs to.
func declScopeOf(file *ast.File, pos token.Pos) ast.Node {
	chain := enclosingFuncChain(file, pos)
	if len(chain) == 0 {
		return nil
	}
	return chain[0]
}

// enclosingFuncChain returns every function declaration or literal containing
// pos, INNERMOST FIRST. A closure and each function enclosing it are all real
// scopes for name resolution, so a caller looking for the binding of a name has
// to consider them in that order rather than only the innermost.
func enclosingFuncChain(file *ast.File, pos token.Pos) []ast.Node {
	var chain []ast.Node
	ast.Inspect(file, func(n ast.Node) bool {
		switch n.(type) {
		case *ast.FuncDecl, *ast.FuncLit:
		default:
			return true
		}
		if pos < n.Pos() || pos > n.End() {
			return true
		}
		chain = append(chain, n)
		return true
	})
	sort.Slice(chain, func(i, j int) bool { return chain[i].Pos() > chain[j].Pos() })
	return chain
}

// scopeBindsName reports whether this function scope declares name at all, by
// := or var. It is deliberately coarse: a binding anywhere in the scope counts,
// including inside a sibling closure. That over-approximates, which refuses
// rather than certifies — the safe direction for a guard, since the cost is a
// loud failure on an odd-but-correct shape rather than a silent pass on a
// wrong one.
func scopeBindsName(scope ast.Node, name string) bool {
	bound := false
	ast.Inspect(scope, func(n ast.Node) bool {
		if bound {
			return false
		}
		switch decl := n.(type) {
		case *ast.AssignStmt:
			for _, lhs := range decl.Lhs {
				if ident, ok := lhs.(*ast.Ident); ok && ident.Name == name {
					bound = true
					return false
				}
			}
		case *ast.ValueSpec:
			for _, ident := range decl.Names {
				if ident.Name == name {
					bound = true
					return false
				}
			}
		}
		return true
	})
	return bound
}

// isFuncScopedName reports whether name is bound by the function's signature —
// a parameter, a receiver, or a named result. Those are caller-supplied and
// therefore unknowable from this side of the call.
func isFuncScopedName(fn ast.Node, name string) bool {
	var sig *ast.FuncType
	var recv *ast.FieldList
	switch f := fn.(type) {
	case *ast.FuncDecl:
		sig, recv = f.Type, f.Recv
	case *ast.FuncLit:
		sig = f.Type
	default:
		return false
	}
	for _, list := range []*ast.FieldList{recv, sig.Params, sig.Results} {
		if list == nil {
			continue
		}
		for _, field := range list.List {
			for _, ident := range field.Names {
				if ident.Name == name {
					return true
				}
			}
		}
	}
	return false
}

// isCalled reports whether a call's function expression names fn, written either
// from inside package web or through the package qualifier.
func isCalled(expr ast.Expr, fn string) bool {
	switch t := expr.(type) {
	case *ast.Ident:
		return t.Name == fn
	case *ast.SelectorExpr:
		return t.Sel.Name == fn
	}
	return false
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	dir, err := filepath.Abs("..")
	require.NoError(t, err)
	for {
		if _, statErr := os.Stat(filepath.Join(dir, "go.mod")); statErr == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		require.NotEqual(t, dir, parent, "no go.mod found above the web package")
		dir = parent
	}
}

func relPos(root string, fset *token.FileSet, pos token.Pos) string {
	position := fset.Position(pos)
	if rel, err := filepath.Rel(root, position.Filename); err == nil {
		position.Filename = rel
	}
	return position.String()
}

func eventTypeStrings(types []EventType) []string {
	out := make([]string, 0, len(types))
	for _, t := range types {
		out = append(out, string(t))
	}
	return out
}

// assertSameEventSet compares two event-name sets exactly in both directions. A
// containment check would let a wrong-but-similar name ("pipeline_sent") pass
// alongside the right one, so equality of the deduplicated, sorted sets is the
// contract.
func assertSameEventSet(t *testing.T, want, got []string, wantLabel, gotLabel, detail string) {
	t.Helper()
	wantSet := uniqueSorted(want)
	gotSet := uniqueSorted(got)
	if assert.ObjectsAreEqual(wantSet, gotSet) {
		return
	}
	if detail != "" {
		detail = "\n" + detail
	}
	assert.Equal(t, wantSet, gotSet,
		"SSE event wiring is inconsistent.\nmissing from %s: %v\nmissing from %s: %v%s",
		gotLabel, difference(wantSet, gotSet),
		wantLabel, difference(gotSet, wantSet),
		detail)
}

func assertNoDuplicates(t *testing.T, names []string, label string) {
	t.Helper()
	seen := map[string]bool{}
	for _, name := range names {
		assert.False(t, seen[name], "%s lists %q more than once", label, name)
		seen[name] = true
	}
}

func uniqueSorted(names []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(names))
	for _, name := range names {
		if seen[name] {
			continue
		}
		seen[name] = true
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func difference(from, remove []string) []string {
	excluded := map[string]bool{}
	for _, name := range remove {
		excluded[name] = true
	}
	out := []string{}
	for _, name := range from {
		if !excluded[name] {
			out = append(out, name)
		}
	}
	return out
}

// stripJSComments removes // line comments and /* */ block comments so a
// structural assertion cannot be satisfied by code that has been commented out.
// String-literal awareness is deliberately omitted: sse.js contains no string
// holding a comment marker, and the check is asserting the PRESENCE of live
// code, so an over-eager strip can only make the assertion stricter.
func stripJSComments(src string) string {
	var b strings.Builder
	for i := 0; i < len(src); {
		if strings.HasPrefix(src[i:], "//") {
			end := strings.IndexByte(src[i:], '\n')
			if end < 0 {
				break
			}
			i += end
			continue
		}
		if strings.HasPrefix(src[i:], "/*") {
			end := strings.Index(src[i+2:], "*/")
			if end < 0 {
				break
			}
			i += 2 + end + 2
			continue
		}
		b.WriteByte(src[i])
		i++
	}
	return b.String()
}

// callerFilesOf returns the sorted, repo-relative production files that call
// fn, excluding the file that declares it and all test files. It walks the tree
// rather than grepping so a caller added in a new package cannot slip past.
func callerFilesOf(t *testing.T, fn string) []string {
	t.Helper()
	root := moduleRoot(t)
	seen := map[string]bool{}
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			switch info.Name() {
			case ".git", "node_modules", "target", "dist":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		fset := token.NewFileSet()
		file, parseErr := parser.ParseFile(fset, path, nil, 0)
		if parseErr != nil {
			return nil
		}
		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return nil
		}
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || !isCalled(call.Fun, fn) {
				return true
			}
			// Exempt the declaration ITSELF, by position — not every call that
			// happens to share a file with it. web.EventTypeFromREST is declared
			// in web/sse.go, the same file that owns SSEBroadcaster.Broadcast, so
			// a file-wide exemption blinds this check to a bridge call sitting
			// directly next to the broadcaster.
			inDeclaration := false
			for _, decl := range file.Decls {
				fd, isFunc := decl.(*ast.FuncDecl)
				if !isFunc || fd.Name.Name != fn {
					continue
				}
				if call.Pos() >= fd.Pos() && call.End() <= fd.End() {
					inDeclaration = true
					break
				}
			}
			if inDeclaration {
				return true
			}
			seen[filepath.ToSlash(rel)] = true
			return true
		})
		return nil
	})
	require.NoError(t, err)

	out := make([]string, 0, len(seen))
	for f := range seen {
		out = append(out, f)
	}
	sort.Strings(out)
	return out
}

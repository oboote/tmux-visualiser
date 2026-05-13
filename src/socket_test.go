package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func stubLisaSockets(t *testing.T, paths []string, err error) {
	t.Helper()
	origList := listLisaSocketPathsFn
	t.Cleanup(func() {
		listLisaSocketPathsFn = origList
	})
	listLisaSocketPathsFn = func(config) ([]string, error) {
		return paths, err
	}
	lisaSocketCache.mu.Lock()
	lisaSocketCache.at = time.Time{}
	lisaSocketCache.paths = nil
	lisaSocketCache.errText = ""
	lisaSocketCache.mu.Unlock()
}

func stubLisaSessionList(t *testing.T, fn func(context.Context, bool) ([]byte, error)) {
	t.Helper()
	orig := runLisaSessionListFn
	t.Cleanup(func() {
		runLisaSessionListFn = orig
	})
	runLisaSessionListFn = fn
}

func TestDiscoverSocketTargets(t *testing.T) {
	t.Setenv("TMUX", "")
	stubLisaSockets(t, []string{}, nil)

	tmpDir := t.TempDir()
	socketA := filepath.Join(tmpDir, "a.sock")
	socketB := filepath.Join(tmpDir, "b.sock")
	if err := os.WriteFile(socketA, []byte("a"), 0o600); err != nil {
		t.Fatalf("write socketA: %v", err)
	}
	if err := os.WriteFile(socketB, []byte("b"), 0o600); err != nil {
		t.Fatalf("write socketB: %v", err)
	}

	cfg := config{
		includeDefaultSocket: true,
		includeLisaSockets:   true,
		socketGlob:           filepath.Join(tmpDir, "*.sock"),
		explicitSockets: []string{
			socketA,
			filepath.Join(tmpDir, "missing.sock"),
			socketA,
		},
	}

	targets, discoveryErrors := discoverSocketTargets(cfg)
	if len(discoveryErrors) != 0 {
		t.Fatalf("unexpected discoveryErrors: %v", discoveryErrors)
	}
	if len(targets) != 4 {
		t.Fatalf("targets len = %d", len(targets))
	}

	got := []string{targets[0].path, targets[1].path, targets[2].path, targets[3].path}
	want := []string{"", filepath.Clean(socketA), filepath.Clean(filepath.Join(tmpDir, "missing.sock")), filepath.Clean(socketB)}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("targets = %v, want %v", got, want)
	}
}

func TestDiscoverSocketTargetsInvalidGlob(t *testing.T) {
	t.Setenv("TMUX", "")
	stubLisaSockets(t, []string{}, nil)

	cfg := config{
		includeLisaSockets: true,
		socketGlob:         "[",
	}

	targets, discoveryErrors := discoverSocketTargets(cfg)
	if len(targets) != 0 {
		t.Fatalf("targets len = %d", len(targets))
	}
	if len(discoveryErrors) != 1 {
		t.Fatalf("discoveryErrors len = %d", len(discoveryErrors))
	}
	if !strings.Contains(discoveryErrors[0], "socket-glob") {
		t.Fatalf("discovery error = %q", discoveryErrors[0])
	}
}

func TestListSessionsReportsUnavailableSocketHints(t *testing.T) {
	t.Setenv("TMUX", "")

	missingSocket := "/tmp/missing-explicit.sock"
	origRun := runTmuxOnSocketFn
	t.Cleanup(func() {
		runTmuxOnSocketFn = origRun
	})
	runTmuxOnSocketFn = func(_ context.Context, _ config, _ string, _ ...string) (string, error) {
		return "", errors.New("failed to connect to server")
	}

	cfg := config{
		includeDefaultSocket: false,
		includeLisaSockets:   false,
		explicitSockets:      []string{missingSocket},
	}
	_, socketCount, err := listSessions(context.Background(), cfg)
	if err == nil {
		t.Fatalf("expected error")
	}
	if socketCount != 1 {
		t.Fatalf("socketCount = %d", socketCount)
	}
	if !strings.Contains(err.Error(), "missing-explicit") {
		t.Fatalf("error = %q", err.Error())
	}
}

func TestListSessionsQualifiedKeyCollision(t *testing.T) {
	t.Setenv("TMUX", "")

	tmpDir := t.TempDir()
	socketA := filepath.Join(tmpDir, "project.sock")
	if err := os.WriteFile(socketA, []byte("x"), 0o600); err != nil {
		t.Fatalf("write socketA: %v", err)
	}

	origRun := runTmuxOnSocketFn
	t.Cleanup(func() {
		runTmuxOnSocketFn = origRun
	})
	runTmuxOnSocketFn = func(_ context.Context, _ config, socket string, args ...string) (string, error) {
		if len(args) < 1 || args[0] != "list-sessions" {
			return "", errors.New("unexpected command")
		}
		if socket == "" {
			return "alpha\nbeta", nil
		}
		if socket == socketA {
			return "alpha", nil
		}
		return "", errors.New("unknown socket")
	}

	cfg := config{
		includeDefaultSocket: true,
		includeLisaSockets:   false,
		explicitSockets:      []string{socketA},
	}
	refs, socketCount, err := listSessions(context.Background(), cfg)
	if err != nil {
		t.Fatalf("listSessions err: %v", err)
	}
	if socketCount != 2 {
		t.Fatalf("socketCount = %d", socketCount)
	}
	if len(refs) != 3 {
		t.Fatalf("refs len = %d", len(refs))
	}

	keys := map[string]bool{}
	for _, ref := range refs {
		keys[ref.key] = true
	}
	if !keys[sessionQualifiedKey("", "alpha")] {
		t.Fatalf("missing default alpha key")
	}
	if !keys[sessionQualifiedKey(socketA, "alpha")] {
		t.Fatalf("missing socket alpha key")
	}
}

func TestListSessionsReturnsPartialResultsWithFatalErrors(t *testing.T) {
	t.Setenv("TMUX", "")

	badSocket := "/tmp/private.sock"
	origRun := runTmuxOnSocketFn
	t.Cleanup(func() {
		runTmuxOnSocketFn = origRun
	})
	runTmuxOnSocketFn = func(_ context.Context, _ config, socket string, args ...string) (string, error) {
		if len(args) < 1 || args[0] != "list-sessions" {
			return "", errors.New("unexpected command")
		}
		if socket == "" {
			return "alpha", nil
		}
		if socket == badSocket {
			return "", errors.New("permission denied")
		}
		return "", errors.New("unknown socket")
	}

	cfg := config{
		includeDefaultSocket: true,
		includeLisaSockets:   false,
		explicitSockets:      []string{badSocket},
	}
	refs, socketCount, err := listSessions(context.Background(), cfg)
	if err == nil {
		t.Fatalf("expected partial error")
	}
	if !strings.Contains(err.Error(), "partial socket failures") {
		t.Fatalf("error = %q", err.Error())
	}
	if !strings.Contains(err.Error(), "private: permission denied") {
		t.Fatalf("error = %q", err.Error())
	}
	if socketCount != 2 {
		t.Fatalf("socketCount = %d", socketCount)
	}
	if len(refs) != 1 {
		t.Fatalf("refs len = %d", len(refs))
	}
	if refs[0].key != sessionQualifiedKey("", "alpha") {
		t.Fatalf("unexpected key = %q", refs[0].key)
	}
}

func TestListSessionsReturnsPartialResultsWithTmuxPermissionError(t *testing.T) {
	t.Setenv("TMUX", "")

	badSocket := "/tmp/private.sock"
	origRun := runTmuxOnSocketFn
	t.Cleanup(func() {
		runTmuxOnSocketFn = origRun
	})
	runTmuxOnSocketFn = func(_ context.Context, _ config, socket string, args ...string) (string, error) {
		if len(args) < 1 || args[0] != "list-sessions" {
			return "", errors.New("unexpected command")
		}
		if socket == "" {
			return "alpha", nil
		}
		if socket == badSocket {
			return "", errors.New("error connecting to /tmp/private.sock (Permission denied)")
		}
		return "", errors.New("unknown socket")
	}

	cfg := config{
		includeDefaultSocket: true,
		includeLisaSockets:   false,
		explicitSockets:      []string{badSocket},
	}
	refs, socketCount, err := listSessions(context.Background(), cfg)
	if err == nil {
		t.Fatalf("expected partial error")
	}
	if !strings.Contains(err.Error(), "partial socket failures") {
		t.Fatalf("error = %q", err.Error())
	}
	if !strings.Contains(err.Error(), "private: error connecting to /tmp/private.sock (Permission denied)") {
		t.Fatalf("error = %q", err.Error())
	}
	if socketCount != 2 {
		t.Fatalf("socketCount = %d", socketCount)
	}
	if len(refs) != 1 {
		t.Fatalf("refs len = %d", len(refs))
	}
	if refs[0].key != sessionQualifiedKey("", "alpha") {
		t.Fatalf("unexpected key = %q", refs[0].key)
	}
}

func TestListSessionsIncludesSocketGlobDiscoveryError(t *testing.T) {
	t.Setenv("TMUX", "")
	stubLisaSockets(t, []string{}, nil)

	origRun := runTmuxOnSocketFn
	t.Cleanup(func() {
		runTmuxOnSocketFn = origRun
	})
	runTmuxOnSocketFn = func(_ context.Context, _ config, socket string, args ...string) (string, error) {
		if socket == "" && len(args) > 0 && args[0] == "list-sessions" {
			return "alpha", nil
		}
		return "", errors.New("unexpected command")
	}

	cfg := config{
		includeDefaultSocket: true,
		includeLisaSockets:   true,
		socketGlob:           "[",
	}
	refs, socketCount, err := listSessions(context.Background(), cfg)
	if err == nil {
		t.Fatalf("expected partial error")
	}
	if !strings.Contains(err.Error(), "partial socket failures") {
		t.Fatalf("error = %q", err.Error())
	}
	if !strings.Contains(err.Error(), "socket-glob") {
		t.Fatalf("error = %q", err.Error())
	}
	if socketCount != 1 {
		t.Fatalf("socketCount = %d", socketCount)
	}
	if len(refs) != 1 {
		t.Fatalf("refs len = %d", len(refs))
	}
}

func TestUpdateStateKeepsPartialSessionsOnSocketError(t *testing.T) {
	t.Setenv("TMUX", "")

	badSocket := "/tmp/private.sock"
	origRun := runTmuxOnSocketFn
	t.Cleanup(func() {
		runTmuxOnSocketFn = origRun
	})
	runTmuxOnSocketFn = func(_ context.Context, _ config, socket string, args ...string) (string, error) {
		switch args[0] {
		case "list-sessions":
			if socket == "" {
				return "alpha", nil
			}
			if socket == badSocket {
				return "", errors.New("permission denied")
			}
		case "list-panes":
			if socket == "" {
				return "1 %1", nil
			}
		case "capture-pane":
			if socket == "" {
				return "line1\n", nil
			}
		}
		return "", errors.New("unexpected command")
	}

	state := appState{
		sessions: map[string]sessionView{},
		scroll:   map[string]int{},
		follow:   map[string]bool{},
	}
	cfg := config{
		lines:                50,
		maxWorkers:           1,
		includeDefaultSocket: true,
		includeLisaSockets:   false,
		explicitSockets:      []string{badSocket},
	}
	updateState(context.Background(), &state, cfg)

	if state.serverDown {
		t.Fatalf("serverDown should be false")
	}
	if !strings.Contains(state.lastErr, "partial socket failures") {
		t.Fatalf("lastErr = %q", state.lastErr)
	}
	if len(state.sessions) != 1 {
		t.Fatalf("sessions len = %d", len(state.sessions))
	}
	if _, ok := state.sessions[sessionQualifiedKey("", "alpha")]; !ok {
		t.Fatalf("missing default alpha session")
	}
}

func TestDiscoverSocketTargetsIncludesLisaSocketsFromHelper(t *testing.T) {
	t.Setenv("TMUX", "")

	tmpDir := t.TempDir()
	socketA := filepath.Join(tmpDir, "a.sock")
	if err := os.WriteFile(socketA, []byte("a"), 0o600); err != nil {
		t.Fatalf("write socketA: %v", err)
	}
	socketFromLisa := filepath.Join(tmpDir, "from-lisa.sock")
	stubLisaSockets(t, []string{socketFromLisa}, nil)

	cfg := config{
		includeDefaultSocket: true,
		includeLisaSockets:   true,
		socketGlob:           filepath.Join(tmpDir, "*.sock"),
	}
	targets, discoveryErrors := discoverSocketTargets(cfg)
	if len(discoveryErrors) != 0 {
		t.Fatalf("unexpected discoveryErrors: %v", discoveryErrors)
	}
	keys := make([]string, 0, len(targets))
	for _, target := range targets {
		keys = append(keys, target.key)
	}
	if !containsString(keys, socketKey(socketFromLisa)) {
		t.Fatalf("missing lisa helper socket target: %v", keys)
	}
}

func TestListLisaSocketPathsFromLISAFallsBackWhenWithNextActionUnsupported(t *testing.T) {
	cfg := config{cmdTimeout: 2 * time.Second}
	unknownFlag := []byte(`{"error":"unknown flag: --with-next-action","errorCode":"unknown_flag","ok":false}`)
	okPayload := []byte(`{"items":[{"projectRoot":"/tmp/proj-a"}]}`)
	calls := make([]bool, 0, 2)
	stubLisaSessionList(t, func(_ context.Context, withNextAction bool) ([]byte, error) {
		calls = append(calls, withNextAction)
		if withNextAction {
			return unknownFlag, errors.New("exit status 1")
		}
		return okPayload, nil
	})
	got, err := listLisaSocketPathsFromLISA(cfg)
	if err != nil {
		t.Fatalf("listLisaSocketPathsFromLISA err: %v", err)
	}
	if !reflect.DeepEqual(calls, []bool{true, false}) {
		t.Fatalf("calls = %v", calls)
	}
	root := canonicalProjectRoot("/tmp/proj-a")
	want := dedupePaths([]string{
		tmuxSocketPathForProjectRoot(root),
		tmuxLegacySocketPathForProjectRoot(root),
	})
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got = %v, want %v", got, want)
	}
}

func TestListLisaSocketPathsFromLISAOldPayloadWithoutItemsReturnsEmpty(t *testing.T) {
	cfg := config{cmdTimeout: 2 * time.Second}
	stubLisaSessionList(t, func(_ context.Context, _ bool) ([]byte, error) {
		return []byte(`{"count":2,"sessions":["a","b"]}`), nil
	})
	got, err := listLisaSocketPathsFromLISA(cfg)
	if err != nil {
		t.Fatalf("listLisaSocketPathsFromLISA err: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected no socket paths, got %v", got)
	}
}

func TestListLisaSocketPathsFromLISAUsesSocketPathFieldWhenPresent(t *testing.T) {
	cfg := config{cmdTimeout: 2 * time.Second}
	stubLisaSessionList(t, func(_ context.Context, _ bool) ([]byte, error) {
		return []byte(`{"items":[{"projectRoot":"/tmp/proj-a","socketPath":"/tmp/custom-a.sock"},{"projectRoot":"/tmp/proj-b"}]}`), nil
	})
	got, err := listLisaSocketPathsFromLISA(cfg)
	if err != nil {
		t.Fatalf("listLisaSocketPathsFromLISA err: %v", err)
	}
	rootB := canonicalProjectRoot("/tmp/proj-b")
	want := dedupePaths([]string{
		"/tmp/custom-a.sock",
		tmuxSocketPathForProjectRoot(rootB),
		tmuxLegacySocketPathForProjectRoot(rootB),
	})
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got = %v, want %v", got, want)
	}
}

func TestListLisaSocketPathsFromProcessTableIncludesNonLisaNamedSockets(t *testing.T) {
	origList := listProcessCommandsFn
	t.Cleanup(func() {
		listProcessCommandsFn = origList
	})
	listProcessCommandsFn = func() ([]string, error) {
		return []string{
			"tmux -S /tmp/custom.sock list-sessions",
			"tmux -S /tmp/lisa-a.sock list-sessions",
		}, nil
	}
	got, err := listLisaSocketPathsFromProcessTable()
	if err != nil {
		t.Fatalf("listLisaSocketPathsFromProcessTable err: %v", err)
	}
	want := []string{"/tmp/custom.sock", "/tmp/lisa-a.sock"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got = %v, want %v", got, want)
	}
}

func TestExtractTmuxSocketPathsFromCommands(t *testing.T) {
	commands := []string{
		"/opt/homebrew/bin/tmux -S /tmp/lisa-a.sock new -d",
		"tmux -S /tmp/tmux-1000/default list-sessions",
		"/usr/bin/tmux -L dev list-sessions",
		"/usr/bin/tmux -S /tmp/lisa-b.sock has-session -t x",
		"zsh -lc echo hi",
	}
	got := extractTmuxSocketPathsFromCommands(commands)
	want := []string{"/tmp/lisa-a.sock", "/tmp/tmux-1000/default", "/tmp/lisa-b.sock"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected socket extraction; got=%v want=%v", got, want)
	}
}

func TestLisaSocketGlobsCustomOverridesFallbacks(t *testing.T) {
	custom := "/tmp/custom-*.sock"
	got := lisaSocketGlobs(custom)
	want := []string{custom}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected globs; got=%v want=%v", got, want)
	}
}

func TestDiscoverSocketTargetsDedupesSymlinkedSocketAliases(t *testing.T) {
	t.Setenv("TMUX", "")
	stubLisaSockets(t, []string{}, nil)

	realDir := t.TempDir()
	linkDir := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(realDir, linkDir); err != nil {
		t.Skipf("symlink unsupported: %v", err)
	}

	socketName := "same.sock"
	realSocket := filepath.Join(realDir, socketName)
	aliasSocket := filepath.Join(linkDir, socketName)
	if err := os.WriteFile(realSocket, []byte("x"), 0o600); err != nil {
		t.Fatalf("write real socket: %v", err)
	}

	cfg := config{
		includeDefaultSocket: false,
		includeLisaSockets:   false,
		explicitSockets:      []string{realSocket, aliasSocket},
	}
	targets, discoveryErrors := discoverSocketTargets(cfg)
	if len(discoveryErrors) != 0 {
		t.Fatalf("unexpected discoveryErrors: %v", discoveryErrors)
	}
	if len(targets) != 1 {
		t.Fatalf("targets len = %d, targets=%v", len(targets), targets)
	}
}

func containsString(items []string, needle string) bool {
	for _, item := range items {
		if item == needle {
			return true
		}
	}
	return false
}

func TestSocketUsedForListPaneCapture(t *testing.T) {
	t.Setenv("TMUX", "")

	socketPath := "/tmp/test.sock"
	calls := make([]string, 0)

	origRun := runTmuxOnSocketFn
	t.Cleanup(func() {
		runTmuxOnSocketFn = origRun
	})
	runTmuxOnSocketFn = func(_ context.Context, _ config, socket string, args ...string) (string, error) {
		calls = append(calls, socket+"|"+strings.Join(args, " "))
		switch args[0] {
		case "list-sessions":
			return "alpha", nil
		case "list-panes":
			return "0 %2\n1 %1", nil
		case "capture-pane":
			return "line1\nline2\n", nil
		default:
			return "", errors.New("unexpected command")
		}
	}

	ctx := context.Background()
	cfg := config{}
	target := makeSocketTarget(socketPath)
	refs, err := listSessionsOnSocket(ctx, cfg, target)
	if err != nil {
		t.Fatalf("listSessionsOnSocket err: %v", err)
	}
	if len(refs) != 1 || refs[0].key != sessionQualifiedKey(socketPath, "alpha") {
		t.Fatalf("refs = %#v", refs)
	}

	paneID, err := activePaneID(ctx, cfg, socketPath, "alpha")
	if err != nil {
		t.Fatalf("activePaneID err: %v", err)
	}
	if paneID != "%1" {
		t.Fatalf("paneID = %q", paneID)
	}

	lines, err := capturePane(ctx, cfg, socketPath, paneID, 20)
	if err != nil {
		t.Fatalf("capturePane err: %v", err)
	}
	if len(lines) != 2 || lines[0] != "line1" || lines[1] != "line2" {
		t.Fatalf("lines = %v", lines)
	}

	for _, call := range calls {
		if !strings.HasPrefix(call, socketPath+"|") {
			t.Fatalf("socket mismatch call: %q", call)
		}
	}
}

func TestDiscoverSocketTargetsIncludesCurrentTMUXSocket(t *testing.T) {
	t.Setenv("TMUX", "/tmp/lisa-a.sock,17,0")

	cfg := config{
		includeDefaultSocket: true,
		includeLisaSockets:   false,
	}

	targets, discoveryErrors := discoverSocketTargets(cfg)
	if len(discoveryErrors) != 0 {
		t.Fatalf("unexpected discoveryErrors: %v", discoveryErrors)
	}
	if len(targets) != 2 {
		t.Fatalf("targets len = %d", len(targets))
	}
	if targets[0].path != "" {
		t.Fatalf("expected first target to be default, got %q", targets[0].path)
	}
	if targets[1].path != "/tmp/lisa-a.sock" {
		t.Fatalf("expected env socket target, got %q", targets[1].path)
	}
}

func TestListSessionsFallsBackToCurrentTMUXSocket(t *testing.T) {
	t.Setenv("TMUX", "/tmp/lisa-b.sock,1,0")

	origRun := runTmuxOnSocketFn
	t.Cleanup(func() {
		runTmuxOnSocketFn = origRun
	})
	runTmuxOnSocketFn = func(_ context.Context, _ config, socket string, args ...string) (string, error) {
		if len(args) < 1 || args[0] != "list-sessions" {
			return "", errors.New("unexpected command")
		}
		if socket == "" {
			return "", errors.New("no server running on /tmp/tmux-1000/default")
		}
		if socket == "/tmp/lisa-b.sock" {
			return "alpha", nil
		}
		return "", errors.New("unknown socket")
	}

	cfg := config{
		includeDefaultSocket: true,
		includeLisaSockets:   false,
	}
	refs, socketCount, err := listSessions(context.Background(), cfg)
	if err != nil {
		t.Fatalf("listSessions err: %v", err)
	}
	if socketCount != 2 {
		t.Fatalf("socketCount = %d", socketCount)
	}
	if len(refs) != 1 {
		t.Fatalf("refs len = %d", len(refs))
	}
	if refs[0].key != sessionQualifiedKey("/tmp/lisa-b.sock", "alpha") {
		t.Fatalf("unexpected key = %q", refs[0].key)
	}
}

func TestPaneQualifiedKey(t *testing.T) {
	got := paneQualifiedKey("/tmp/a.sock", "alpha", "%3")
	want := sessionQualifiedKey("/tmp/a.sock", "alpha") + "::%3"
	if got != want {
		t.Fatalf("paneQualifiedKey = %q, want %q", got, want)
	}

	got = paneQualifiedKey("/tmp/a.sock", "alpha", "")
	want = sessionQualifiedKey("/tmp/a.sock", "alpha")
	if got != want {
		t.Fatalf("paneQualifiedKey fallback = %q, want %q", got, want)
	}
}

func TestCapturePaneFallsBackToAlternateScreen(t *testing.T) {
	origRun := runTmuxOnSocketFn
	t.Cleanup(func() {
		runTmuxOnSocketFn = origRun
	})

	calls := make([]string, 0, 2)
	runTmuxOnSocketFn = func(_ context.Context, _ config, socket string, args ...string) (string, error) {
		calls = append(calls, socket+"|"+strings.Join(args, " "))
		if len(args) == 0 || args[0] != "capture-pane" {
			return "", errors.New("unexpected command")
		}
		if len(args) > 1 && args[1] == "-a" {
			return "alt-line-1\nalt-line-2\n", nil
		}
		return "\n", nil
	}

	lines, err := capturePane(context.Background(), config{}, "/tmp/test.sock", "%1", 80)
	if err != nil {
		t.Fatalf("capturePane err: %v", err)
	}
	want := []string{"alt-line-1", "alt-line-2"}
	if !reflect.DeepEqual(lines, want) {
		t.Fatalf("lines = %v, want %v", lines, want)
	}
	if len(calls) != 2 {
		t.Fatalf("calls len = %d", len(calls))
	}
	if !strings.Contains(calls[1], "capture-pane -a -t %1 -p -e -S -80") {
		t.Fatalf("expected alternate-screen capture fallback, got %q", calls[1])
	}
}

func TestListSessionsOnSocketAllPanes(t *testing.T) {
	socketPath := "/tmp/test-all-panes.sock"

	origRun := runTmuxOnSocketFn
	t.Cleanup(func() {
		runTmuxOnSocketFn = origRun
	})
	runTmuxOnSocketFn = func(_ context.Context, _ config, socket string, args ...string) (string, error) {
		if socket != socketPath {
			return "", errors.New("unexpected socket")
		}
		switch args[0] {
		case "list-sessions":
			return "alpha\nbeta", nil
		case "list-panes":
			switch args[2] {
			case "alpha:":
				return "%1\n%3", nil
			case "beta:":
				return "%7", nil
			default:
				return "", errors.New("unexpected session")
			}
		default:
			return "", errors.New("unexpected command")
		}
	}

	cfg := config{allPanes: true}
	target := makeSocketTarget(socketPath)
	refs, err := listSessionsOnSocket(context.Background(), cfg, target)
	if err != nil {
		t.Fatalf("listSessionsOnSocket err: %v", err)
	}
	if len(refs) != 3 {
		t.Fatalf("refs len = %d", len(refs))
	}

	keys := map[string]bool{}
	panes := map[string]bool{}
	for _, ref := range refs {
		keys[ref.key] = true
		panes[ref.paneID] = true
	}
	if !keys[paneQualifiedKey(socketPath, "alpha", "%1")] {
		t.Fatalf("missing alpha %%1 key")
	}
	if !keys[paneQualifiedKey(socketPath, "alpha", "%3")] {
		t.Fatalf("missing alpha %%3 key")
	}
	if !keys[paneQualifiedKey(socketPath, "beta", "%7")] {
		t.Fatalf("missing beta %%7 key")
	}
	if !panes["%1"] || !panes["%3"] || !panes["%7"] {
		t.Fatalf("unexpected pane ids: %#v", panes)
	}
}

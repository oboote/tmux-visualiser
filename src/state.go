package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

func updateState(ctx context.Context, state *appState, cfg config) {
	refs, socketCount, err := listSessions(ctx, cfg)
	state.lastRefresh = time.Now()
	state.socketCount = socketCount
	if err != nil {
		state.lastErr = err.Error()
		if refs == nil && isSocketUnavailableError(err) {
			state.serverDown = true
			state.sessions = map[string]sessionView{}
			state.scroll = map[string]int{}
			state.follow = map[string]bool{}
			state.focusIndex = 0
			state.focusName = ""
			return
		}
		if refs == nil {
			state.serverDown = false
			return
		}
	} else {
		state.lastErr = ""
	}
	state.serverDown = false

	newSessions := make(map[string]sessionView, len(refs))
	keepScroll := make(map[string]int, len(refs))
	keepFollow := make(map[string]bool, len(refs))
	if len(refs) == 0 {
		state.sessions = newSessions
		state.scroll = keepScroll
		state.follow = keepFollow
		state.focusIndex = 0
		state.focusName = ""
		return
	}

	workers := cfg.maxWorkers
	if workers > len(refs) {
		workers = len(refs)
	}
	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, ref := range refs {
		ref := ref
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			paneID := ref.paneID
			if paneID == "" {
				var err error
				paneID, err = activePaneID(ctx, cfg, ref.socket.path, ref.name)
				if err != nil {
					mu.Lock()
					newSessions[ref.key] = sessionView{
						key:        ref.key,
						name:       ref.name,
						socketPath: ref.socket.path,
						socketHint: ref.socket.hint,
						paneID:     "",
						lines:      []string{err.Error()},
						updated:    time.Now(),
					}
					mu.Unlock()
					return
				}
			}

			lines, err := capturePane(ctx, cfg, ref.socket.path, paneID, cfg.lines)
			if err != nil {
				lines = []string{err.Error()}
			}

			mu.Lock()
			newSessions[ref.key] = sessionView{
				key:        ref.key,
				name:       ref.name,
				socketPath: ref.socket.path,
				socketHint: ref.socket.hint,
				paneID:     paneID,
				lines:      lines,
				updated:    time.Now(),
			}
			mu.Unlock()
		}()
	}

	wg.Wait()
	state.sessions = newSessions
	for _, ref := range refs {
		key := ref.key
		keepScroll[key] = state.scroll[key]
		if _, ok := state.follow[key]; ok {
			keepFollow[key] = state.follow[key]
		} else {
			keepFollow[key] = true
		}
	}
	state.scroll = keepScroll
	state.follow = keepFollow
	keys := make([]string, 0, len(refs))
	for _, ref := range refs {
		keys = append(keys, ref.key)
	}
	state.focusIndex = focusIndexForName(keys, state.focusName)
	if state.focusIndex < 0 || state.focusIndex >= len(keys) {
		state.focusIndex = 0
		state.focusName = keys[0]
	} else {
		state.focusName = keys[state.focusIndex]
	}
}

func listSessions(ctx context.Context, cfg config) ([]sessionRef, int, error) {
	targets, discoveryErrors := discoverSocketTargets(cfg)
	if len(targets) == 0 {
		if len(discoveryErrors) > 0 {
			sort.Strings(discoveryErrors)
			return nil, 0, errors.New(strings.Join(discoveryErrors, " | "))
		}
		return nil, 0, errors.New("no tmux sockets configured")
	}

	merged := make([]sessionRef, 0)
	fatalErrors := append([]string{}, discoveryErrors...)
	unavailableTargets := make([]string, 0)
	successCount := 0

	for _, target := range targets {
		refs, err := listSessionsOnSocket(ctx, cfg, target)
		if err != nil {
			if isSocketUnavailableError(err) {
				unavailableTargets = append(unavailableTargets, target.hint)
				continue
			}
			fatalErrors = append(fatalErrors, fmt.Sprintf("%s: %v", target.hint, err))
			continue
		}
		successCount++
		merged = append(merged, refs...)
	}

	if successCount > 0 {
		sort.Slice(merged, func(i, j int) bool {
			if merged[i].name == merged[j].name {
				if merged[i].socket.key == merged[j].socket.key {
					if merged[i].paneID == merged[j].paneID {
						return merged[i].key < merged[j].key
					}
					return merged[i].paneID < merged[j].paneID
				}
				return merged[i].socket.key < merged[j].socket.key
			}
			return merged[i].name < merged[j].name
		})
		if len(fatalErrors) > 0 {
			sort.Strings(fatalErrors)
			return merged, len(targets), errors.New("partial socket failures: " + strings.Join(fatalErrors, " | "))
		}
		return merged, len(targets), nil
	}

	if len(fatalErrors) > 0 {
		sort.Strings(fatalErrors)
		return nil, len(targets), errors.New(strings.Join(fatalErrors, " | "))
	}
	if len(unavailableTargets) > 0 {
		sort.Strings(unavailableTargets)
		return nil, len(targets), errors.New("no server running on discovered sockets: " + strings.Join(unavailableTargets, ", "))
	}

	return []sessionRef{}, len(targets), nil
}

func listSessionsOnSocket(ctx context.Context, cfg config, target socketTarget) ([]sessionRef, error) {
	out, err := runTmuxOnSocketFn(ctx, cfg, target.path, "list-sessions", "-F", "#S")
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(out) == "" {
		return []sessionRef{}, nil
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	refs := make([]sessionRef, 0, len(lines))
	for _, line := range lines {
		sessionName := strings.TrimSpace(line)
		if sessionName == "" {
			continue
		}
		if cfg.allPanes {
			paneIDs, paneErr := listPaneIDs(ctx, cfg, target.path, sessionName)
			if paneErr != nil {
				refs = append(refs, sessionRef{
					key:    sessionQualifiedKey(target.path, sessionName),
					name:   sessionName,
					paneID: "",
					socket: target,
				})
				continue
			}
			if len(paneIDs) == 0 {
				refs = append(refs, sessionRef{
					key:    sessionQualifiedKey(target.path, sessionName),
					name:   sessionName,
					paneID: "",
					socket: target,
				})
				continue
			}
			for _, paneID := range paneIDs {
				refs = append(refs, sessionRef{
					key:    paneQualifiedKey(target.path, sessionName, paneID),
					name:   sessionName,
					paneID: paneID,
					socket: target,
				})
			}
			continue
		}
		refs = append(refs, sessionRef{
			key:    sessionQualifiedKey(target.path, sessionName),
			name:   sessionName,
			paneID: "",
			socket: target,
		})
	}
	return refs, nil
}

func listPaneIDs(ctx context.Context, cfg config, socketPath string, session string) ([]string, error) {
	out, err := runTmuxOnSocketFn(ctx, cfg, socketPath, "list-panes", "-t", session+":", "-F", "#{pane_id}")
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(out) == "" {
		return []string{}, nil
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	paneIDs := make([]string, 0, len(lines))
	seen := make(map[string]struct{}, len(lines))
	for _, line := range lines {
		paneID := strings.TrimSpace(line)
		if paneID == "" {
			continue
		}
		if _, ok := seen[paneID]; ok {
			continue
		}
		seen[paneID] = struct{}{}
		paneIDs = append(paneIDs, paneID)
	}
	return paneIDs, nil
}

func activePaneID(ctx context.Context, cfg config, socketPath string, session string) (string, error) {
	out, err := runTmuxOnSocketFn(ctx, cfg, socketPath, "list-panes", "-t", session+":", "-F", "#{pane_active} #{pane_id}")
	if err != nil {
		return "", err
	}
	var fallback string
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		if fallback == "" {
			fallback = fields[1]
		}
		if fields[0] == "1" {
			return fields[1], nil
		}
	}
	if fallback == "" {
		return "", errors.New("no pane found")
	}
	return fallback, nil
}

func capturePane(ctx context.Context, cfg config, socketPath string, paneID string, lines int) ([]string, error) {
	if lines < 1 {
		lines = 1
	}
	rangeArg := fmt.Sprintf("-%d", lines)
	out, err := runTmuxOnSocketFn(ctx, cfg, socketPath, "capture-pane", "-t", paneID, "-p", "-e", "-S", rangeArg)
	if err != nil {
		return nil, err
	}

	primary := normalizeCaptureOutput(out)
	if hasVisibleCapture(primary) {
		return primary, nil
	}

	altOut, altErr := runTmuxOnSocketFn(ctx, cfg, socketPath, "capture-pane", "-a", "-t", paneID, "-p", "-e", "-S", rangeArg)
	if altErr == nil {
		alt := normalizeCaptureOutput(altOut)
		if len(alt) > 0 {
			return alt, nil
		}
	}

	if len(primary) == 0 {
		return []string{"(empty)"}, nil
	}
	return primary, nil
}

func normalizeCaptureOutput(out string) []string {
	if out == "" {
		return []string{}
	}
	result := strings.Split(out, "\n")
	if len(result) > 0 && result[len(result)-1] == "" {
		result = result[:len(result)-1]
	}
	return result
}

func hasVisibleCapture(lines []string) bool {
	for _, line := range lines {
		if strings.TrimSpace(line) != "" {
			return true
		}
	}
	return false
}

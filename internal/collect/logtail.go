package collect

import (
	"bufio"
	"context"
	"io"
	"os"
	"path/filepath"
	"time"
)

// Tailer polls a file that lives inside mlog's `<baseDir>/latest -> <run
// dir>/` symlink layout (pkg/mlog/mlog.go), following both plain growth and
// rotation: when mithril restarts, `latest` repoints at a new run directory
// and the target file starts over from byte 0. Tailer detects that (path
// change or size shrinking) and restarts from the top of the new file.
//
// Polling (not fsnotify) keeps this dependency-free; mithril's log/JSONL
// volume is low enough that a short interval costs nothing measurable.
type Tailer struct {
	BaseDir      string
	FileName     string // e.g. "mithril.log" or "replay_timings.jsonl"
	PollInterval time.Duration
	OnLine       func(line string)

	resolvedPath string
	offset       int64
	partial      []byte
}

func (t *Tailer) Run(ctx context.Context) {
	if t.PollInterval <= 0 {
		t.PollInterval = time.Second
	}
	ticker := time.NewTicker(t.PollInterval)
	defer ticker.Stop()
	for {
		t.pollOnce()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (t *Tailer) pollOnce() {
	runDir, err := filepath.EvalSymlinks(filepath.Join(t.BaseDir, "latest"))
	if err != nil {
		return // no run yet, or log dir not mounted at this path
	}
	path := filepath.Join(runDir, t.FileName)

	if path != t.resolvedPath {
		t.resolvedPath = path
		t.offset = 0
		t.partial = t.partial[:0]
	}

	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return
	}
	if info.Size() < t.offset {
		// Truncated or rotated out from under us (lumberjack rotation, or a
		// fresh run reusing the same file name) — start over.
		t.offset = 0
		t.partial = t.partial[:0]
	}
	if info.Size() == t.offset {
		return // nothing new
	}

	if _, err := f.Seek(t.offset, io.SeekStart); err != nil {
		return
	}
	r := bufio.NewReaderSize(f, 64*1024)
	for {
		chunk, err := r.ReadBytes('\n')
		if len(chunk) > 0 {
			t.offset += int64(len(chunk))
			if chunk[len(chunk)-1] == '\n' {
				line := chunk
				if len(t.partial) > 0 {
					line = append(append([]byte(nil), t.partial...), chunk...)
					t.partial = t.partial[:0]
				}
				line = line[:len(line)-1]
				if len(line) > 0 && line[len(line)-1] == '\r' {
					line = line[:len(line)-1]
				}
				if t.OnLine != nil {
					t.OnLine(string(line))
				}
			} else {
				// Last, incomplete line in the file so far — remember it and
				// pick it up on the next poll once it's terminated.
				t.partial = append(t.partial, chunk...)
			}
		}
		if err != nil {
			break
		}
	}
}

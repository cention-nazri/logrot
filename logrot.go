/*
   Copyright 2015 The Logrot Authors. See the AUTHORS file at the
   top-level directory of this distribution and at
   <https://xi2.org/x/logrot/m/AUTHORS>.

   This file is part of Logrot.

   Logrot is free software: you can redistribute it and/or modify it
   under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   Lotrot is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with Logrot.  If not, see <https://www.gnu.org/licenses/>.
*/

// Package logrot implements simple log rotation with compression.
//
// Note: The API is presently experimental and may change.
package logrot // import "xi2.org/x/logrot"

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

type writeCloser struct {
	path     string
	maxLines int
	maxFiles int
	file     *os.File
	lines    int
	closed   bool
	writeErr error
	mutex    sync.Mutex
}

// rotate performs the rotation as described in the comment for Open.
func (wc *writeCloser) rotate() error {
	err := os.Remove(fmt.Sprintf("%s.%d.gz", wc.path, wc.maxFiles-1))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	for i := wc.maxFiles - 2; i > 0; i-- {
		err = os.Rename(
			fmt.Sprintf("%s.%d.gz", wc.path, i),
			fmt.Sprintf("%s.%d.gz", wc.path, i+1))
		if err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	err = wc.file.Close()
	if err != nil {
		return err
	}
	r, err := os.Open(wc.path)
	if err != nil {
		return err
	}
	w, err := os.OpenFile(
		fmt.Sprintf("%s.1.gz", wc.path),
		os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0600)
	if err != nil {
		_ = r.Close()
		return err
	}
	gw := gzip.NewWriter(w)
	_, err = io.Copy(gw, r)
	if err != nil {
		_ = gw.Close()
		_ = w.Close()
		_ = r.Close()
		return err
	}
	err = gw.Close()
	if err != nil {
		_ = w.Close()
		_ = r.Close()
		return err
	}
	err = w.Close()
	if err != nil {
		_ = r.Close()
		return err
	}
	err = r.Close()
	if err != nil {
		return err
	}
	err = os.Remove(wc.path)
	if err != nil {
		return err
	}
	wc.lines = 0
	file, err := os.OpenFile(
		wc.path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	wc.file = file
	return nil
}

func (wc *writeCloser) Write(p []byte) (_ int, err error) {
	wc.mutex.Lock()
	defer wc.mutex.Unlock()
	if wc.writeErr != nil {
		// once Write returns an error, any subsequent calls will
		// return the same error. To continue writing one must create
		// a new WriteCloser using Open.
		return 0, wc.writeErr
	}
	defer func() {
		wc.writeErr = err
	}()
	if wc.closed {
		return 0, errors.New("logrot: WriterCloser is closed")
	}
	br := 0 // bytes read from p
	bw := 0 // bytes written
	for br < len(p) {
		bs := br
		for wc.lines < wc.maxLines {
			i := bytes.IndexByte(p[br:], '\n')
			if i == -1 {
				br += len(p[br:])
				break
			}
			br += i + 1
			wc.lines++
		}
		var n int
		n, err = wc.file.Write(p[bs:br])
		bw += n
		if err != nil {
			return bw, err
		}
		if wc.lines >= wc.maxLines {
			err = wc.rotate()
			if err != nil {
				return bw, err
			}
		}
	}
	return bw, nil
}

func (wc *writeCloser) Close() error {
	wc.mutex.Lock()
	defer wc.mutex.Unlock()
	if !wc.closed {
		err := wc.file.Close()
		if err != nil {
			return err
		}
		wc.closed = true
	}
	return nil
}

// Open opens the file at path for writing in append mode. If it does
// not exist it is created with permissions of 0600.
//
// The returned WriteCloser keeps track of the number of lines in the
// file. When that number becomes greater than or equal to maxLines a
// rotation occurs. A rotation is the following procedure:
//
// Let N = maxFiles-1. Firstly, the file <path>.<N>.gz is deleted if
// it exists. Then, if N > 0, for n from N-1 down to 1 the file
// <path>.<n>.gz is renamed to <path>.<n+1>.gz if it exists. Next,
// <path> is gzipped and saved to the file <path>.1.gz . Lastly,
// <path> is deleted and then opened again as a new file and writing
// continues.
//
// It is safe to call Write/Close from multiple goroutines.
func Open(path string, maxLines int, maxFiles int) (io.WriteCloser, error) {
	if maxLines < 1 {
		return nil, errors.New("logrot: maxLines < 1")
	}
	if maxFiles < 1 {
		return nil, errors.New("logrot: maxFiles < 1")
	}
	lines := 0
	file, err := os.Open(path)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if err == nil {
		// count lines in file
		r := bufio.NewReader(file)
		for {
			_, err := r.ReadSlice('\n')
			if err == io.EOF {
				break
			}
			if err != nil {
				_ = file.Close()
				return nil, err
			}
			lines++
		}
		err = file.Close()
		if err != nil {
			return nil, err
		}
	}
	file, err = os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	return &writeCloser{
		path:     path,
		maxLines: maxLines,
		maxFiles: maxFiles,
		file:     file,
		lines:    lines,
	}, nil
}

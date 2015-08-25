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

// Package logrot implements a log file writer with rotation and gzip
// compression. The rotation and compression are handled as needed
// during write operations.
//
// Note: The API is presently experimental and may change.
//
// Example
//
// Suppose you have a log file that looks like this:
//
//   -rw------- 1 user user  876543 Aug 24 02:41 logfile
//
// and you open it with Open specifying a maxSize parameter of 1000000
// bytes and a maxFiles parameter of 3.
//
//   w, err := logrot.Open("logfile", 0600, 1000000, 3)
//
// If you write a further 1000000 bytes Logrot will perform a rotation
// during the write and you'll be left with something like this:
//
//   -rw------- 1 user user  876564 Aug 24 02:44 logfile
//   -rw------- 1 user user  115460 Aug 24 02:44 logfile.1.gz
//
// Write a further 500000 bytes and you'll get this:
//
//   -rw------- 1 user user  376585 Aug 24 02:45 logfile
//   -rw------- 1 user user  111956 Aug 24 02:45 logfile.1.gz
//   -rw------- 1 user user  115460 Aug 24 02:44 logfile.2.gz
//
// Lastly, writing a further 1000000 bytes gives:
//
//   -rw------- 1 user user  376617 Aug 24 02:45 logfile
//   -rw------- 1 user user  122492 Aug 24 02:45 logfile.1.gz
//   -rw------- 1 user user  111956 Aug 24 02:45 logfile.2.gz
//
// There is no logfile.3.gz created since maxFiles is 3.
//
// In the example presented here, as is typical for normal log files,
// logfile.1.gz and logfile.2.gz will be equal to or just under
// maxSize bytes when decompressed. There is one unusual case where
// you will end up with files greater than maxSize which is if you
// write more than maxSize bytes without a newline. Logrot only splits
// files at newlines so in this case it allows the log file to grow
// larger and then splits it when/if a newline is finally written.
package logrot // import "xi2.org/x/logrot"

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

type writeCloser struct {
	path        string
	perm        os.FileMode
	maxSize     int64
	maxFiles    int
	file        *os.File
	size        int64
	lastNewline int64
	closed      bool
	writeErr    error
	mutex       sync.Mutex
}

// rotate performs the rotation as described in the comment for
// Open. It assumes file contains a newline.
func (wc *writeCloser) rotate() error {
	// find highest n such that <path>.<n>.gz exists
	n := 0
	for {
		_, err := os.Lstat(fmt.Sprintf("%s.%d.gz", wc.path, n+1))
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		if err == nil {
			n++
		} else {
			break
		}
	}
	// delete expired gz files
	for ; n > wc.maxFiles-2 && n > 0; n-- {
		err := os.Remove(fmt.Sprintf("%s.%d.gz", wc.path, n))
		if err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	// move each gz file up one number
	for ; n > 0; n-- {
		err := os.Rename(
			fmt.Sprintf("%s.%d.gz", wc.path, n),
			fmt.Sprintf("%s.%d.gz", wc.path, n+1))
		if err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	// copy file contents up to last newline to <path>.1.gz
	if wc.maxFiles > 1 {
		w, err := os.OpenFile(
			fmt.Sprintf("%s.1.gz", wc.path), os.O_WRONLY|os.O_CREATE, wc.perm)
		if err != nil {
			return err
		}
		gw := gzip.NewWriter(w)
		err = func() error {
			// wrap in function literal to ensure gw and w are closed and
			// flushed before next step
			defer func() {
				e := gw.Close()
				if e != nil {
					err = e
				}
				e = w.Close()
				if e != nil {
					err = e
				}
			}()
			_, err = wc.file.Seek(0, 0)
			if err != nil {
				return err
			}
			_, err = io.CopyN(gw, wc.file, wc.lastNewline+1)
			return err
		}()
		if err != nil {
			return err
		}
	}
	// copy contents beyond last newline to beginning of file
	sr := io.NewSectionReader(
		wc.file, wc.lastNewline+1, wc.size-wc.lastNewline-1)
	_, err := wc.file.Seek(0, 0)
	if err != nil {
		return err
	}
	_, err = io.Copy(wc.file, sr)
	if err != nil {
		return err
	}
	// truncate file
	err = wc.file.Truncate(wc.size - wc.lastNewline - 1)
	if err != nil {
		return err
	}
	// adjust recorded size
	wc.size = wc.size - wc.lastNewline - 1
	wc.lastNewline = -1
	return nil
}

func (wc *writeCloser) Write(p []byte) (_ int, err error) {
	wc.mutex.Lock()
	defer wc.mutex.Unlock()
	if wc.writeErr != nil {
		// If Write returns an error once, any subsequent calls
		// fail. To continue writing one must create a new WriteCloser
		// using Open.
		return 0, fmt.Errorf(
			"logrot: Write cannot complete due to previous error: %v",
			wc.writeErr)
	}
	defer func() {
		// save return value on exit
		wc.writeErr = err
	}()
	if wc.closed {
		return 0, errors.New("logrot: WriteCloser is closed")
	}
	bw := 0 // total bytes written
	br := 0 // bytes read from p in each loop iteration
	for ; len(p) > 0; p, br = p[br:], 0 {
		// advance br a line at a time until we reach end of buffer or
		// br+wc.size advances past wc.maxSize
		for {
			i := bytes.IndexByte(p[br:], '\n')
			if i == -1 {
				br += len(p[br:])
				break
			}
			lnl := wc.size + int64(br+i)
			if lnl < wc.maxSize || wc.lastNewline == -1 {
				// record newline if before maxSize or first newline found
				wc.lastNewline = lnl
			}
			br += i + 1
			if wc.size+int64(br) > wc.maxSize {
				break
			}
		}
		rotate := false
		if wc.lastNewline != -1 {
			max := wc.lastNewline + 1
			if wc.maxSize > max {
				max = wc.maxSize
			}
			if wc.size+int64(br) > max {
				// file data + data to be written contains a newline
				// and exceeds max(maxSize,lastNewline+1) in
				// size. Reduce write down to this limit and schedule
				// a rotation following the write.
				br = int(max - wc.size)
				rotate = true
			}
		}
		var n int
		n, err = wc.file.WriteAt(p[:br], wc.size)
		bw += n
		wc.size += int64(n)
		if err != nil {
			return bw, err
		}
		if rotate {
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
// not exist it is created with permissions of perm.
//
// The returned WriteCloser keeps track of the size of the file and
// the position of the most recent newline. If during a call to Write
// a particular byte to be written would cause the file size to exceed
// maxSize bytes, and at least one newline has been written to the
// file already, then a rotation occurs before the byte is written. A
// rotation is the following procedure:
//
// Let N = highest n such that <path>.<n>.gz exists or zero
// otherwise. Let M = maxFiles. Starting at n = N, while n > M-2 and n
// > 0 delete <path>.<n>.gz and decrement n. Then, while n > 0, rename
// <path>.<n>.gz to <path>.<n+1>.gz and decrement n. Next, if M > 1,
// the contents of <path> up to and including the final newline are
// gzipped and saved to the file <path>.1.gz . Lastly, the contents of
// <path> beyond the final newline are copied to the beginning of the
// file and <path> is truncated to contain just those contents.
//
// It is safe to call Write/Close from multiple goroutines.
func Open(path string, perm os.FileMode, maxSize int64, maxFiles int) (io.WriteCloser, error) {
	if maxSize < 1 {
		return nil, errors.New("logrot: maxSize < 1")
	}
	if maxFiles < 1 {
		return nil, errors.New("logrot: maxFiles < 1")
	}
	// if path exists determine size and check path is a regular file.
	var size int64
	fi, err := os.Lstat(path)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if err == nil {
		if fi.Mode()&os.ModeType != 0 {
			return nil, fmt.Errorf("logrot: %s is not a regular file", path)
		}
		size = fi.Size()
	}
	// open path for reading/writing, creating it if necessary.
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, perm)
	if err != nil {
		return nil, err
	}
	// determine last newline position within file by reading backwards.
	var lastNewline int64 = -1
	const bufExp = 13 // 8KB buffer
	buf := make([]byte, 1<<bufExp)
	off := ((size - 1) >> bufExp) << bufExp
	bufSz := size - off
	for off >= 0 {
		_, err = file.ReadAt(buf[:bufSz], off)
		if err != nil {
			_ = file.Close()
			return nil, err
		}
		i := bytes.LastIndexByte(buf[:bufSz], '\n')
		if i != -1 {
			lastNewline = off + int64(i)
			break
		}
		off -= 1 << bufExp
		bufSz = 1 << bufExp
	}
	return &writeCloser{
		path:        path,
		perm:        perm,
		maxSize:     maxSize,
		maxFiles:    maxFiles,
		file:        file,
		size:        size,
		lastNewline: lastNewline,
	}, nil
}

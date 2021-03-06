/*
 * Minio Cloud Storage, (C) 2019 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"bytes"
	"context"
	"encoding/hex"
	"hash"
	"io"
        "fmt"
	"github.com/minio/minio/cmd/logger"
)

// Calculates bitrot in chunks and writes the hash into the stream.
type streamingBitrotWriter struct {
	iow       *io.PipeWriter
	h         hash.Hash
	shardSize int64
	canClose  chan struct{} // Needed to avoid race explained in Close() call.

	// Following two fields are used only to make sure that Write(p) is called such that
	// len(p) is always the block size except the last block, i.e prevent programmer errors.
	currentBlockIdx int
	verifyTillIdx   int
}

func (b *streamingBitrotWriter) Write(p []byte) (int, error) {
	if b.currentBlockIdx < b.verifyTillIdx && int64(len(p)) != b.shardSize {
		// All blocks except last should be of the length b.shardSize
                fmt.Println("### wrong length !! ::", b.shardSize, len(p), b.currentBlockIdx, b.verifyTillIdx)
		logger.LogIf(context.Background(), errUnexpected)
		return 0, errUnexpected
	}
	if len(p) == 0 {
		return 0, nil
	}
        //fmt.Println("### Write ::",b.shardSize, len(p))
        if (!globalZeroCopyReader) {
	  b.h.Reset()
	  b.h.Write(p)
	  hashBytes := b.h.Sum(nil)
          //fmt.Println("### Check-Sum Write ::", len(hashBytes))
	  n, err := b.iow.Write(hashBytes)
	  if n != len(hashBytes) {
		logger.LogIf(context.Background(), err)
		return 0, err
	  }
        }
	n_p, err_p := b.iow.Write(p)
	b.currentBlockIdx++
	return n_p, err_p
}

func (b *streamingBitrotWriter) Close() error {
	err := b.iow.Close()
	// Wait for all data to be written before returning else it causes race conditions.
	// Race condition is because of io.PipeWriter implementation. i.e consider the following
	// sequent of operations:
	// 1) pipe.Write()
	// 2) pipe.Close()
	// Now pipe.Close() can return before the data is read on the other end of the pipe and written to the disk
	// Hence an immediate Read() on the file can return incorrect data.
	<-b.canClose
	return err
}

// Returns streaming bitrot writer implementation.
func newStreamingBitrotWriter(disk StorageAPI, volume, filePath string, length int64, algo BitrotAlgorithm, shardSize int64) io.WriteCloser {
	r, w := io.Pipe()
	h := algo.New()
	bw := &streamingBitrotWriter{w, h, shardSize, make(chan struct{}), 0, int(length / shardSize)}
	go func() {
		bitrotSumsTotalSize := ceilFrac(length, shardSize) * int64(h.Size()) // Size used for storing bitrot checksums.
                if (globalZeroCopyReader) {
                  bitrotSumsTotalSize = 0
                }
		totalFileSize := bitrotSumsTotalSize + length
		err := disk.CreateFile(volume, filePath, totalFileSize, r)
		if err != nil {
			logger.LogIf(context.Background(), err)
			r.CloseWithError(err)
		}
		close(bw.canClose)
	}()
	return bw
}

// ReadAt() implementation which verifies the bitrot hash available as part of the stream.
type streamingBitrotReader struct {
	disk       StorageAPI
	rc         io.ReadCloser
	volume     string
	filePath   string
	tillOffset int64
	currOffset int64
	h          hash.Hash
	shardSize  int64
	hashBytes  []byte
}

func (b *streamingBitrotReader) Close() error {
	if b.rc == nil {
		return nil
	}
	return b.rc.Close()
}

func (b *streamingBitrotReader) ReadAt(buf []byte, offset int64) (int, error) {
        if (globalDummy_read == 2) {
          //fmt.Println("### Returning Dummy read ::",b.volume, b.filePath, len(buf)) 
          return len(buf), nil
        }
        //fmt.Println("### ReadAt, buf_len, offset, volume, filePath, path", len(buf), offset, b.volume, b.filePath, b.disk.String())        
	var err error
	if offset%b.shardSize != 0 {
		// Offset should always be aligned to b.shardSize
		logger.LogIf(context.Background(), errUnexpected)
		return 0, errUnexpected
	}
	if b.rc == nil {
		// For the first ReadAt() call we need to open the stream for reading.
		b.currOffset = offset
		streamOffset := (offset/b.shardSize)*int64(b.h.Size()) + offset
                /*fmt.Println("### Calling from streamingBitrotReader->ReadAt, volume, filePath, path, offset, len ::", 
                             b.volume, b.filePath, b.disk.String(), streamOffset, b.tillOffset-streamOffset)*/
		b.rc, err = b.disk.ReadFileStream(b.volume, b.filePath, streamOffset, b.tillOffset-streamOffset)
		if ((b.rc == nil) || (err != nil)) {
			logger.LogIf(context.Background(), err)
                        fmt.Println("!!! ReadFileStream failed,  err = ", err, b.volume, b.filePath, b.disk.String())
			return 0, err
		}
	}
	if offset != b.currOffset {
		logger.LogIf(context.Background(), errUnexpected)
		return 0, errUnexpected
	}
	//b.h.Reset()
        var h_bytes_read int = 0
        var d_bytes_read int = 0
        //if (!globalZeroCopyReader || len(buf) <= 8192) {
        if (!globalZeroCopyReader) {
	  h_bytes_read, err = io.ReadFull(b.rc, b.hashBytes)
	  if err != nil {
		logger.LogIf(context.Background(), err)
                fmt.Println("!!! Read failed during hash read, err = ", err, h_bytes_read)
		return 0, err
	  }
        }
	d_bytes_read, err = io.ReadFull(b.rc, buf)
	if err != nil {
		logger.LogIf(context.Background(), err)
                fmt.Println("!!! Read failed during data read, err = ", err, d_bytes_read)
		return 0, err
	}
        data_length := b.tillOffset - int64 (b.h.Size())
        //fmt.Println("###### ReadAt, Total bytes read ::", d_bytes_read, h_bytes_read, b.tillOffset, len(buf), data_length, b.disk.String())
        if (globalDummy_read == 0 && globalVerifyChecksum && !globalZeroCopyReader) {
          //fmt.Println("### Doing checksum verify..", b.volume, b.filePath, data_length, b.disk.String())
          b.h.Reset()
	  b.h.Write(buf[:data_length])

	  if (!bytes.Equal(b.h.Sum(nil), b.hashBytes)) {
		err = hashMismatchError{hex.EncodeToString(b.hashBytes), hex.EncodeToString(b.h.Sum(nil))}
		logger.LogIf(context.Background(), err)
                fmt.Println("!!! Read failed, err = ", err, b.disk.String())
		return 0, err
	  }
        }

	b.currOffset += int64(len(buf))
	return len(buf), nil
}

// Returns streaming bitrot reader implementation.
func newStreamingBitrotReader(disk StorageAPI, volume, filePath string, tillOffset int64, algo BitrotAlgorithm, shardSize int64) *streamingBitrotReader {
        //fmt.Println("##newStreamingBitrotReader, volume, filePath, tillOffset, shardSize", volume, filePath, tillOffset, shardSize)
	h := algo.New()
        if (!globalZeroCopyReader) {
         tillOffset += ceilFrac(tillOffset, shardSize)*int64(h.Size())
        }
        
	return &streamingBitrotReader{
		disk,
		nil,
		volume,
		filePath,
		tillOffset,
		0,
		h,
		shardSize,
		make([]byte, h.Size()),
	}
}

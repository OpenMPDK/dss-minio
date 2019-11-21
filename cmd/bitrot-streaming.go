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
        //"io/ioutil"
        "fmt"
        "os"
        "sync/atomic"
        //"runtime"
	"github.com/minio/minio/cmd/logger"
        "strconv"
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
		logger.LogIf(context.Background(), errUnexpected)
		return 0, errUnexpected
	}
	if len(p) == 0 {
		return 0, nil
	}
	b.h.Reset()
	b.h.Write(p)
	hashBytes := b.h.Sum(nil)
	n, err := b.iow.Write(hashBytes)
	if n != len(hashBytes) {
		logger.LogIf(context.Background(), err)
		return 0, err
	}
	n, err = b.iow.Write(p)
	b.currentBlockIdx++
	return n, err
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
        totalRead  int64 
        r_offset   int64
        r_b        []byte
        r_id       int64
        l_c_s      int
        buf_m      int
        last_till  int64
        buffer_th  int
}

func (b *streamingBitrotReader) Close() error {
	if b.rc == nil {
		return nil
	}
	return b.rc.Close()
}

var reader_uuid int64 = 0

func (b *streamingBitrotReader) ReadAt(buf []byte, offset int64) (int, error) {
	var err error
        //var reader_id int64
        //var r_b []byte
        //var r_chunk_size int = 0

        //var readLen int64 = 0;
	if offset%b.shardSize != 0 {
		// Offset should always be aligned to b.shardSize
		logger.LogIf(context.Background(), errUnexpected)
		return 0, errUnexpected
	}
	if b.rc == nil {
		// For the first ReadAt() call we need to open the stream for reading.
                b.r_b = nil
                //runtime.GC()
                if (b.totalRead != b.last_till) {
                  fmt.Println("### Read not finished !! ", b.totalRead, b.last_till)
                }
                chunk_len := len(buf) + len(b.hashBytes)
                mult := b.buf_m
                for (chunk_len < b.buffer_th) {
                 
                  chunk_len *= mult
                  mult += 1
                }
                //fmt.Println("### Chunk len = ", chunk_len)
                if (mult != b.buf_m) {
                  //b.r_b = make([]byte, chunk_len)
                }
                b.r_offset = 0
                b.totalRead = 0
                b.r_id = atomic.AddInt64(&reader_uuid, 1)
		b.currOffset = offset
		streamOffset := (offset/b.shardSize)*int64(b.h.Size()) + offset
                b.last_till = b.tillOffset-streamOffset
		b.rc, err = b.disk.ReadFileStream(b.volume, b.filePath, streamOffset, b.tillOffset-streamOffset)
		if err != nil {
			logger.LogIf(context.Background(), err)
			return 0, err
		}
                
	} else {
          //fmt.Println("### Reusing the stream ### ", offset, int64(len(b.hashBytes)), int64(len(buf)), b.currOffset, b.tillOffset)
          
        }
	if offset != b.currOffset {
		logger.LogIf(context.Background(), errUnexpected)
		return 0, errUnexpected
	}
	b.h.Reset()
        if (b.r_b != nil) {
          if ((int(b.r_offset) == b.l_c_s) && (b.totalRead != b.last_till)) {
            //fmt.Println("### Before ReadAll ### ", b.r_id, b.r_offset, int64(len(b.r_b)), b.totalRead, b.tillOffset)
            //b.r_b, err = ioutil.ReadAll(b.rc)
            b.l_c_s, err = io.ReadFull(b.rc, b.r_b)
            if err != nil {
              //fmt.Println("??? Got error while reading from b.rc???", b.l_c_s, err)
            }
            for (int64(len(b.r_b)) == 0) {
 
              fmt.Println("### ReadAll ### ", b.r_id, offset, b.r_offset, b.totalRead, int64(len(buf)), b.currOffset, b.tillOffset)
              //b.r_b, err = ioutil.ReadAll(b.rc)
              b.l_c_s, err = io.ReadFull(b.rc, b.r_b)
              if err != nil {
                fmt.Println("??? Got error while reading from b.rc???")
                return 0, errUnexpected
              }
            
            }
            b.totalRead += int64(b.l_c_s)
            b.r_offset = 0
            //fmt.Println("### After ReadAll ### ", b.r_id, b.r_offset, b.l_c_s, int64(len(b.r_b)), int64(len(buf)), b.totalRead, b.tillOffset)
          } else {
            //fmt.Println("### Still consuming old read ### ", b.r_id, b.r_offset, b.l_c_s, int64(len(b.r_b)), int64(len(buf)), b.totalRead, b.tillOffset)
          }
          if (b.r_offset > int64(len(b.r_b))) {
            fmt.Println("### copy1 ### ", b.r_id, b.r_offset, int64(len(b.r_b)))
          }
          copy (b.hashBytes, b.r_b[b.r_offset:])
        
          b.r_offset += int64(len(b.hashBytes))

          if (b.r_offset > int64(len(b.r_b))) {
            fmt.Println("### copy2 ### ", b.r_id, b.r_offset, int64(len(b.r_b)))
          }        
          copy(buf, b.r_b[b.r_offset:])
          b.r_offset += int64(len(buf)) 

        } else {
          //fmt.Println("### No copy ### ", int64(len(buf)), int64(len(b.hashBytes)))
	  _, err = io.ReadFull(b.rc, b.hashBytes)
	  if err != nil {
	    logger.LogIf(context.Background(), err)
	    return 0, err
	  }
	  _, err = io.ReadFull(b.rc, buf)
	  if err != nil {
	    logger.LogIf(context.Background(), err)
	    return 0, err
	  }
        } 

	b.h.Write(buf)

	if !bytes.Equal(b.h.Sum(nil), b.hashBytes) {
        //fmt.Println("### Hash compare ### ", b.r_id, h_offset, int64(len(b.hashBytes)), int64(len(b.r_b)))
	//if !bytes.Equal(b.h.Sum(nil), b.r_b[h_offset:int64(len(b.hashBytes))]) {
		err = hashMismatchError{hex.EncodeToString(b.hashBytes), hex.EncodeToString(b.h.Sum(nil))}
		logger.LogIf(context.Background(), err)
                fmt.Println("### Hash mismatch ### ", err)
                os.Exit(3)
		return 0, err
	}

	b.currOffset += int64(len(buf))

        //readSofar := int64(len(buf)) + int64(len(b.hashBytes));
        //b.totalRead += readSofar
        //if ((readSofar != readLen) && (readLen != 0)) {
          //fmt.Println("### Read pending ###", offset, int64(len(buf)), int64(len(b.hashBytes)), readLen, b.currOffset, b.tillOffset)
        //}
        //if (b.totalRead == b.tillOffset) {
        //  b.num_opened -= 1
        //  fmt.Println("### Clearing the stream ### ", offset, int64(len(buf)), b.currOffset, b.totalRead, b.num_opened)
        //  _, err_r:= ioutil.ReadAll(b.rc)
        //  if err_r != nil {
                //log.Fatal(err)
          //  fmt.Println("??? Got error while reading ???")

          //}
        //}

	return len(buf), nil
}

// Returns streaming bitrot reader implementation.
func newStreamingBitrotReader(disk StorageAPI, volume, filePath string, tillOffset int64, algo BitrotAlgorithm, shardSize int64) *streamingBitrotReader {
	h := algo.New()
        buf_multi := 2
        str := os.Getenv("MINIO_NKV_PIPE_READ_BUFFER_MULTIPLIER") 
        if str != "" {

          valSize, _ := strconv.Atoi(str)
          buf_multi = valSize
        }
        buf_th := 6 * 1024 * 1024
        str_th := os.Getenv("MINIO_NKV_PIPE_READ_BUFFER_THRESHOLD")
        if str_th != "" {

          valSize, _ := strconv.Atoi(str_th)
          buf_th = valSize
        }

	return &streamingBitrotReader{
		disk,
		nil,
		volume,
		filePath,
		ceilFrac(tillOffset, shardSize)*int64(h.Size()) + tillOffset,
		0,
		h,
		shardSize,
		make([]byte, h.Size()),
                0,
                0,
                //nil,
                //make([]byte, 5242912),
                nil,
                -1,
                0,
                buf_multi,
                0,
                buf_th,
	}
}

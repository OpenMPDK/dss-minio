/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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
	"io"
        "sync/atomic"
        "fmt"
        "sync"
	"github.com/klauspost/reedsolomon"
	"github.com/minio/minio/cmd/logger"
)

type kvPoolECPoolType struct {
       *sync.Pool
       count uint64
}

func (k *kvPoolECPoolType) Get() interface{} {
       atomic.AddUint64(&k.count, 1)
       return k.Pool.Get()
}

func (k *kvPoolECPoolType) Put(x interface{}) {
       //fmt.Println("## EC-Pool Put called, ", k.count)
       pool_cnt := atomic.LoadUint64(&k.count)
       if (pool_cnt > 0) {
         atomic.AddUint64(&k.count, ^uint64(0))
         k.Pool.Put(x)
       }
}

func (k *kvPoolECPoolType) PrintCount() {
       fmt.Println("## EC-Pool count", k.count)
}

type kvMetaPoolECPoolType struct {
       *sync.Pool
       count uint64
}

func (k *kvMetaPoolECPoolType) Get() interface{} {
       atomic.AddUint64(&k.count, 1)
       return k.Pool.Get()
}

func (k *kvMetaPoolECPoolType) Put(x interface{}) {
       //fmt.Println("## EC-Pool Put called, ", k.count)
       pool_cnt := atomic.LoadUint64(&k.count)
       if (pool_cnt > 0) {
         atomic.AddUint64(&k.count, ^uint64(0))
         k.Pool.Put(x)
       }
}

func (k *kvMetaPoolECPoolType) PrintCount() {
       fmt.Println("## EC-Meta-Pool count", k.count)
}

// Pools used in Replication path..

type kvMinSizePoolRepPoolType struct {
       *sync.Pool
}

func (k *kvMinSizePoolRepPoolType) Get() interface{} {
       return k.Pool.Get()
}

func (k *kvMinSizePoolRepPoolType) Put(x interface{}) {
       //fmt.Println("## EC-Pool Put called, ", k.count)
       k.Pool.Put(x)
}


type kvMidSizePoolRepPoolType struct {
       *sync.Pool
}

func (k *kvMidSizePoolRepPoolType) Get() interface{} {
       return k.Pool.Get()
}

func (k *kvMidSizePoolRepPoolType) Put(x interface{}) {
       //fmt.Println("## EC-Pool Put called, ", k.count)
       k.Pool.Put(x)
}


type kvMaxSizePoolRepPoolType struct {
       *sync.Pool
}

func (k *kvMaxSizePoolRepPoolType) Get() interface{} {
       return k.Pool.Get()
}

func (k *kvMaxSizePoolRepPoolType) Put(x interface{}) {
       k.Pool.Put(x)
}

var kvMinPoolRep *kvMinSizePoolRepPoolType = nil
var kvMidPoolRep *kvMidSizePoolRepPoolType = nil
var kvMaxPoolRep *kvMaxSizePoolRepPoolType = nil
var midSize int64 = 0
var minSize int64 = 0

func initRepPool(divFactor int) {

  fmt.Println("### Creating Max Rep Pool with object size = ", globalMaxKVObject)
  kvMaxPoolRep = &kvMaxSizePoolRepPoolType{
       Pool: &sync.Pool{
               New: func() interface{} {
                       b := make([]byte, globalMaxKVObject)
                       return b
               },
        },
  }

  midSize = globalMaxKVObject/int64 (divFactor)
  for (midSize <= 131072) {
    midSize = (2 * globalMaxKVObject)/int64 (divFactor)
  }
  fmt.Println("### Creating Mid Rep Pool with object size = ", midSize )
  kvMidPoolRep = &kvMidSizePoolRepPoolType{
       Pool: &sync.Pool{
               New: func() interface{} {
                       b := make([]byte, midSize)
                       return b
               },
        },
  }

  divFactor = 2 * divFactor
  minSize = globalMaxKVObject/int64 (divFactor)
  for (minSize <= 32768) {
    minSize = (2 * globalMaxKVObject)/int64 (divFactor)
  }
  fmt.Println("### Creating Min Rep Pool with object size = ", minSize )
  kvMinPoolRep = &kvMinSizePoolRepPoolType{
       Pool: &sync.Pool{
               New: func() interface{} {
                       b := make([]byte, minSize)
                       return b
               },
        },
  }



}

func  poolAllocRep(size int64) []byte {
  if (size <= minSize) {
    return kvMinPoolRep.Get().([]byte)
  } else if ((size > minSize) && (size <= midSize)) {
    return kvMidPoolRep.Get().([]byte)
  } else {
    return kvMaxPoolRep.Get().([]byte)
  }
  
}

func  poolDeAllocRep(buffer []byte, size int64) {
  if (size <= minSize) {
    kvMinPoolRep.Put(buffer)
  } else if ((size > minSize) && (size <= midSize)) {
    kvMidPoolRep.Put(buffer)
  } else {
    kvMaxPoolRep.Put(buffer)
  }

}




// End Rep pool

var kvPoolEC *kvPoolECPoolType = nil
var kvMetaPoolEC *kvMetaPoolECPoolType = nil

func initECPool() {
  fmt.Println("### Creating EC Pool with object size, EC Block size = ",customECpoolObjSize, blockSizeV1)
  kvPoolEC = &kvPoolECPoolType{
       Pool: &sync.Pool{
               New: func() interface{} {
                       b := make([]byte, customECpoolObjSize)
                       return b
               },
        },
  }

  fmt.Println("### Creating Meta EC Pool with object size = ", 8192)
  kvMetaPoolEC = &kvMetaPoolECPoolType{
       Pool: &sync.Pool{
               New: func() interface{} {
                       b := make([]byte, 8192)
                       return b
               },
        },
  }


}


func  poolAlloc(size int64) []byte {
  if (size <= 8192) {
    return kvMetaPoolEC.Get().([]byte)
  } else {
    return kvPoolEC.Get().([]byte)
  }
}

func releasePoolBuf(bufs [][]byte, dataBlocks int, shardSize int64) {
        if (bufs == nil) {
          return          
        }

	for rev_index, _ := range bufs[:dataBlocks] {
        	rev_index = len(bufs[:dataBlocks]) - 1 - rev_index
                block := bufs[rev_index]
                if block != nil {
                      if (shardSize <= 8192) {
                        kvMetaPoolEC.Put(block)
                      } else {
                        kvPoolEC.Put(block)
                      }
                }
        }

}


// getDataBlockLen - get length of data blocks from encoded blocks.
func getDataBlockLen(enBlocks [][]byte, dataBlocks int) int {
	size := 0
	// Figure out the data block length.
	for _, block := range enBlocks[:dataBlocks] {
		size += len(block)
	}
	return size
}

// Writes all the data blocks from encoded blocks until requested
// outSize length. Provides a way to skip bytes until the offset.
func writeDataBlocks(ctx context.Context, dst io.Writer, enBlocks [][]byte, dataBlocks int, offset int64, length int64) (int64, error) {
	// Offset and out size cannot be negative.
	if offset < 0 || length < 0 {
		logger.LogIf(ctx, errUnexpected)
		return 0, errUnexpected
	}

	// Do we have enough blocks?
	if len(enBlocks) < dataBlocks {
		logger.LogIf(ctx, reedsolomon.ErrTooFewShards)
		return 0, reedsolomon.ErrTooFewShards
	}

	// Do we have enough data?
	if int64(getDataBlockLen(enBlocks, dataBlocks)) < length {
		logger.LogIf(ctx, reedsolomon.ErrShortData)
		return 0, reedsolomon.ErrShortData
	}

	// Counter to decrement total left to write.
	write := length

	// Counter to increment total written.
	var totalWritten int64

	// Write all data blocks to dst.
	for _, block := range enBlocks[:dataBlocks] {
		// Skip blocks until we have reached our offset.
		if offset >= int64(len(block)) {
			// Decrement offset.
			offset -= int64(len(block))
			continue
		} else {
			// Skip until offset.
			block = block[offset:]

			// Reset the offset for next iteration to read everything
			// from subsequent blocks.
			offset = 0
		}
		// We have written all the blocks, write the last remaining block.
		if write < int64(len(block)) {
			n, err := io.Copy(dst, bytes.NewReader(block[:write]))
			if err != nil {
				if err != io.ErrClosedPipe {
					logger.LogIf(ctx, err)
				}
				return 0, err
			}
			totalWritten += n
			break
		}
		// Copy the block.
		n, err := io.Copy(dst, bytes.NewReader(block))
		if err != nil {
			// The writer will be closed incase of range queries, which will emit ErrClosedPipe.
			if err != io.ErrClosedPipe {
				logger.LogIf(ctx, err)
			}
			return 0, err
		}

		// Decrement output size.
		write -= n

		// Increment written.
		totalWritten += n
	}

	// Success.
	return totalWritten, nil
}

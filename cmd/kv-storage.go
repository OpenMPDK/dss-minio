package cmd

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync"
	"time"
        //"runtime"
        //"runtime/debug"
        "sync/atomic"
        //"strconv"
)

const kvVolumesKey = ".minio.sys/kv-volumes"

type kvVolumes struct {
	Version  string
	VolInfos []VolInfo
}

type KVStorage struct {
	kv        KVInterface
	volumes   *kvVolumes
	path      string
	volumesMu sync.RWMutex
}

var kvStorageCache = make(map[string]StorageAPI)
var kvStorageCacheMu sync.Mutex

func newPosix(path string) (StorageAPI, error) {
	kvStorageCacheMu.Lock()
	defer kvStorageCacheMu.Unlock()

	cache := kvStorageCache[path]
	if cache != nil {
		return cache, nil
	}
	cache, err := newKVPosix(path)
	if err != nil {
		return nil, err
	}
	kvStorageCache[path] = cache
	return cache, nil
}

func invokeGC() {
    for {
        //fmt.Println("About to invoke kvpool->print")
        //runtime.GC()
         kvValuePool.PrintCount()    
        time.Sleep(2 * time.Second)
    }
}
var init_gc uint32 = 0

func newKVPosix(path string) (StorageAPI, error) {
	kvPath := path
	path = strings.TrimPrefix(path, "/nkv/")

	if os.Getenv("MINIO_NKV_EMULATOR") != "" {
		dataDir := pathJoin("/tmp", path, "data")
		os.MkdirAll(dataDir, 0777)
		return &debugStorage{path, &KVStorage{kv: &KVEmulator{dataDir}, path: kvPath}, true}, nil
	}

	configPath := os.Getenv("MINIO_NKV_CONFIG")
	if configPath == "" {
		return nil, errDiskNotFound
	}

	if err := minio_nkv_open(configPath); err != nil {
		return nil, err
	}

        if (init_gc == 0) {
          go invokeGC()
          atomic.AddUint32(&init_gc, 1)
        }

	nkvSync := true
	if os.Getenv("MINIO_NKV_ASYNC") != "" {
		nkvSync = false
	}

	kv, err := newKV(path, nkvSync)
	if err != nil {
		return nil, err
	}
	p := &KVStorage{kv: kv, path: kvPath}
	if os.Getenv("MINIO_NKV_DEBUG") == "" {
		return p, nil
	}
	return &debugStorage{path, p, true}, nil
}

func (k *KVStorage) DataKey(id string) string {
	return path.Join(kvDataDir, id)
}

func (k *KVStorage) String() string {
	return k.path
}

func (k *KVStorage) IsOnline() bool {
	return true
}

func (k *KVStorage) LastError() error {
	return nil
}

var close_nkv uint32 = 0

func (k *KVStorage) Close() error {
        fmt.Println("#### Closing KV devices ####")
        if (close_nkv == 0 && globalIsStopping) {
          k.kv.nkv_close()
          atomic.AddUint32(&close_nkv, 1)
        }
	return nil
}

func (k *KVStorage) DiskInfo() (info DiskInfo, err error) {
	return k.kv.DiskInfo()
}

func (k *KVStorage) loadVolumes() (*kvVolumes, error) {
	volumes := &kvVolumes{}
	bufp := kvValuePool.Get().(*[]byte)
	defer kvValuePool.Put(bufp)
        //bufp := kvValuePoolMeta.Get().(*[]byte)
        //defer kvValuePoolMeta.Put(bufp)

	value, err := k.kv.Get(kvVolumesKey, *bufp)
	if err != nil {
		return volumes, nil
	}
	if err = json.Unmarshal(value, volumes); err != nil {
		return nil, err
	}
	return volumes, nil
}

func (k *KVStorage) verifyVolume(volume string) error {
	_, err := k.StatVol(volume)
	return err
}

func (k *KVStorage) SyncVolumes () (err error) {
        k.volumes, _ = k.loadVolumes()
	return nil
}

func (k *KVStorage) MakeVol(volume string) (err error) {
	k.volumesMu.Lock()
	defer k.volumesMu.Unlock()
	volumes, err := k.loadVolumes()
	if err != nil {
		return err
	}

	for _, vol := range volumes.VolInfos {
		if vol.Name == volume {
			return errVolumeExists
		}
	}

	volumes.VolInfos = append(volumes.VolInfos, VolInfo{volume, time.Now()})
	b, err := json.Marshal(volumes)
	if err != nil {
		return err
	}
	err = k.kv.Put(kvVolumesKey, b)
	if err != nil {
		return err
	}
	k.volumes = volumes
	return nil
}

func (k *KVStorage) ListVols() (vols []VolInfo, err error) {
        //fmt.Println("### ListVols called ###")
	k.volumesMu.Lock()
	defer k.volumesMu.Unlock()
	if k.volumes == nil {
		k.volumes, err = k.loadVolumes()
		if err != nil {
			return nil, err
		}
	}
	for _, vol := range k.volumes.VolInfos {
		if vol.Name == ".minio.sys/multipart" {
			continue
		}
		if vol.Name == ".minio.sys/tmp" {
			continue
		}
		vols = append(vols, vol)
	}
	return vols, nil
}

func (k *KVStorage) StatVol(volume string) (vol VolInfo, err error) {
	k.volumesMu.Lock()
	defer k.volumesMu.Unlock()
	if k.volumes == nil {
		k.volumes, err = k.loadVolumes()
		if err != nil {
			return vol, err
		}
	}
	for _, vol := range k.volumes.VolInfos {
                //fmt.Println("### StatVol::", vol.Name, volume)
		if vol.Name == volume {
			return VolInfo{vol.Name, vol.Created}, nil
		}
	}
	return vol, errVolumeNotFound
}

func (k *KVStorage) DeleteVol(volume string) (err error) {
	k.volumesMu.Lock()
	defer k.volumesMu.Unlock()
	volumes, err := k.loadVolumes()
	if err != nil {
		return err
	}
	foundIndex := -1
	for i, vol := range volumes.VolInfos {
		if vol.Name == volume {
			foundIndex = i
			break
		}
	}
	if foundIndex == -1 {
		return errVolumeNotFound
	}
	entries, err := k.ListDir(volume, "", -1)
	if err != nil {
		return err
	}
	if len(entries) > 0 {
		return errVolumeNotEmpty
	}
	volumes.VolInfos = append(volumes.VolInfos[:foundIndex], volumes.VolInfos[foundIndex+1:]...)

	b, err := json.Marshal(volumes)
	if err != nil {
		return err
	}
	err = k.kv.Put(kvVolumesKey, b)
	if err != nil {
		return err
	}
	k.volumes = volumes
	return err
}

func (k *KVStorage) getKVNSEntry(nskey string) (entry KVNSEntry, err error) {
	tries := 10
	//bufp := kvValuePool.Get().(*[]byte)
	bufp := kvValuePoolMeta.Get().(*[]byte)
	defer kvValuePoolMeta.Put(bufp)
	//defer kvValuePool.Put(bufp)

	for {
		value, err := k.kv.Get(nskey, *bufp)
		if err != nil {
			return entry, err
		}
		err = KVNSEntryUnmarshal(value, &entry)
		if err != nil {
			length := 200
			if len(value) < length {
				length = len(value)
			}
			fmt.Println("##### Unmarshal failed on ", nskey, err, "\n", "hexdump: ", hex.EncodeToString(value[:length]))
			tries--
			if tries == 0 {
				fmt.Println("##### Unmarshal failed (after 10 retries on GET) on ", k.path, nskey)
				os.Exit(0)
			}
			continue
		}
		if entry.Key != nskey {
			fmt.Printf("##### key mismatch, requested: %s, got: %s\n", nskey, entry.Key)
			tries--
			if tries == 0 {
				fmt.Printf("##### key mismatch after 10 retries, requested: %s, got: %s\n", nskey, entry.Key)
				os.Exit(0)
			}
			continue
		}
		return entry, nil
	}
}

func (k *KVStorage) ListDirForRename(volume, dirPath string, count int) ([]string, error) {
     
	nskey := pathJoin(volume, dirPath, "xl.json")

	entry, err := k.getKVNSEntry(nskey)
	if err != nil {
		return nil, err
	}

	bufp := kvValuePool.Get().(*[]byte)
	defer kvValuePool.Put(bufp)

	tries := 10
	for {
		value, err := k.kv.Get(k.DataKey(entry.IDs[0]), *bufp)
		if err != nil {
			return nil, err
		}
		xlMeta, err := xlMetaV1UnmarshalJSON(context.Background(), value)
		if err != nil {
			fmt.Println("##### xlMetaV1UnmarshalJSON failed on", k.DataKey(entry.IDs[0]), len(value), string(value))
			tries--
			if tries == 0 {
				fmt.Println("##### xlMetaV1UnmarshalJSON failed on (10 retries)", k.DataKey(entry.IDs[0]), len(value), string(value))
				os.Exit(1)
			}
			continue
		}
		listEntries := []string{"xl.json"}
		for _, part := range xlMeta.Parts {
			listEntries = append(listEntries, part.Name)
		}
		return listEntries, err
	}
}

func (k *KVStorage) ListDir(volume, dirPath string, count int) ([]string, error) {
       //buf_trace := make([]byte, 1<<16)
       //runtime.Stack(buf_trace, true)
       //fmt.Printf("%s", buf_trace)
	bufp := kvValuePool.Get().(*[]byte)
	defer kvValuePool.Put(bufp)

	entries, err := k.kv.List(pathJoin(volume, dirPath), *bufp)
	if err != nil {
                fmt.Println("### Error during kv.List = ", volume, dirPath, err)
		return nil, err
	}
        //fmt.Println("## Num list entries from ListDir = ", volume, dirPath, k.path, len(entries), entries)
	return entries, nil
}

func (k *KVStorage) ReadFile(volume string, path string, offset int64, buf []byte, verifier *BitrotVerifier) (n int64, err error) {
	if err = k.verifyVolume(volume); err != nil {
		return 0, err
	}
	return 0, errFileAccessDenied
}

func (k *KVStorage) AppendFile(volume string, path string, buf []byte) (err error) {
	if err = k.verifyVolume(volume); err != nil {
		return err
	}
	return errFileAccessDenied
}

func (k *KVStorage) CreateDir(volume, dirPath string) error {
	if err := k.verifyVolume(volume); err != nil {
		return err
	}
	return k.kv.Put(pathJoin(volume, dirPath), []byte("abcd"))
}

func (k *KVStorage) StatDir(volume, dirPath string) error {
	if err := k.verifyVolume(volume); err != nil {
		return err
	}
	//bufp := kvValuePool.Get().(*[]byte)
	//defer kvValuePool.Put(bufp)
        bufp := kvValuePoolMeta.Get().(*[]byte)
        defer kvValuePoolMeta.Put(bufp)

	_, err := k.kv.Get(pathJoin(volume, dirPath), *bufp)
	return err
}

func (k *KVStorage) DeleteDir(volume, dirPath string) error {
	if err := k.verifyVolume(volume); err != nil {
		return err
	}
	return k.kv.Delete(pathJoin(volume, dirPath))
}

func (k *KVStorage) CreateFile(volume, filePath string, size int64, reader io.Reader) error {
	if err := k.verifyVolume(volume); err != nil {
		return err
	}
	nskey := pathJoin(volume, filePath)
	entry := KVNSEntry{Key: nskey, Size: size, ModTime: time.Now()}
	bufp := kvValuePool.Get().(*[]byte)
	defer kvValuePool.Put(bufp)

	buf := *bufp
	for {
		if size == 0 {
			break
		}
		if size < int64(len(buf)) {
			buf = buf[:size]
		}
		n, err := io.ReadFull(reader, buf)
		if err != nil {
			return err
		}
		size -= int64(n)
		id := mustGetUUID()
		if kvPadding {
			if len(buf) < kvMaxValueSize {
				paddedSize := ceilFrac(int64(len(buf)), kvNSEntryPaddingMultiple) * kvNSEntryPaddingMultiple
				for {
					if int64(len(buf)) == paddedSize {
						break
					}
					buf = append(buf, '\x00')
				}
			}
		}
		if err = k.kv.Put(k.DataKey(id), buf); err != nil {
			return err
		}
		entry.IDs = append(entry.IDs, id)
	}
	b, err := KVNSEntryMarshal(entry, *bufp)
	if err != nil {
		return err
	}

	return k.kv.Put(nskey, b)
}

type Reader struct {
        key_name string
	valid_data []byte
        pool_buf *[]byte
        entry KVNSEntry
        k *KVStorage
	readIndex int64
        total_read int64
        offset int64
        length int64
        startIndex int64
        endIndex int64
        kvMaxValueSize int64 
        index int64
        is_freed bool
        async_chan chan int64
        will_stop int32
}

func (r *Reader) async_kv (key string) {

  //ticker := time.NewTicker(1 * time.Millisecond)
  pool_buf := kvValuePool.Get().(*[]byte)

  for {
    if (atomic.LoadInt32(&r.will_stop) == 1) {
      kvValuePool.Put(pool_buf)
      close(r.async_chan)
      return
    }
    if (r == nil) {
      return
    }
    id := r.entry.IDs[r.index]
    if (globalDummy_read == 8) {
      time.Sleep(10 * time.Nanosecond)
    } else if (globalDummy_read == 9) {
      time.Sleep(100 * time.Nanosecond)
    } else if (globalDummy_read == 10) {
      time.Sleep(1 * time.Millisecond)
    } else if (globalDummy_read == 11) { 
      time.Sleep(15 * time.Millisecond)
    } else {
      time.Sleep(5 * time.Millisecond)
    }
    _, err := r.k.kv.Get(r.k.DataKey(id), *pool_buf)
    if err != nil {
      fmt.Println("###Error during kv get", r.k.DataKey(id), err)
      kvValuePool.Put(pool_buf)
      return
    } else {
      //fmt.Println("### Async get call success for key = ", id, len(data_b))
    }

    /*select {
      case val:= <-r.async_chan:
        if (val > 0) {
          id := r.entry.IDs[val]
          //fmt.Println("### Async get call for key = ", id)
          //time.Sleep(5 * time.Millisecond)
          _, err := r.k.kv.Get(r.k.DataKey(id), *pool_buf)
          if err != nil {
            fmt.Println("###Error during kv get", r.k.DataKey(id), err)
            kvValuePool.Put(pool_buf)  
            return
          }
          
        } else {
          //fmt.Println("### Closing async channel ###")
          kvValuePool.Put(pool_buf)
          return
        }

      case  <-ticker.C:
        //fmt.Println("Tick at", t)
     
      default:
          if (atomic.LoadInt32(&r.will_stop) == 1) {
            kvValuePool.Put(pool_buf)
            fmt.Println("### Returning from async_kv ###, key = ", key)
            close(r.async_chan)
            return
          }
          if (r == nil) {
	    return
          }
          id := r.entry.IDs[r.index]
          //fmt.Println("### Async get call for key = ", id)
          //time.Sleep(5 * time.Millisecond)
          _, err := r.k.kv.Get(r.k.DataKey(id), *pool_buf)
          if err != nil {
            fmt.Println("###Error during kv get", r.k.DataKey(id), err)
            kvValuePool.Put(pool_buf)
            return
          } else {
            //fmt.Println("### Async get call success for key = ", id, len(data_b))
          }

    }*/
        
  }  
}

//var pool_buf_data *[]byte = kvValuePool.Get().(*[]byte)

func NewReader_kv(key string, k *KVStorage, entry KVNSEntry, offset, length int64) *Reader {
        kvMaxValueSize := int64(kvMaxValueSize)
        startIndex := offset / kvMaxValueSize
        endIndex := (offset + length) / kvMaxValueSize
        index := startIndex
        var pool_buf *[]byte = nil
        if (length > 8192) {
          if (!globalZeroCopyReader || (length % 4 != 0)) {
            pool_buf = kvValuePool.Get().(*[]byte)
          }
          //pool_buf = pool_buf_data
        } else {
          //fmt.Println("###Meta pool alloc", length)
          //if (length % 4 != 0) {
            pool_buf = kvValuePoolMeta.Get().(*[]byte)
          //}
        }
        var blockOffset, blockLength int64
        switch {
          case startIndex == endIndex:
            blockOffset = offset % kvMaxValueSize
            blockLength = length
          case index == startIndex:
            blockOffset = offset % kvMaxValueSize
            blockLength = kvMaxValueSize - blockOffset
          case index == endIndex:
            blockOffset = 0
            blockLength = (offset + length) % kvMaxValueSize
          default:
            blockOffset = 0
            blockLength = kvMaxValueSize
        }
        id := entry.IDs[index]
        //fmt.Println("### NewReader_kv ::", globalZeroCopyReader, globalDummy_read, length)
        if ( pool_buf != nil && (globalDummy_read <= 0 || globalDummy_read > 3 )) {
        //if ((!globalZeroCopyReader || length <= 8192) && (globalDummy_read <= 0 || globalDummy_read > 3 )) {
        //if (!globalZeroCopyReader && (globalDummy_read == 0 || globalDummy_read > 3 || length < 4096)) {
          //fmt.Println("### Old way ::", globalZeroCopyReader, globalDummy_read, length, key, k.DataKey(id), k.path)
          data_b, err := k.kv.Get(k.DataKey(id), *pool_buf)
          if err != nil {
            //debug.PrintStack()
            fmt.Println("###Error during kv get", key, k.DataKey(id), entry.IDs, err)
            return nil
          }

          index++
          a_chan := make(chan int64)
          custom_reader := Reader{key, data_b[blockOffset : blockOffset+blockLength], pool_buf, entry, k, 0, 0, offset, length, startIndex, endIndex, kvMaxValueSize, index, false, a_chan, 0}
          if (globalDummy_read > 5 && length > 4096) {
            //go custom_reader.async_kv (k, entry, index, a_chan )
            go custom_reader.async_kv (id)
          }
	  //return &Reader{key, data_b[blockOffset : blockOffset+blockLength], pool_buf, entry, k, 0, 0, offset, length, startIndex, endIndex, kvMaxValueSize, index, false, a_chan}
          return &custom_reader
        } else if (globalZeroCopyReader) {
          a_chan := make(chan int64)
          custom_reader := Reader{key, nil, nil, entry, k, 0, 0, offset, length, startIndex, endIndex, kvMaxValueSize, index, false, a_chan, 0}
          return &custom_reader

        } else {
          a_chan := make(chan int64)
          custom_reader := Reader{key, (*pool_buf)[blockOffset : blockOffset+blockLength], pool_buf, entry, k, 0, 0, offset, length, startIndex, endIndex, kvMaxValueSize, index, false, a_chan, 0}          
          return &custom_reader
        }
}

func (r *Reader) Close() error {
  if (r == nil) {
    fmt.Println("### Error, null reader to close")
    return errFaultyDisk 
  }
  if (!r.is_freed) {
    if (!globalZeroCopyReader && r.pool_buf != nil) {
      if (r.length > 8192) {
        kvValuePool.Put(r.pool_buf)
      } else {
        kvValuePoolMeta.Put(r.pool_buf)
      }
    } 
    r.is_freed = true
  }
  if (globalDummy_read > 5 && r.length > 4096) {
    /*select {
      case r.async_chan <- 0:
      default:
        fmt.Println("no message sent during close")
    }*/
    //r.async_chan <- 0
    atomic.AddInt32(&r.will_stop, 1)

  }
  return nil
}


func (r *Reader) Read(p []byte) (n int, err error) {
        //fmt.Println("### NewReader_kv.Read, key, input_buffer_size, total_length, mount_point", r.key_name, len(p), r.length, r.offset, r.k.path, globalDummy_read )
        if (r == nil) {
          fmt.Println("### Error, null reader to read")
          return 0, errFaultyDisk
        }
        n = 0
        var bytes_copied int = 0
        var read_next_chunk bool = false

        //r.valid_data = nil
        //if (globalZeroCopyReader && r.length > 8192) {
        if (globalZeroCopyReader && r.pool_buf == nil) {

          if (r.index > r.endIndex) {
            //fmt.Println("Returning EOF from index !!", r.index, r.endIndex, r.total_read, r.length)
            //os.Exit(1)
            err = io.EOF
            //err = nil
            
            //return int(r.total_read), err 
            return int(len(p)), err 
          }
          for {
            var blockOffset, blockLength int64
            switch {
              case r.startIndex == r.endIndex:
                blockOffset = r.offset % r.kvMaxValueSize
                blockLength = r.length
              case r.index == r.startIndex:
                blockOffset = r.offset % r.kvMaxValueSize
                blockLength = r.kvMaxValueSize - blockOffset
              case r.index == r.endIndex:
                blockOffset = 0
                blockLength = (r.offset + r.length) % r.kvMaxValueSize
              default:
                blockOffset = 0
                blockLength = r.kvMaxValueSize
            }
            id := r.entry.IDs[r.index]
            //fmt.Println("##### Passing input buffer to kv", n, len(p), len(p) - n, r.offset, r.length, r.kvMaxValueSize, r.k.path)
            //data_bl, err := r.k.kv.Get(r.k.DataKey(id), p[n:n+int(r.kvMaxValueSize)])
            if (globalDummy_read > 0) {
              return len(p), nil
            }
            data_bl, err := r.k.kv.Get(r.k.DataKey(id), p)
            if err != nil {
               fmt.Println("###Error during kv get", r.key_name, r.k.DataKey(id), err)
               return n, err
            }
            if (len(data_bl) == 0) {
              fmt.Println("##### KV GET failed with 0 length object, copied sofar, buff_len, key ::", n, len(p), r.k.DataKey(id))
              os.Exit(1)
            }
            r.index++
            r.readIndex = 0
            valid_len := len(data_bl[blockOffset + r.readIndex : blockOffset+blockLength])
            n += valid_len
            r.total_read += int64(valid_len)

            if (((len(p) - n) == 0 ) || (r.total_read >= r.length)) {
               r.valid_data = nil
               //fmt.Println("##### Returning read globalZeroCopyReader ::", n, r.total_read, r.length, r.k.path)
               return n, nil
            }
            continue
          }

        } else {

          if (r.total_read >= r.length) {
	    err = io.EOF
            //fmt.Println("### EOF hit ::", r.total_read, r.length, len(p))
            return int(len(p)), err
	    //return
          } else {
            if r.readIndex >= int64(len(r.valid_data)) {
              //fmt.Println("### r.readIndex, r.valid_data len ::", r.readIndex, len(r.valid_data))
              read_next_chunk = true
            } else {
              remaining := int64(len(r.valid_data)) - r.readIndex
              if ((remaining > 0) && (int64(len(p)) > remaining)) {
                bytes_copied = copy(p[:remaining], r.valid_data[r.readIndex:])
                r.readIndex += int64(bytes_copied)
                r.total_read += int64(bytes_copied)
                n = bytes_copied
                //fmt.Println("### copied from leftover ::", n, r.readIndex, r.total_read, r.length)
                if (r.total_read < r.length) {
                  read_next_chunk = true
                } else {
                  return
                }
              }

            }
          }
        }

        if (read_next_chunk) {
          //Read the next chunk from KV
          if (r.index > r.endIndex) {
            fmt.Println("Returning EOF from index !!", r.index, r.endIndex, r.total_read, r.length)
            return
          }
          for {
            var blockOffset, blockLength int64
            switch {
              case r.startIndex == r.endIndex:
                blockOffset = r.offset % r.kvMaxValueSize
                blockLength = r.length
              case r.index == r.startIndex:
                blockOffset = r.offset % r.kvMaxValueSize
                blockLength = r.kvMaxValueSize - blockOffset
              case r.index == r.endIndex:
                blockOffset = 0
                blockLength = (r.offset + r.length) % r.kvMaxValueSize
              default:
                blockOffset = 0
                blockLength = r.kvMaxValueSize
            }
            id := r.entry.IDs[r.index]
            //var data_b []byte
            if (globalDummy_read > 0 && r.length > 4096) {
              //data_b = *r.pool_buf
              r.valid_data = *r.pool_buf
              //fmt.Println("## Dummy read::", len(p), len(r.valid_data), globalDummy_read)
              break;
            } else {
              if ((len(p) - n) >= int (r.kvMaxValueSize)) {
                //fmt.Println("##### Passing input buffer to kv", n, len(p), len(p) - n, r.kvMaxValueSize, r.k.path) 
                data_bl, err := r.k.kv.Get(r.k.DataKey(id), p[n:n+int(r.kvMaxValueSize)])
                if err != nil {
                  fmt.Println("###Error during kv get", r.key_name, r.k.DataKey(id), err)
                  return n, err
                }
                if (len(data_bl) == 0) {
                  fmt.Println("##### KV GET failed with 0 length object, copied sofar, buff_len, key ::", n, len(p), r.k.DataKey(id))
                  os.Exit(1)
                }
                r.index++
                r.readIndex = 0
                valid_len := len(data_bl[blockOffset + r.readIndex : blockOffset+blockLength])
                if (r.readIndex != 0) {
                  fmt.Println("##### Increase buf to r.readIndex = ", r.readIndex) 
                  p = data_bl[blockOffset + r.readIndex : blockOffset+blockLength]
                  r.readIndex = 0
                }
                n += valid_len
                //r.readIndex += len(data_bl)
                r.total_read += int64(valid_len)
                //if (n == len(p)) {
                  //return n, nil
                //}
                if ((len(p) - n) < int (r.kvMaxValueSize)) {
                  r.valid_data = nil
                  return n, nil
                }
                
                continue
              } else {
                //fmt.Println("???? Passing poolBuf to kv", len(p), n, len(p) - n, r.kvMaxValueSize, r.k.path)
                data_bl, err := r.k.kv.Get(r.k.DataKey(id), *r.pool_buf)
                if err != nil {
                  fmt.Println("###Error during kv get", r.key_name, r.k.DataKey(id), err)
                  return n, err
                }

                //data_b = data_bl
                r.index++
                r.valid_data = data_bl[blockOffset : blockOffset+blockLength]
                if (len(r.valid_data) == 0) {
                  fmt.Println("##### KV GET failed with 0 length object, key = ", r.k.DataKey(id))
                  os.Exit(1)
                }
                r.readIndex = 0

                break;
              }
            }
          }
        }
        var num_bytes int = 0
	if (globalDummy_read == 0 || r.length <= 4096) {
	  //n = copy(p[bytes_copied:], r.valid_data[r.readIndex:])
	  num_bytes = copy(p[n:], r.valid_data[r.readIndex:])
        } else {
          num_bytes = len(p) - bytes_copied
          if (globalDummy_read == 4 ) {
            //fmt.Println("## Dummy copy for SC4 ::", len(p), len(r.valid_data))
            copy(p, r.valid_data)
          }
          if (globalDummy_read >= 5 && read_next_chunk) {
            //fmt.Println("## Dummy copy for SC5 ::", len(p), len(r.valid_data))
            if (globalDummy_read == 5) {
              id := r.entry.IDs[r.index]
              _, err := r.k.kv.Get(r.k.DataKey(id), *r.pool_buf)
              if err != nil {
                fmt.Println("###Error during kv get", r.key_name, r.k.DataKey(id), err)
                return n, err
              }
              r.readIndex = 0 
            } else {
              //go r.k.kv.Get(r.k.DataKey(id), *r.pool_buf)
    	      /*select {
                case r.async_chan <- r.index:
                default:
                  //fmt.Println("no message sent during IO")
              }*/

              //r.async_chan <- r.index  
              r.readIndex = 0
              if (globalDummy_read == 7 ) {
                copy(p, r.valid_data)
              }
            }
          }
        }
        //r.readIndex = 0
	r.readIndex += int64(num_bytes)
        r.total_read += int64(num_bytes)
        n += num_bytes
        //r.valid_data = nil
	return
}


func (k *KVStorage) ReadFileStream(volume, filePath string, offset, length int64) (io.ReadCloser, error) {
	//if err := k.verifyVolume(volume); err != nil {
	//	return nil, err
	//}
	nskey := pathJoin(volume, filePath)
	entry, err := k.getKVNSEntry(nskey)
	if err != nil {
		return nil, err
	}
        if (length == -1) {
          length = entry.Size
          //fmt.Println("### Adjusted length:: ",length, entry.Size, nskey)
        }

        if (use_custome_reader) {
          r_io := NewReader_kv(nskey, k, entry, offset, length)
          if (r_io == nil) {
            return r_io, errFaultyDisk
          } else {       
            return r_io, nil
          }
        } else {

	  r, w := io.Pipe()
	  go func() {
		bufp := kvValuePool.Get().(*[]byte)
		defer kvValuePool.Put(bufp)
		kvMaxValueSize := int64(kvMaxValueSize)
		startIndex := offset / kvMaxValueSize
		endIndex := (offset + length) / kvMaxValueSize
		for index := startIndex; index <= endIndex; index++ {
			var blockOffset, blockLength int64
			switch {
			case startIndex == endIndex:
				blockOffset = offset % kvMaxValueSize
				blockLength = length
			case index == startIndex:
				blockOffset = offset % kvMaxValueSize
				blockLength = kvMaxValueSize - blockOffset
			case index == endIndex:
				blockOffset = 0
				blockLength = (offset + length) % kvMaxValueSize
			default:
				blockOffset = 0
				blockLength = kvMaxValueSize
			}
			if blockLength == 0 {
				break
			}
			id := entry.IDs[index]
			data, err := k.kv.Get(k.DataKey(id), *bufp)
			if err != nil {
				w.CloseWithError(err)
				return
			}

			w.Write(data[blockOffset : blockOffset+blockLength])
		}
		w.Close()
	  }()
	  return ioutil.NopCloser(r), nil
        }

}

func (k *KVStorage) RenameFile(srcVolume, srcPath, dstVolume, dstPath string) error {
	if err := k.verifyVolume(srcVolume); err != nil {
		return err
	}
	if err := k.verifyVolume(dstVolume); err != nil {
		return err
	}
	rename := func(src, dst string, data_chunk_delete bool) error {
		//bufp := kvValuePool.Get().(*[]byte)
		//defer kvValuePool.Put(bufp)
                bufp := kvValuePoolMeta.Get().(*[]byte)
                defer kvValuePoolMeta.Put(bufp)
		if src == ".minio.sys/format.json.tmp" && dst == ".minio.sys/format.json" {
			value, err := k.kv.Get(src, *bufp)
			if err != nil {
				return err
			}
			err = k.kv.Put(dst, value)
			if err != nil {
				return err
			}
			err = k.kv.Delete(src)
			return err
		}
		entry, err := k.getKVNSEntry(src)
		if err != nil {
			return err
		}
		entry.Key = dst
		value, err := KVNSEntryMarshal(entry, *bufp)
		if err != nil {
			return err
		}
		err = k.kv.Put(dst, value)
		if err != nil {
			return err
		}
                if (data_chunk_delete) {
                  for _, id := range entry.IDs {
                    k.kv.Delete(k.DataKey(id))
                  }
                }

		err = k.kv.Delete(src)
		return err
	}

        var del_chunk bool = true
        if ((srcVolume == minioMetaTmpBucket) || (srcVolume == minioMetaMultipartBucket)) {
          del_chunk = false
        }

	if !strings.HasSuffix(srcPath, slashSeparator) && !strings.HasSuffix(dstPath, slashSeparator) {
		return rename(pathJoin(srcVolume, srcPath), pathJoin(dstVolume, dstPath), del_chunk)
	}
	if strings.HasSuffix(srcPath, slashSeparator) && strings.HasSuffix(dstPath, slashSeparator) {
		entries, err := k.ListDirForRename(srcVolume, srcPath, -1)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if err = rename(pathJoin(srcVolume, srcPath, entry), pathJoin(dstVolume, dstPath, entry), del_chunk); err != nil {
				return err
			}
		}
		return nil
	}

	return errUnexpected
}

func (k *KVStorage) StatFile(volume string, path string) (fi FileInfo, err error) {

       //buf_trace := make([]byte, 1<<16)
       //runtime.Stack(buf_trace, true)
       //fmt.Printf("%s", buf_trace)


        //fmt.Printf("In StatFile, volume = %s, path = %s\n", volume, path)

	//if err := k.verifyVolume(volume); err != nil {
	//	return fi, err
	//}
	nskey := pathJoin(volume, path)
	entry, err := k.getKVNSEntry(nskey)
	if err != nil {
		return fi, err
	}

	return FileInfo{
		Volume:  volume,
		Name:    path,
		ModTime: entry.ModTime,
		Size:    entry.Size,
		Mode:    0,
	}, nil
}

func (k *KVStorage) die(key string, err error) {
	fmt.Println("GET corrupted", k.path, key, err)
	os.Exit(1)
}

func (k *KVStorage) DeleteFile(volume string, path string) (err error) {
	if err := k.verifyVolume(volume); err != nil {
		return err
	}
	nskey := pathJoin(volume, path)
	entry, err := k.getKVNSEntry(nskey)
	if err != nil {
		return err
	}
	for _, id := range entry.IDs {
		k.kv.Delete(k.DataKey(id))
	}
	return k.kv.Delete(nskey)
}

func (k *KVStorage) WriteAll(volume string, filePath string, buf []byte) (err error) {
	if err = k.verifyVolume(volume); err != nil {
		return err
	}
	if filePath == "format.json.tmp" {
		return k.kv.Put(pathJoin(volume, filePath), buf)
	}
	return k.CreateFile(volume, filePath, int64(len(buf)), bytes.NewBuffer(buf))
}

func (k *KVStorage) ReadAll(volume string, filePath string) (buf []byte, err error) {
	//if err = k.verifyVolume(volume); err != nil {
	//	return nil, err
	//}

	if filePath == "format.json" {
		//bufp := kvValuePool.Get().(*[]byte)
		//defer kvValuePool.Put(bufp)
                bufp := kvValuePoolMeta.Get().(*[]byte)
                defer kvValuePoolMeta.Put(bufp)

		buf, err = k.kv.Get(pathJoin(volume, filePath), *bufp)
		if err != nil {
			return nil, err
		}
		newBuf := make([]byte, len(buf))
		copy(newBuf, buf)
		return newBuf, err
	}
	//fi, err := k.StatFile(volume, filePath)
	//if err != nil {
	//	return nil, err
	//}
        //fmt.Println("### Calling from ReadAll, volume, filePath, path", volume, filePath, k.path)
	//r, err := k.ReadFileStream(volume, filePath, 0, fi.Size)
        //debug.PrintStack()
	r, err := k.ReadFileStream(volume, filePath, 0, -1)
	if (r == nil || err != nil) {
                fmt.Println("??? Got error while ReadFileStream ???", volume, filePath, err)
                if (r != nil) {
                  r.Close()
                }
		return nil, err
	}
	r_b, err_r := ioutil.ReadAll(r)
	if err_r != nil {
	  fmt.Println("??? Got error during read ???", err_r, pathJoin(volume, filePath))
          r.Close()
          return nil, err_r
	}
        r.Close()
	return r_b, nil
}

func (k *KVStorage) UpdateStats() error {

  k.kv.UpdateStats()
  return nil
}

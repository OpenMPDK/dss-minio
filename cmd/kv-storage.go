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

func (k *KVStorage) Close() error {
	return nil
}

func (k *KVStorage) DiskInfo() (info DiskInfo, err error) {
	return k.kv.DiskInfo()
}

func (k *KVStorage) loadVolumes() (*kvVolumes, error) {
	volumes := &kvVolumes{}
	//bufp := kvValuePool.Get().(*[]byte)
	//defer kvValuePool.Put(bufp)
        bufp := kvValuePoolMeta.Get().(*[]byte)
        defer kvValuePoolMeta.Put(bufp)

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
		return nil, err
	}
        //fmt.Println("Num list entries from ListDir = ", len(entries))
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
        
}

func NewReader_kv(key string, k *KVStorage, entry KVNSEntry, offset, length int64) *Reader {
        kvMaxValueSize := int64(kvMaxValueSize)
        startIndex := offset / kvMaxValueSize
        endIndex := (offset + length) / kvMaxValueSize
        index := startIndex
        pool_buf := kvValuePool.Get().(*[]byte)
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
        data_b, err := k.kv.Get(k.DataKey(id), *pool_buf)
        if err != nil {
          fmt.Println("###Error during kv get", key, err)
        }

        index++
	return &Reader{key, data_b[blockOffset : blockOffset+blockLength], pool_buf, entry, k, 0, 0, offset, length, startIndex, endIndex, kvMaxValueSize, index, false}
}

func (r *Reader) Close() error {
  if (!r.is_freed) {
    kvValuePool.Put(r.pool_buf)
    r.is_freed = true
  }

  return nil
}


func (r *Reader) Read(p []byte) (n int, err error) {
 
       
        n = 0
        var bytes_copied int = 0
        var read_next_chunk bool = false

        if (r.total_read >= r.length) {
	  err = io.EOF
	  return
        } else {
            if r.readIndex >= int64(len(r.valid_data)) {
              read_next_chunk = true
            } else {
              remaining := int64(len(r.valid_data)) - r.readIndex
              if ((remaining > 0) && (int64(len(p)) > remaining)) {
                bytes_copied = copy(p[:remaining], r.valid_data[r.readIndex:])
                r.readIndex += int64(bytes_copied)
                r.total_read += int64(bytes_copied)
                n = bytes_copied
                if (r.total_read < r.length) {
                  read_next_chunk = true
                } else {
                  return

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
          data_b, err := r.k.kv.Get(r.k.DataKey(id), *r.pool_buf)
          if err != nil {
            fmt.Println("###Error during kv get", r.key_name, err)
            
            return n, err
          }
          r.index++
          r.valid_data = data_b[blockOffset : blockOffset+blockLength]
          if (len(r.valid_data) == 0) {
            fmt.Println("##### KV GET failed with 0 length object, key = ", r.k.DataKey(id))
            os.Exit(1)            
          }
          r.readIndex = 0
        }
	
	n = copy(p[bytes_copied:], r.valid_data[r.readIndex:])
	r.readIndex += int64(n)
        r.total_read += int64(n)
        n += bytes_copied
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
        return NewReader_kv(nskey, k, entry, offset, length), nil

}

func (k *KVStorage) RenameFile(srcVolume, srcPath, dstVolume, dstPath string) error {
	if err := k.verifyVolume(srcVolume); err != nil {
		return err
	}
	if err := k.verifyVolume(dstVolume); err != nil {
		return err
	}
	rename := func(src, dst string) error {
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
		err = k.kv.Delete(src)
		return err
	}
	if !strings.HasSuffix(srcPath, slashSeparator) && !strings.HasSuffix(dstPath, slashSeparator) {
		return rename(pathJoin(srcVolume, srcPath), pathJoin(dstVolume, dstPath))
	}
	if strings.HasSuffix(srcPath, slashSeparator) && strings.HasSuffix(dstPath, slashSeparator) {
		entries, err := k.ListDirForRename(srcVolume, srcPath, -1)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if err = rename(pathJoin(srcVolume, srcPath, entry), pathJoin(dstVolume, dstPath, entry)); err != nil {
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
	fi, err := k.StatFile(volume, filePath)
	if err != nil {
		return nil, err
	}
	r, err := k.ReadFileStream(volume, filePath, 0, fi.Size)
	if err != nil {
                fmt.Println("??? Got error while ReadFileStream ???", err)
                r.Close()
		return nil, err
	}
	r_b, err_r := ioutil.ReadAll(r)
	if err_r != nil {
	  fmt.Println("??? Got error during read ???", err_r)
          r.Close()
          return nil, err_r
	}
        r.Close()
	return r_b, nil
}

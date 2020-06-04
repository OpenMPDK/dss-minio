package cmd

/*
#include <stdio.h>
#include <stdlib.h>
#include "nkv_api.h"
#include "nkv_result.h"

struct minio_nkv_handle {
  uint64_t nkv_handle;
  uint64_t container_hash;
  uint64_t network_path_hash;
};

static int minio_nkv_open(char *config, uint64_t *nkv_handle) {
  uint64_t instance_uuid = 0;
  nkv_result result;
  result = nkv_open(config, "minio", "msl-ssg-sk01", 1023, &instance_uuid, nkv_handle);
  return result;
}

static int minio_nkv_open_path(struct minio_nkv_handle *handle, char *mount_point) {
  uint32_t index = 0;
  uint32_t cnt_count = NKV_MAX_ENTRIES_PER_CALL;
  nkv_container_info *cntlist = malloc(sizeof(nkv_container_info)*NKV_MAX_ENTRIES_PER_CALL);
  memset(cntlist, 0, sizeof(nkv_container_info) * NKV_MAX_ENTRIES_PER_CALL);

  for (int i = 0; i < NKV_MAX_ENTRIES_PER_CALL; i++) {
    cntlist[i].num_container_transport = NKV_MAX_CONT_TRANSPORT;
    cntlist[i].transport_list = malloc(sizeof(nkv_container_transport)*NKV_MAX_CONT_TRANSPORT);
    memset(cntlist[i].transport_list, 0, sizeof(nkv_container_transport)*NKV_MAX_CONT_TRANSPORT);
  }

  int result = nkv_physical_container_list (handle->nkv_handle, index, cntlist, &cnt_count);
  if (result != 0) {
    printf("NKV getting physical container list failed !!, error = %d\n", result);
    exit(1);
  }

  for (uint32_t i = 0; i < cnt_count; i++) {
    for (int p = 0; p < cntlist[i].num_container_transport; p++) {
      printf("Transport information :: hash = %lu, id = %d, address = %s, port = %d, family = %d, speed = %d, status = %d, numa_node = %d\n",
              cntlist[i].transport_list[p].network_path_hash, cntlist[i].transport_list[p].network_path_id, cntlist[i].transport_list[p].ip_addr,
              cntlist[i].transport_list[p].port, cntlist[i].transport_list[p].addr_family, cntlist[i].transport_list[p].speed,
              cntlist[i].transport_list[p].status, cntlist[i].transport_list[p].numa_node);
      if(!strcmp(cntlist[i].transport_list[p].mount_point, mount_point)) {
              handle->container_hash = cntlist[i].container_hash;
              handle->network_path_hash = cntlist[i].transport_list[p].network_path_hash;
              return 0;
      }
    }
  }
  return 1;
}

static int minio_nkv_put(struct minio_nkv_handle *handle, void *key, int keyLen, void *value, int valueLen) {
  nkv_result result;
  nkv_io_context ctx;
  ctx.is_pass_through = 1;
  ctx.container_hash = handle->container_hash;
  ctx.network_path_hash = handle->network_path_hash;
  ctx.ks_id = 0;

  const nkv_key  nkvkey = {key, keyLen};
  nkv_store_option option = {0};
  nkv_value nkvvalue = {value, valueLen, 0};
  result = nkv_store_kvp(handle->nkv_handle, &ctx, &nkvkey, &option, &nkvvalue);
  return result;
}

static int minio_nkv_get(struct minio_nkv_handle *handle, void *key, int keyLen, void *value, int valueLen, int *actual_length) {
  nkv_result result;
  nkv_io_context ctx;
  ctx.is_pass_through = 1;
  ctx.container_hash = handle->container_hash;
  ctx.network_path_hash = handle->network_path_hash;
  ctx.ks_id = 0;

  const nkv_key  nkvkey = {key, keyLen};
  nkv_retrieve_option option = {0};

  nkv_value nkvvalue = {value, valueLen, 0};
  result = nkv_retrieve_kvp(handle->nkv_handle, &ctx, &nkvkey, &option, &nkvvalue);
  *actual_length = nkvvalue.actual_length;
  return result;
}

static int minio_nkv_delete(struct minio_nkv_handle *handle, void *key, int keyLen) {
  nkv_result result;
  nkv_io_context ctx;
  ctx.is_pass_through = 1;
  ctx.container_hash = handle->container_hash;
  ctx.network_path_hash = handle->network_path_hash;
  ctx.ks_id = 0;

  const nkv_key  nkvkey = {key, keyLen};
  result = nkv_delete_kvp(handle->nkv_handle, &ctx, &nkvkey);
  return result;
}

#define LIST_KEYS_COUNT 10000

static int minio_nkv_list(struct minio_nkv_handle *handle, void *prefix, int prefixLen, void *buf, int bufLen, int *numKeys, void **iter_context) {
  nkv_result result;
  nkv_io_context ctx;
  ctx.is_pass_through = 1;
  ctx.container_hash = handle->container_hash;
  ctx.network_path_hash = handle->network_path_hash;
  ctx.ks_id = 0;

  uint32_t max_keys = LIST_KEYS_COUNT;
  nkv_key keys_out[LIST_KEYS_COUNT];
  char keys[LIST_KEYS_COUNT][256];
  for (int iter = 0; iter < LIST_KEYS_COUNT; iter++) {
    memset(&keys_out[iter], 0, sizeof(nkv_key));
    memset(keys[iter], 0, 256);
    keys_out[iter].key = keys[iter];
    keys_out[iter].length = 256;
  }
  char prefixStr[257];
  memset(prefixStr, 0, 257);
  strncpy(prefixStr, prefix, prefixLen);
  result = nkv_indexing_list_keys(handle->nkv_handle, &ctx, NULL, prefixStr, "/", NULL, &max_keys, keys_out, iter_context);
  *numKeys = (int)max_keys;
  char *bufChar = (char *) buf;
  for (int iter = 0; iter < *numKeys; iter++) {
    strncpy(bufChar, keys_out[iter].key, keys_out[iter].length);
    bufChar += keys_out[iter].length;
    *bufChar = 0;
    bufChar++;
  }
  return result;
}

extern void minio_nkv_callback(void *, int, int);

static void nkv_aio_complete (nkv_aio_construct* op_data, int32_t num_op) {
  if (!op_data) {
    printf("NKV Async IO returned NULL op_data");
    exit(1);
  }
  uint64_t actual_length = 0;
  if (op_data->result == 0 && op_data->opcode == 0) {
    actual_length = op_data->value.actual_length;
  }
  minio_nkv_callback(op_data->private_data_1, op_data->result, actual_length);
  free(op_data->private_data_2);
}

static int minio_nkv_put_async(struct minio_nkv_handle *handle, void *id, void *key, int keyLen, void *value, int valueLen) {
  nkv_postprocess_function *pfn = (nkv_postprocess_function *)malloc(sizeof(nkv_postprocess_function));
  pfn->nkv_aio_cb = nkv_aio_complete;
  pfn->private_data_1 = id;
  pfn->private_data_2 = (void*)pfn;

  nkv_io_context ctx;
  ctx.is_pass_through = 1;
  ctx.container_hash = handle->container_hash;
  ctx.network_path_hash = handle->network_path_hash;
  ctx.ks_id = 0;

  const nkv_key  nkvkey = {key, keyLen};
  nkv_store_option option = {0};
  nkv_value nkvvalue = {value, valueLen, 0};

  nkv_result result = nkv_store_kvp_async(handle->nkv_handle, &ctx, &nkvkey, &option, &nkvvalue, pfn);
  return result;
}

static int minio_nkv_get_async(struct minio_nkv_handle *handle, void *id, void *key, int keyLen, void *value, int valueLen) {
  nkv_postprocess_function *pfn = (nkv_postprocess_function *)malloc(sizeof(nkv_postprocess_function));
  pfn->nkv_aio_cb = nkv_aio_complete;
  pfn->private_data_1 = id;
  pfn->private_data_2 = (void*)pfn;

  nkv_io_context ctx;
  ctx.is_pass_through = 1;
  ctx.container_hash = handle->container_hash;
  ctx.network_path_hash = handle->network_path_hash;
  ctx.ks_id = 0;

  const nkv_key  nkvkey = {key, keyLen};
  nkv_retrieve_option option = {0};
  nkv_value nkvvalue = {value, valueLen, 0};

  nkv_result result = nkv_retrieve_kvp_async(handle->nkv_handle, &ctx, &nkvkey, &option, &nkvvalue, pfn);
  return result;
}

static int minio_nkv_delete_async(struct minio_nkv_handle *handle, void *id, void *key, int keyLen) {
  nkv_postprocess_function *pfn = (nkv_postprocess_function *)malloc(sizeof(nkv_postprocess_function));
  pfn->nkv_aio_cb = nkv_aio_complete;
  pfn->private_data_1 = id;
  pfn->private_data_2 = (void*)pfn;

  nkv_io_context ctx;
  ctx.is_pass_through = 1;
  ctx.container_hash = handle->container_hash;
  ctx.network_path_hash = handle->network_path_hash;
  ctx.ks_id = 0;

  const nkv_key  nkvkey = {key, keyLen};

  nkv_result result = nkv_delete_kvp_async(handle->nkv_handle, &ctx, &nkvkey, pfn);
  return result;
}

static int minio_nkv_diskinfo(struct minio_nkv_handle *handle, long long *total, long long *used) {
  nkv_mgmt_context mg_ctx = {0};
  mg_ctx.is_pass_through = 1;

  mg_ctx.container_hash = handle->container_hash;
  mg_ctx.network_path_hash = handle->network_path_hash;

  nkv_path_stat p_stat = {0};
  nkv_result stat = nkv_get_path_stat (handle->nkv_handle, &mg_ctx, &p_stat);

  if (stat == NKV_SUCCESS) {
    *total = (long long)p_stat.path_storage_capacity_in_bytes;
    *used = (long long)p_stat.path_storage_usage_in_bytes;
  }
  return stat;
}

*/
import "C"

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"encoding/hex"
	"encoding/json"

	humanize "github.com/dustin/go-humanize"
	"github.com/minio/minio/cmd/logger"
        "sync/atomic"
)

//export minio_nkv_callback
func minio_nkv_callback(id unsafe.Pointer, result C.int, actualLength C.int) {
	globalAsyncKVResponseCh <- asyncKVResponse{uint64(uintptr(id)), int(result), int(actualLength)}
}

var kvTimeout time.Duration = func() time.Duration {
	timeoutStr := os.Getenv("MINIO_NKV_TIMEOUT")
	if timeoutStr == "" {
		return time.Duration(10) * time.Second
	}
	i, err := strconv.Atoi(timeoutStr)
	if err != nil {
		fmt.Println("MINIO_NKV_TIMEOUT is incorrect", timeoutStr, err)
		os.Exit(1)
	}
	return time.Duration(i) * time.Second
}()

var kvPadding bool = os.Getenv("MINIO_NKV_PADDING") != "off"

var kvMaxValueSize = getKVMaxValueSize()

func getKVMaxValueSize() int {
	str := os.Getenv("MINIO_NKV_MAX_VALUE_SIZE")
	if str == "" {
		return 2 * 1024 * 1024
	}
	valSize, err := strconv.Atoi(str)
	logger.FatalIf(err, "parsing MINIO_NKV_MAX_VALUE_SIZE")
	return valSize
}

var kvChecksum = os.Getenv("MINIO_NKV_CHECKSUM") != ""
var use_custome_reader = os.Getenv("MINIO_NKV_USE_CUSTOM_READER") != ""

var globalNKVHandle C.uint64_t

func minio_nkv_open(configPath string) error {
	if globalNKVHandle != 0 {
		return nil
	}
	go kvAsyncLoop()
	cs := C.CString(configPath)
	status := C.minio_nkv_open(cs, &globalNKVHandle)
	C.free(unsafe.Pointer(cs))
	if status != 0 {
		return errDiskNotFound
	}
	return nil
}

func newKV(path string, sync bool) (*KV, error) {
	kv := &KV{}
	kv.path = path
	kv.sync = sync
	kv.handle.nkv_handle = globalNKVHandle
	kv.kvHashSumMap = make(map[string]*kvHashSum)
	cs := C.CString(path)
	status := C.minio_nkv_open_path(&kv.handle, C.CString(path))
	C.free(unsafe.Pointer(cs))
	if status != 0 {
		fmt.Println("unable to open", path, status)
		return nil, errors.New("unable to open device")
	}
	return kv, nil
}

type kvHashSum struct {
	sum string
	sync.RWMutex
}

type KV struct {
	handle         C.struct_minio_nkv_handle
	path           string
	sync           bool
	kvHashSumMap   map[string]*kvHashSum
	kvHashSumMapMu sync.RWMutex
}

type kvValuePoolType struct {
       *sync.Pool
       count uint64
       sync.Mutex
}

func (k *kvValuePoolType) Get() interface{} {
       //k.Lock()
       //k.count++
       //k.Unlock()
       atomic.AddUint64(&k.count, 1) 
       return k.Pool.Get()
}

func (k *kvValuePoolType) Put(x interface{}) {
       //k.Lock()
       //k.count--
       //k.Unlock()
       atomic.AddUint64(&k.count, ^uint64(0))
       k.Pool.Put(x)
}

func (k *kvValuePoolType) PrintCount() {
       //k.Lock()
       //count := k.count
       //k.Unlock()
       fmt.Println("Pool count", k.count)
}

var kvValuePool = &kvValuePoolType{
       Pool: &sync.Pool{
               New: func() interface{} {
                       b := make([]byte, kvMaxValueSize)
                       return &b
               },
        },
 }


//var kvValuePool = sync.Pool{
//	New: func() interface{} {
//		b := make([]byte, kvMaxValueSize)
//		return &b
//	},
//}

var kvValuePoolMeta = sync.Pool{
        New: func() interface{} {
                b := make([]byte, 4096)
                return &b
        },
}

var kvValuePoolList = sync.Pool{
        New: func() interface{} {
                b := make([]byte, 131072)
                return &b
        },
}

const kvKeyLength = 255

var kvMu sync.Mutex
var kvSerialize = os.Getenv("MINIO_NKV_SERIALIZE") != ""

type kvCallType string

const (
	kvCallPut kvCallType = "put"
	kvCallGet            = "get"
	kvCallDel            = "delete"
)

type asyncKVLoopRequest struct {
	call  kvCallType
	kv    *KV
	key   []byte
	value []byte
	ch    chan asyncKVLoopResponse
}

type asyncKVLoopResponse struct {
	status       int
	actualLength int
}

type asyncKVResponse struct {
	id           uint64
	status       int
	actualLength int
}

var globalAsyncKVLoopRequestCh chan asyncKVLoopRequest
var globalAsyncKVResponseCh chan asyncKVResponse
var globalAsyncKVLoopEndCh chan struct{}
var globalDumpKVStatsCh chan chan []byte

type kvDeviceStats struct {
	PendingPuts    uint64
	PendingGets    uint64
	PendingDeletes uint64

	TotalPutsCount    uint64
	TotalGetsCount    uint64
	TotalDeletesCount uint64

	totalPutsSize uint64
	TotalPutsSize string
	totalGetsSize uint64
	TotalGetsSize string

	ThroughputPuts string
	ThroughputGets string

	currentPutsSum uint64
	currentGetsSum uint64
}

func kvAsyncLoop() {
	runtime.LockOSThread()
	globalAsyncKVLoopRequestCh = make(chan asyncKVLoopRequest)
	globalAsyncKVResponseCh = make(chan asyncKVResponse)
	globalAsyncKVLoopEndCh = make(chan struct{})
	globalDumpKVStatsCh = make(chan chan []byte)

	statsMap := make(map[string]*kvDeviceStats)

	getStatsStruct := func(path string) *kvDeviceStats {
		stats, ok := statsMap[path]
		if ok {
			return stats
		}
		stats = &kvDeviceStats{}
		statsMap[path] = stats
		return stats
	}

	ticker := time.NewTicker(time.Second)

	var id uint64
	idMap := make(map[uint64]asyncKVLoopRequest)
	for {
		select {
		case request := <-globalAsyncKVLoopRequestCh:
			id++
			idMap[id] = request
			t := time.AfterFunc(kvTimeout, func() {
				fmt.Println("##### timeout while calling KV", request.call, string(request.key))
			})
			switch request.call {
			case kvCallPut:
				getStatsStruct(request.kv.path).PendingPuts++
				C.minio_nkv_put_async(&request.kv.handle, unsafe.Pointer(uintptr(id)), unsafe.Pointer(uintptr(unsafe.Pointer(&request.key[0]))), C.int(len(request.key)), unsafe.Pointer(uintptr(unsafe.Pointer(&request.value[0]))), C.int(len(request.value)))
			case kvCallGet:
				getStatsStruct(request.kv.path).PendingGets++
				C.minio_nkv_get_async(&request.kv.handle, unsafe.Pointer(uintptr(id)), unsafe.Pointer(uintptr(unsafe.Pointer(&request.key[0]))), C.int(len(request.key)), unsafe.Pointer(uintptr(unsafe.Pointer(&request.value[0]))), C.int(len(request.value)))
			case kvCallDel:
				getStatsStruct(request.kv.path).PendingDeletes++
				C.minio_nkv_delete_async(&request.kv.handle, unsafe.Pointer(uintptr(id)), unsafe.Pointer(uintptr(unsafe.Pointer(&request.key[0]))), C.int(len(request.key)))
			}
			t.Stop()
		case response := <-globalAsyncKVResponseCh:
			request, ok := idMap[response.id]
			if !ok {
				fmt.Println("#####", id, "not found in the map")
				os.Exit(1)
			}
			delete(idMap, response.id)

			request.ch <- asyncKVLoopResponse{response.status, response.actualLength}

			switch request.call {
			case kvCallPut:
				stats := getStatsStruct(request.kv.path)
				stats.PendingPuts--
				stats.TotalPutsCount++
				if response.status != 0 {
					break
				}
				stats.currentPutsSum += uint64(len(request.value))
				stats.totalPutsSize += uint64(len(request.value))
			case kvCallGet:
				stats := getStatsStruct(request.kv.path)
				stats.PendingGets--
				stats.TotalGetsCount++
				if response.status != 0 {
					break
				}
				stats.currentGetsSum += uint64(response.actualLength)
				stats.totalGetsSize += uint64(response.actualLength)
			case kvCallDel:
				stats := getStatsStruct(request.kv.path)
				stats.PendingDeletes--
				stats.TotalDeletesCount++
			}

		case <-globalAsyncKVLoopEndCh:
			fmt.Println("Pending requests:", len(idMap))
			for id, request := range idMap {
				fmt.Printf("private_data_1=%x key=%s request=%s", id, string(request.key), request.call)
			}
			os.Exit(1)
		case <-ticker.C:
			for _, stats := range statsMap {
				stats.ThroughputPuts = humanize.Bytes(stats.currentPutsSum) + "/sec"
				stats.ThroughputGets = humanize.Bytes(stats.currentGetsSum) + "/sec"
				stats.currentPutsSum = 0
				stats.currentGetsSum = 0
			}
		case dumpStatsCh := <-globalDumpKVStatsCh:
			for _, stats := range statsMap {
				stats.TotalPutsSize = humanize.Bytes(stats.totalPutsSize)
				stats.TotalGetsSize = humanize.Bytes(stats.totalGetsSize)
			}
			b, err := json.MarshalIndent(statsMap, "", "    ")
			if err != nil {
				dumpStatsCh <- []byte(err.Error())
				continue
			}
			dumpStatsCh <- b
		}
	}
}

func (k *KV) Put(keyStr string, value []byte) error {
	if !strings.HasPrefix(keyStr, kvDataDir) {
		keyStr = pathJoin(kvMetaDir, keyStr)
	}
	if kvSerialize {
		kvMu.Lock()
		defer kvMu.Unlock()
	}
	if len(value) > kvMaxValueSize {
		return errValueTooLong
	}
	key := []byte(keyStr)
	// if kvPadding {
	// 	for len(key) < kvKeyLength {
	// 		key = append(key, '\x00')
	// 	}
	// }
	if len(key) > kvKeyLength {
		fmt.Println("##### invalid key length", key, len(key))
		os.Exit(0)
	}
	var valuePtr unsafe.Pointer
	if len(value) > 0 {
		valuePtr = unsafe.Pointer(&value[0])
	}
	var hashSum *kvHashSum
	if kvChecksum {
		k.kvHashSumMapMu.Lock()
		hashSum = k.kvHashSumMap[keyStr]
		if hashSum == nil {
			hashSum = new(kvHashSum)
			k.kvHashSumMap[keyStr] = hashSum
		}
		k.kvHashSumMapMu.Unlock()
		hashSum.Lock()
		defer hashSum.Unlock()
	}
	var status int
	if k.sync {
		cstatus := C.minio_nkv_put(&k.handle, unsafe.Pointer(&key[0]), C.int(len(key)), valuePtr, C.int(len(value)))
		status = int(cstatus)
	} else {
		ch := make(chan asyncKVLoopResponse, 1)
		var response asyncKVLoopResponse
		select {
		case globalAsyncKVLoopRequestCh <- asyncKVLoopRequest{call: kvCallPut, kv: k, key: key, value: value, ch: ch}:
		case <-time.After(kvTimeout):
			fmt.Println("##### Put timeout on globalAsyncKVRequestCh", k.path, keyStr)
			globalAsyncKVLoopEndCh <- struct{}{}
			time.Sleep(time.Hour)
			return errDiskNotFound
		}

		select {
		case response = <-ch:
		case <-time.After(kvTimeout):
			fmt.Println("##### Put timeout", k.path, keyStr)
			globalAsyncKVLoopEndCh <- struct{}{}
			time.Sleep(time.Hour)
			return errDiskNotFound
		}
		status = response.status
	}

	if status != 0 {
		return errors.New("error during put")
	}
	if kvChecksum {
		sum := HighwayHash256.New()
		sum.Write(value)
		hashSum.sum = hex.EncodeToString(sum.Sum(nil))
	}
	return nil
}

func (k *KV) Get(keyStr string, value []byte) ([]byte, error) {
        
	if !strings.HasPrefix(keyStr, kvDataDir) {
		keyStr = pathJoin(kvMetaDir, keyStr)
	}
	if kvSerialize {
		kvMu.Lock()
		defer kvMu.Unlock()
	}
	key := []byte(keyStr)
	// if kvPadding {
	// 	for len(key) < kvKeyLength {
	// 		key = append(key, '\x00')
	// 	}
	// }
	if len(key) > kvKeyLength {
		fmt.Println("##### invalid key length", key, len(key))
		os.Exit(0)
	}
	var actualLength int

	var hashSum *kvHashSum
	if kvChecksum {
		k.kvHashSumMapMu.Lock()
		hashSum = k.kvHashSumMap[keyStr]
		if hashSum == nil {
			hashSum = new(kvHashSum)
			k.kvHashSumMap[keyStr] = hashSum
		}
		k.kvHashSumMapMu.Unlock()
		hashSum.Lock()
		defer hashSum.Unlock()
	}
	tries := 10
	for {
		status := 1
		if k.sync {
			var actualLengthCint C.int
			cstatus := C.minio_nkv_get(&k.handle, unsafe.Pointer(&key[0]), C.int(len(key)), unsafe.Pointer(&value[0]), C.int(len(value)), &actualLengthCint)
			status = int(cstatus)
			actualLength = int(actualLengthCint)
		} else {
			ch := make(chan asyncKVLoopResponse, 1)
			var response asyncKVLoopResponse
			select {
			case globalAsyncKVLoopRequestCh <- asyncKVLoopRequest{call: kvCallGet, kv: k, key: key, value: value, ch: ch}:
			case <-time.After(kvTimeout):
				fmt.Println("##### Get timeout on globalAsyncKVRequestCh", k.path, keyStr)
				globalAsyncKVLoopEndCh <- struct{}{}
				time.Sleep(time.Hour)
				os.Exit(1)
			}

			select {
			case response = <-ch:
			case <-time.After(kvTimeout):
				fmt.Println("##### Get timeout", k.path, keyStr)
				globalAsyncKVLoopEndCh <- struct{}{}
				time.Sleep(time.Hour)
				os.Exit(1)
				return nil, errDiskNotFound
			}
			status = response.status
			if status == 0 {
				actualLength = response.actualLength
			}

		}
                /*if (status == NKV_ERR_KEY_NOT_EXIST) {
                  return nil, errDiskAccessDenied
                }*/

		if status != 0 {
			return nil, errFileNotFound
		}
		if actualLength > 0 {
			break
		}
		fmt.Println("##### GET returned actual_length=", actualLength)
		tries--
		if tries == 0 {
			fmt.Println("##### GET failed (after 10 retries) on (actual_length=0)", k.path, keyStr)
			os.Exit(1)
		}
	}
	if kvChecksum {
		if hashSum.sum != "" {
			sum := HighwayHash256.New()
			sum.Write(value[:actualLength])
			if hashSum.sum != hex.EncodeToString(sum.Sum(nil)) {
				fmt.Printf("Value content mismatch: (%s) (%s), (expected:%s != got:%s)\n", k.path, keyStr, hashSum.sum, hex.EncodeToString(sum.Sum(nil)))
			}
		}
	}
	return value[:actualLength], nil
}

func (k *KV) Delete(keyStr string) error {
	if !strings.HasPrefix(keyStr, kvDataDir) {
		keyStr = pathJoin(kvMetaDir, keyStr)
	}
	if kvSerialize {
		kvMu.Lock()
		defer kvMu.Unlock()
	}
	key := []byte(keyStr)
	// if kvPadding {
	// 	for len(key) < kvKeyLength {
	// 		key = append(key, '\x00')
	// 	}
	// }
	if len(key) > kvKeyLength {
		fmt.Println("##### invalid key length", key, len(key))
		os.Exit(0)
	}
	var hashSum *kvHashSum
	if kvChecksum {
		k.kvHashSumMapMu.Lock()
		hashSum = k.kvHashSumMap[keyStr]
		if hashSum == nil {
			hashSum = new(kvHashSum)
			k.kvHashSumMap[keyStr] = hashSum
		}
		k.kvHashSumMapMu.Unlock()
		hashSum.Lock()
		defer hashSum.Unlock()
	}

	var status int
	if k.sync {
		cstatus := C.minio_nkv_delete(&k.handle, unsafe.Pointer(&key[0]), C.int(len(key)))
		status = int(cstatus)
	} else {
		ch := make(chan asyncKVLoopResponse, 1)
		var response asyncKVLoopResponse
		select {
		case globalAsyncKVLoopRequestCh <- asyncKVLoopRequest{call: kvCallDel, kv: k, key: key, ch: ch}:
		case <-time.After(kvTimeout):
			fmt.Println("##### Delete timeout on globalAsyncKVRequestCh", k.path, keyStr)
			globalAsyncKVLoopEndCh <- struct{}{}
			time.Sleep(time.Hour)
			os.Exit(1)
		}
		select {
		case response = <-ch:
		case <-time.After(kvTimeout):
			fmt.Println("##### Delete timeout", k.path, keyStr)
			globalAsyncKVLoopEndCh <- struct{}{}
			time.Sleep(time.Hour)
			os.Exit(1)
			return errDiskNotFound
		}
		status = response.status
	}
	if kvChecksum {
		k.kvHashSumMapMu.Lock()
		delete(k.kvHashSumMap, keyStr)
		k.kvHashSumMapMu.Unlock()
	}
	if status != 0 {
		return errFileNotFound
	}
	return nil
}

func (k *KV) List(keyStr string, b []byte) ([]string, error) {
	if kvSerialize {
		kvMu.Lock()
		defer kvMu.Unlock()
	}
	keyStr = pathJoin(kvMetaDir, keyStr)
	if !strings.HasSuffix(keyStr, slashSeparator) {
		keyStr += slashSeparator
	}
	key := []byte(keyStr)
	var numKeysC C.int
	var numKeys int
	var entries []string
	var iterContext unsafe.Pointer
	for {
		buf := b
		cstatus := C.minio_nkv_list(&k.handle, unsafe.Pointer(&key[0]), C.int(len(key)), unsafe.Pointer(&buf[0]), C.int(len(buf)), &numKeysC, &iterContext)
		if cstatus != 0 && cstatus != 0x01F {
			return nil, errFileNotFound
		}
		numKeys = int(numKeysC)
		for i := 0; i < numKeys; i++ {
			index := bytes.IndexByte(buf, '\x00')
			if index == -1 {
				break
			}
			entries = append(entries, string(buf[:index]))
			buf = buf[index+1:]
		}

		if cstatus == 0 {
			break
		}
	}
	return entries, nil
}

func (k *KV) DiskInfo() (DiskInfo, error) {
	var total C.longlong
	var used C.longlong

	status := C.minio_nkv_diskinfo(&k.handle, &total, &used)

	if status != 0 {
		return DiskInfo{}, errDiskNotFound
	}

	return DiskInfo{
		Total:    uint64(total),
		Free:     uint64(total - used),
		Used:     uint64(used),
		RootDisk: false,
	}, nil
}

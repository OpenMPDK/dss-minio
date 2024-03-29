/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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
	"context"
	"hash/crc32"
	"io"
        "fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
        "os"
        "strconv"
	"crypto/sha1"
	"encoding/hex"
	"bytes"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/bpool"
	"github.com/minio/minio/pkg/madmin"
	"github.com/minio/minio/pkg/policy"
	"github.com/minio/minio/pkg/sync/errgroup"
)

// setsStorageAPI is encapsulated type for Close()
type setsStorageAPI [][]StorageAPI

func (s setsStorageAPI) Close() error {
	for i := 0; i < len(s); i++ {
		for _, disk := range s[i] {
			if disk == nil {
				continue
			}
			disk.Close()
		}
	}
	return nil
}

// xlSets implements ObjectLayer combining a static list of erasure coded
// object sets. NOTE: There is no dynamic scaling allowed or intended in
// current design.
type xlSets struct {
	sets []*xlObjects

	// Reference format.
	format *formatXLV3

	// xlDisks mutex to lock xlDisks.
	xlDisksMu sync.RWMutex

	// Re-ordered list of disks per set.
	xlDisks setsStorageAPI

	// List of endpoints provided on the command line.
	endpoints EndpointList

	// Total number of sets and the number of disks per set.
	setCount, drivesPerSet int

	// Done channel to control monitoring loop.
	disksConnectDoneCh chan struct{}

	// Distribution algorithm of choice.
	distributionAlgo string

	// Pack level listObjects pool management.
	listPool *treeWalkPool
}

// isConnected - checks if the endpoint is connected or not.
func (s *xlSets) isConnected(endpoint Endpoint) bool {
	s.xlDisksMu.RLock()
	defer s.xlDisksMu.RUnlock()

	for i := 0; i < s.setCount; i++ {
		for j := 0; j < s.drivesPerSet; j++ {
			if s.xlDisks[i][j] == nil {
				continue
			}
			var endpointStr string
			if endpoint.IsLocal {
				endpointStr = endpoint.Path
			} else {
				endpointStr = endpoint.String()
			}
			if s.xlDisks[i][j].String() != endpointStr {
				continue
			}
			return s.xlDisks[i][j].IsOnline()
		}
	}
	return false
}

// Initializes a new StorageAPI from the endpoint argument, returns
// StorageAPI and also `format` which exists on the disk.
func connectEndpoint(endpoint Endpoint) (StorageAPI, *formatXLV3, error) {
	disk, err := newStorageAPI(endpoint)
	if err != nil {
		return nil, nil, err
	}

	format, err := loadFormatXL(disk)
	if err != nil {
		// Close the internal connection to avoid connection leaks.
		disk.Close()
		return nil, nil, err
	}

	return disk, format, nil
}

// findDiskIndex - returns the i,j'th position of the input `format` against the reference
// format, after successful validation.
func findDiskIndex(refFormat, format *formatXLV3) (int, int, error) {
	if err := formatXLV3Check(refFormat, format); err != nil {
		return 0, 0, err
	}

	if format.XL.This == offlineDiskUUID {
		return -1, -1, fmt.Errorf("diskID: %s is offline", format.XL.This)
	}

	for i := 0; i < len(refFormat.XL.Sets); i++ {
		for j := 0; j < len(refFormat.XL.Sets[0]); j++ {
			if refFormat.XL.Sets[i][j] == format.XL.This {
				return i, j, nil
			}
		}
	}

	return -1, -1, fmt.Errorf("diskID: %s not found", format.XL.This)
}

// Re initializes all disks based on the reference format, this function is
// only used by HealFormat and ReloadFormat calls.
func (s *xlSets) reInitDisks(refFormat *formatXLV3, storageDisks []StorageAPI, formats []*formatXLV3) [][]StorageAPI {
	xlDisks := make([][]StorageAPI, s.setCount)
	for i := 0; i < len(refFormat.XL.Sets); i++ {
		xlDisks[i] = make([]StorageAPI, s.drivesPerSet)
	}
	for k := range storageDisks {
		if storageDisks[k] == nil || formats[k] == nil {
			continue
		}
		i, j, err := findDiskIndex(refFormat, formats[k])
		if err != nil {
			reqInfo := (&logger.ReqInfo{}).AppendTags("storageDisk", storageDisks[i].String())
			ctx := logger.SetReqInfo(context.Background(), reqInfo)
			logger.LogIf(ctx, err)
			continue
		}
		xlDisks[i][j] = storageDisks[k]
	}
	return xlDisks
}

// connectDisksWithQuorum is same as connectDisks but waits
// for quorum number of formatted disks to be online in
// any given sets.
func (s *xlSets) connectDisksWithQuorum() {
	var onlineDisks int
        var set_index int = 0
        var drive_index int = 0
        var endpoint_index int = 0

	for onlineDisks < len(s.endpoints)/2 {
		for _, endpoint := range s.endpoints {
                        endpoint_index++
			if s.isConnected(endpoint) {
				continue
			}
			disk, format, err := connectEndpoint(endpoint)
			if err != nil {
				printEndpointError(endpoint, err)
				continue
			}
			i, j, err := findDiskIndex(s.format, format)
			if err != nil {
				// Close the internal connection to avoid connection leaks.
				disk.Close()
				printEndpointError(endpoint, err)
				continue
			}
                        if (globalNkvShared) {
                          num_sets := len(s.format.XL.Sets)
                          num_drives_per_set := len(s.format.XL.Sets[0])
                          if ((endpoint_index -1) == 0) {
                            set_index = 0
                            drive_index = 0
                          } else {

                            if ((endpoint_index -1) % num_drives_per_set == 0) {
                              set_index = ((endpoint_index -1) / num_drives_per_set)
                              drive_index = 0
                            } else {
                              drive_index++
                            }
                          }
                          i = set_index
                          j = drive_index
                          fmt.Println("In connectDisksWithQuorum, sets, driverperset, endpoint, disk, index (i,j) = ", 
                                      num_sets, num_drives_per_set, endpoint, disk, i, j)
                        }
			s.xlDisks[i][j] = disk
			onlineDisks++
		}
		// Sleep for a while - so that we don't go into
		// 100% CPU when half the disks are online.
		time.Sleep(500 * time.Millisecond)
	}
}

// connectDisks - attempt to connect all the endpoints, loads format
// and re-arranges the disks in proper position.
func (s *xlSets) connectDisks() {
        var set_index int = 0
        var drive_index int = 0
        var endpoint_index int = 0

	for _, endpoint := range s.endpoints {
                endpoint_index++
		if s.isConnected(endpoint) {
			continue
		}
		disk, format, err := connectEndpoint(endpoint)
		if err != nil {
			printEndpointError(endpoint, err)
			continue
		}
		i, j, err := findDiskIndex(s.format, format)
		if err != nil {
			// Close the internal connection to avoid connection leaks.
			disk.Close()
			printEndpointError(endpoint, err)
			continue
		}

                if (globalNkvShared) {
                  num_sets := len(s.format.XL.Sets)
                  num_drives_per_set := len(s.format.XL.Sets[0])
                  if ((endpoint_index -1) == 0) {
                    set_index = 0
                    drive_index = 0
                  } else {

                    if ((endpoint_index -1) % num_drives_per_set == 0) {
                      set_index = ((endpoint_index -1) / num_drives_per_set)
                      drive_index = 0
                    } else {
                      drive_index++
                    }
                  }
                  i = set_index
                  j = drive_index
                  fmt.Println("In connectDisks, sets, driverperset, endpoint, disk, index (i,j) = ",
                               num_sets, num_drives_per_set, endpoint, disk, i, j)
                }

		s.xlDisksMu.Lock()
		s.xlDisks[i][j] = disk
		s.xlDisksMu.Unlock()
	}
}

// monitorAndConnectEndpoints this is a monitoring loop to keep track of disconnected
// endpoints by reconnecting them and making sure to place them into right position in
// the set topology, this monitoring happens at a given monitoring interval.
func (s *xlSets) monitorAndConnectEndpoints(monitorInterval time.Duration) {
	ticker := time.NewTicker(monitorInterval)
	// Stop the timer.
	defer ticker.Stop()

	for {
		select {
		case <-GlobalServiceDoneCh:
			return
		case <-s.disksConnectDoneCh:
			return
		case <-ticker.C:
			s.connectDisks()
		}
	}
}

// GetDisks returns a closure for a given set, which provides list of disks per set.
func (s *xlSets) GetDisks(setIndex int) func() []StorageAPI {
	return func() []StorageAPI {
		s.xlDisksMu.Lock()
		defer s.xlDisksMu.Unlock()
		disks := make([]StorageAPI, s.drivesPerSet)
		copy(disks, s.xlDisks[setIndex])
		return disks
	}
}


func (s *xlSets) UpdateCountersToDisk() {
    var init_stati int = 0
    statTimeStr := os.Getenv("MINIO_NKV_STAT_INTERVAL")
    if statTimeStr == "" {
      init_stati = 10
    } else {
      i, err := strconv.Atoi(statTimeStr)
      if err != nil {
        fmt.Println("MINIO_NKV_STAT_INTERVAL is incorrect", statTimeStr, err)
        os.Exit(1)
      } else {
        init_stati = i
      }
    }
    for {
      if (report_minio_stats) {
        fmt.Println("About to update counters")
        for _, set := range s.sets {
          for _, disk := range set.getDisks() {
            if disk == nil {
              continue
            }
            if err := disk.UpdateStats(); err != nil {
              fmt.Println("Updating counter stat failed, disk = ", disk)
            } else {
              break
            }
          }
        }
      }
      fmt.Printf("Minio QD stat: TotalGetQD = %d, TotalECReqQD = %d, TotalHeadObjQD = %d\n", uint64(globalTotalGetQD), uint64(globalTotalECReqQD), uint64(globalTotalHeadObjQD))
      fmt.Printf("Minio IO count stat: TotalGets = %d, TotalHeads = %d\n", uint64(globalTotalGetIOCount), uint64(globalTotalHeadIOCount))
      time.Sleep(time.Duration(init_stati) * time.Second)
    }
}
// BW/IOPS reporting thread
func metricsReporting() {
    fmt.Println("### Metrics reporting thread started")
    for {
        // Need to use atomic operation for setting and checking whether to collect
        // It's probably fine to not use atomic within this thread, but IO threads will need it accurately determine whether to record
        // Go 1.12 doesn't have atomic structs (Go 1.19 introduced atomic structs)

        // Signal to start recording IO, then sleep for 1s and report the counters
        atomic.StoreUint32(&globalCollectMetrics, 1)
        
        time.Sleep(1 * time.Second)
        atomic.StoreUint32(&globalCollectMetrics, 0)
        // Signal to stop after collecting for 1s

        // Record counters to a variable for Prometheus to report
        atomic.StoreUint64(&globalReportPutIOPS, atomic.LoadUint64(&globalCurrPutIOPS))
        atomic.StoreUint64(&globalReportGetIOPS, atomic.LoadUint64(&globalCurrGetIOPS))
        atomic.StoreUint64(&globalReportPutBW, atomic.LoadUint64(&globalCurrPutBW))
        atomic.StoreUint64(&globalReportGetBW, atomic.LoadUint64(&globalCurrGetBW))
        atomic.StoreUint64(&globalReportDel, atomic.LoadUint64(&globalCurrDel))
        // Reset counters after recording the last second of metrics
        atomic.StoreUint64(&globalCurrPutIOPS, 0)
        atomic.StoreUint64(&globalCurrGetIOPS, 0)
        atomic.StoreUint64(&globalCurrPutBW, 0)
        atomic.StoreUint64(&globalCurrGetBW, 0)
        atomic.StoreUint64(&globalCurrDel, 0)
    }
}


func (s *xlSets) syncSharedVols() {
	var init_multi int = 0
        syncTimeStr := os.Getenv("MINIO_NKV_SHARED_SYNC_INTERVAL")
        if syncTimeStr == "" {
        	init_multi = 2
        } else {
          i, err := strconv.Atoi(syncTimeStr)
          if err != nil {
        	fmt.Println("MINIO_NKV_SHARED_SYNC_INTERVAL is incorrect", syncTimeStr, err)
    		os.Exit(1)
  	  } else {
    		init_multi = i
  	  }
        }
  	for {
                if (!globalDontUseECMemPool) {
                  kvPoolEC.PrintCount()
                  kvMetaPoolEC.PrintCount()
                }
                if (globalNkvShared) {
        	  for _, set := range s.sets {
        		for _, disk := range set.getDisks() {
                		if disk == nil {
                        		continue
                		}
	        		_ = disk.SyncVolumes()
            
                	}
        	  }
                }
        	time.Sleep(time.Duration(init_multi) * time.Second)
  	}
}

func generateUUID(data []byte) string {
	hash := sha1.New()
	// namespaceUUID as defined by UUID RFC 
	namespaceURL := []byte{107, 167, 184, 17, 157, 173, 17, 209, 128, 180, 0, 192, 79, 212, 48, 200}
	hash.Write(namespaceURL)
	hash.Write(data)
	sum := hash.Sum(nil)
	ver := 5 	// UUID version 5 (sha1 hash)
	// change bytes to show proper UUID info
	sum[6] = (sum[6] & 0x0f) | uint8((ver&0xf)<<4)
	sum[8] = (sum[8] & 0x3f) | 0x80

	var buf [36]byte
	uuid := buf[:]

	// add hyphen formatting
	hex.Encode(uuid, sum[:4])
	uuid[8] = '-'
	hex.Encode(uuid[9:13], sum[4:6])
	uuid[13] = '-'
	hex.Encode(uuid[14:18], sum[6:8])
	uuid[18] = '-'
	hex.Encode(uuid[19:23], sum[8:10])
	uuid[23] = '-'
	hex.Encode(uuid[24:], sum[10:16])

	return string(uuid)
}

// Write clusterID to all disks, if too many fail then error out
func writeClusterIDWithQuorum(disks []StorageAPI) error {
	dataDrives, _ := getRedundancyCount(standardStorageClass, len(disks))
	writeQuorum := dataDrives
	var mErrs = make([]error, len(disks))
	for index, disk := range disks {
		if disk == nil {
			mErrs[index] = errDiskNotFound
			continue
		}

		if err := disk.WriteAll(clusterIDBucket, clusterIDFilePath, []byte(clusterID)); err != nil {
			mErrs[index] = err
		}
	}
	// Check how many errors, success if error is nil for >= quorum
	maxCount, maxErr := reduceErrs(mErrs, objectOpIgnoredErrs)
	if maxCount < writeQuorum {
		return errXLWriteQuorum	
	}
	if maxErr != nil {
		return maxErr
	}
	return nil
}

// Check if stored data matches, throw an error if not enough matches
// Only checking the data, don't have to worry about errors since that is
// already checked
func getClusterIDInQuorum(uuids []string, readQuorum int) (string, error) {
	uuidCount := make(map[string]int)
	for _, uuid := range uuids {
		if uuid == "" {
			continue
		}
		uuidCount[uuid]++
	}

	maxID := ""
	maxCount := 0
	for uuid, count := range uuidCount {
		if count > maxCount {
			maxCount = count
			maxID = uuid
		}
	}
	// If the amount of a stored UUID is >= quorum, success
	if maxCount < readQuorum {
		return "", errXLReadQuorum
	}
	return maxID, nil
}

// Attempt to read the clusterID from disks in a set
// Use half the disks as quorum since we are just writing to all disks
// Throws an error if either enough reads fail or if stored UUIDs don't match
func readClusterIDWithQuorum(disks []StorageAPI) (string, error) {
	readQuorum := len(disks) / 2

	var mErrs = make([]error, len(disks))
	var uuids = make([]string, len(disks))
	for index, disk := range disks {
		if disk == nil {
			mErrs[index] = errDiskNotFound
			continue
		}
		buf, err := disk.ReadAll(clusterIDBucket, clusterIDFilePath)
		if err != nil {
			mErrs[index] = err
			continue
		}
		buf = bytes.Trim(buf, "\x00")
		uuids[index] = string(buf)
	}

	// Check if enough reads succeeded
	maxCount, maxErr := reduceErrs(mErrs, objectOpIgnoredErrs)
	if maxCount < readQuorum {
		return "", errXLReadQuorum
	}
	if maxErr != nil {
		return "", maxErr
	}
	
	fmt.Println("Read err quorum succeeded, see if there is usable clusterID")
	// Check if UUIDs match between disks, throw error if quorum not met
	return getClusterIDInQuorum(uuids, readQuorum)
}

func (s *xlSets) assignClusterID() (err error) {
	// Attempt to read with quorum, if read fails then we can write
	// If we find a clusterID in one of the sets, then use it
	var uuid string
	for _, set := range s.sets {
		lock := set.nsMutex.NewNSLock(clusterIDBucket, clusterIDFilePath)
		err = lock.GetRLock(globalObjectTimeout)
		defer lock.RUnlock()
		if err != nil {
			fmt.Println("UUID write GetRLock timed out")
			return err
		}
		// attempt to read with quorum
		uuid, err = readClusterIDWithQuorum(set.getDisks())
		if err == nil {
			clusterID = uuid
			fmt.Println("Found stored UUID, using it:", clusterID)
			return err
		}
		
	}
	// if read w/ quorum fails, we should try to write the data next
	fmt.Println("### UUID read failed, attempt to write new UUID now. Error: ", err)
	// generate UUID from IPs/devices given in args and write to all disks
	hostnamesString := ""
	for _, endpoint := range globalEndpoints {
		hostnamesString = hostnamesString + endpoint.String()
	}
	clusterID = generateUUID([]byte(hostnamesString))
	for _, set := range s.sets {
		set.nsMutex.ForceUnlock(clusterIDBucket, clusterIDFilePath)
		lock := set.nsMutex.NewNSLock(clusterIDBucket, clusterIDFilePath)
		err = lock.GetLock(globalObjectTimeout)
		defer lock.Unlock()
		if err != nil {
			fmt.Println("UUID write GetLock timed out")
			return err
		}
		// if error is not nil, then we should bail out
		err = writeClusterIDWithQuorum(set.getDisks())
		if err != nil {
			return err
		}
	}
	fmt.Println("### Successfully wrote ClusterID: ", clusterID)

	return err
}

const defaultMonitorConnectEndpointInterval = time.Second * 10 // Set to 10 secs.

// Initialize new set of erasure coded sets.
func newXLSets(endpoints EndpointList, format *formatXLV3, setCount int, drivesPerSet int) (ObjectLayer, error) {
	// Initialize the XL sets instance.
	s := &xlSets{
		sets:               make([]*xlObjects, setCount),
		xlDisks:            make([][]StorageAPI, setCount),
		endpoints:          endpoints,
		setCount:           setCount,
		drivesPerSet:       drivesPerSet,
		format:             format,
		disksConnectDoneCh: make(chan struct{}),
		distributionAlgo:   format.XL.DistributionAlgo,
		listPool:           newTreeWalkPool(globalLookupTimeout),
	}

	mutex := newNSLock(globalIsDistXL)

	// Initialize byte pool once for all sets, bpool size is set to
	// setCount * drivesPerSet with each memory upto blockSizeV1.
	bp := bpool.NewBytePoolCap(setCount*drivesPerSet, int(blockSizeV1), int(blockSizeV1*2))

	for i := 0; i < len(format.XL.Sets); i++ {
		s.xlDisks[i] = make([]StorageAPI, drivesPerSet)

		// Initialize xl objects for a given set.
		s.sets[i] = &xlObjects{
			getDisks: s.GetDisks(i),
			nsMutex:  mutex,
			bp:       bp,
		}
		go s.sets[i].cleanupStaleMultipartUploads(context.Background(), GlobalMultipartCleanupInterval, GlobalMultipartExpiry, GlobalServiceDoneCh)
	}

	// Connect disks right away, but wait until we have `format.json` quorum.
	s.connectDisksWithQuorum()

	// Start the disk monitoring and connect routine.
	go s.monitorAndConnectEndpoints(defaultMonitorConnectEndpointInterval)

	err := s.assignClusterID()
	if err != nil {
		fmt.Println("Unable to write ClusterID:", err)
		// Error out if we can't assign ClusterID, unable to write ClusterID
		return nil, err
	}

        if (customECpoolObjSize == 0) {
          scData, _ := getRedundancyCount(standardStorageClass, s.drivesPerSet)
          customECpoolObjSize = ceilFrac(blockSizeV1, int64(scData))
        }

        globalDontUseECMemPool = false
        if os.Getenv("MINIO_DONT_USE_EC_MEM_POOL") != "" {
          fmt.Println("### Setting up *not to* use EC mem-pool.. ###")
          globalDontUseECMemPool = true
        }

        if (!globalDontUseECMemPool) {
          fmt.Println("### Setting up to *use* EC mem-pool.. ###")
          initECPool()
        }

        //Sync vols created by other minio instances in case of shared storage mode
        go s.syncSharedVols()
        
        //if (report_minio_stats) {
        if (track_minio_stats) {
          fmt.Println("### Minio stat is enabled.. ###")
          go s.UpdateCountersToDisk()
        }

        // Only record metrics if MINIO_REPORT_METRICS env variable is set
        if (report_minio_metrics) {
            go metricsReporting()
        }

        globalSC_read = false
        if os.Getenv("MINIO_ENABLE_SC_READ") != "" {
          fmt.Println("### Setting up for SC read.. ###")
          globalSC_read = true
        }

        globalNolock_read = false
        if os.Getenv("MINIO_ENABLE_NO_LOCK_READ") != "" {
          fmt.Println("### Setting up for no Lock during read.. ###")
          globalNolock_read = true
        }

        globalNolock_write = false
        if os.Getenv("MINIO_ENABLE_NO_LOCK_WRITE") != "" {
          fmt.Println("### Setting up for no Lock during write.. ###")
          globalNolock_write = true
        }

        globalDo_Write_Opt = false
        if os.Getenv("MINIO_ENABLE_OPT_WRITE") != "" {
          fmt.Println("### Setting up for optimized write.. ###")
          globalDo_Write_Opt = true
        }

        globalNotransaction_write = false
        if os.Getenv("MINIO_ENABLE_NON_TRANSACTIONAL_WRITE") != "" {
          fmt.Println("### Setting up for non transactional write.. ###")
          globalNotransaction_write = true
        }

        globalVerifyChecksum = true
        if os.Getenv("MINIO_ENABLE_NO_READ_VERIFY") != "" {
          fmt.Println("### Setting up for no checksum verify during read.. ###")
          globalVerifyChecksum = false
        }

        globalZeroCopyReader = false
        if os.Getenv("MINIO_ENABLE_ZERO_COPY_READER") != "" {
          fmt.Println("### Setting up zero copy reader during read.. ###")
          globalZeroCopyReader = true
        }

        globalOptimizedMetaReader = true
        if os.Getenv("MINIO_DISABLE_OPTIMIZED_META_READ") != "" {
          fmt.Println("### Setting up non-optimized meta read during read.. ###")
          globalOptimizedMetaReader = false
        }

        globalMetaOptNoStat = false
        if os.Getenv("MINIO_DISABLE_META_STAT") != "" {
          fmt.Println("### Setting up optimized meta disabling stat.. ###")
          globalMetaOptNoStat = true
        }

        globalNoEC = false
        if os.Getenv("MINIO_DISABLE_EC") != "" {
          fmt.Println("### Setting up Minio without EC.. ###")
          globalNoEC = true
        }
        globalNoReadVerifyList = false
        if os.Getenv("MINIO_LIST_NO_READ_VERIFY") != "" {
          fmt.Println("### Setting up Minio not to verify list with read.. ###")
          globalNoReadVerifyList = true
        }

        if v := os.Getenv("MINIO_KV_MAX_SIZE"); v != "" {
          var err error
          globalMaxKVObject, err = strconv.ParseInt(v, 10, 64)
          if err != nil {
            fmt.Printf("### Wrong value of MINIO_KV_MAX_SIZE = %s\n", v)
            globalMaxKVObject = 1048576
          }
        } else {
          globalMaxKVObject = 1048576
        }
        fmt.Println("### Max KV object size supported = ###", globalMaxKVObject)

        globalNoRDD = false
        if os.Getenv("MINIO_DISABLE_RDD") != "" {
          fmt.Println("### Setting up Minio without RDD support.. ###")
          globalNoRDD = true
        }

        globalRddSeparator = "-rdd-"
        if v := os.Getenv("MINIO_RDD_KEY_SEPARATOR"); v != "" {
          globalRddSeparator = v
        }
        fmt.Printf("### RDD Key separator  = %s\n", globalRddSeparator)

        globalDummy_read = -1

        globalDontUseRepMemPool = false
        if os.Getenv("MINIO_DONT_USE_REP_MEM_POOL") != "" {
          fmt.Println("### Setting up *not to* use Rep mem-pool.. ###")
          globalDontUseRepMemPool = true
        }

        if (!globalDontUseRepMemPool) {
          var divFactor int64
          fmt.Println("### Setting up to *use* Rep mem-pool.. ###")
          if v := os.Getenv("MINIO_REP_MEM_POOL_DIV_FACTOR"); v != "" {
            var err error
            divFactor, err = strconv.ParseInt(v, 10, 64)
            if err != nil {
              fmt.Printf("### Wrong value of MINIO_REP_MEM_POOL_DIV_FACTOR = %s\n", v)
              divFactor = 4
            }
          } else {
            divFactor = 4
          }
          fmt.Println("### Div Factor passed = ###", divFactor)

          initRepPool(int (divFactor))
        }

       
	return s, nil
}

// StorageInfo - combines output of StorageInfo across all erasure coded object sets.
func (s *xlSets) StorageInfo(ctx context.Context) StorageInfo {
	var storageInfo StorageInfo
	storageInfo.Backend.Type = BackendErasure
	for _, set := range s.sets {
		lstorageInfo := set.StorageInfo(ctx)
		storageInfo.Total = storageInfo.Total + lstorageInfo.Total
		storageInfo.Used = storageInfo.Used + lstorageInfo.Used
		storageInfo.Backend.OnlineDisks = storageInfo.Backend.OnlineDisks + lstorageInfo.Backend.OnlineDisks
		storageInfo.Backend.OfflineDisks = storageInfo.Backend.OfflineDisks + lstorageInfo.Backend.OfflineDisks
	}

	scData, scParity := getRedundancyCount(standardStorageClass, s.drivesPerSet)
	storageInfo.Backend.StandardSCData = scData
	storageInfo.Backend.StandardSCParity = scParity

	rrSCData, rrSCparity := getRedundancyCount(reducedRedundancyStorageClass, s.drivesPerSet)
	storageInfo.Backend.RRSCData = rrSCData
	storageInfo.Backend.RRSCParity = rrSCparity

	storageInfo.Backend.Sets = make([][]madmin.DriveInfo, s.setCount)
	for i := range storageInfo.Backend.Sets {
		storageInfo.Backend.Sets[i] = make([]madmin.DriveInfo, s.drivesPerSet)
	}

	storageDisks, err := initStorageDisks(s.endpoints)
	if err != nil {
		return storageInfo
	}
	defer closeStorageDisks(storageDisks)

	formats, sErrs := loadFormatXLAll(storageDisks)

	drivesInfo := formatsToDrivesInfo(s.endpoints, formats, sErrs)
	refFormat, err := getFormatXLInQuorum(formats)
	if err != nil {
		// Ignore errors here, since this call cannot do anything at
		// this point. too many disks are down already.
		return storageInfo
	}

	// fill all the available/online endpoints
	for _, drive := range drivesInfo {
		if drive.UUID == "" {
			continue
		}
		for i := range refFormat.XL.Sets {
			for j, driveUUID := range refFormat.XL.Sets[i] {
				if driveUUID == drive.UUID {
					storageInfo.Backend.Sets[i][j] = drive
				}
			}
		}
	}

	// fill all the offline, missing endpoints as well.
	for _, drive := range drivesInfo {
		if drive.UUID == "" {
			for i := range storageInfo.Backend.Sets {
				for j := range storageInfo.Backend.Sets[i] {
					if storageInfo.Backend.Sets[i][j].Endpoint == drive.Endpoint {
						continue
					}
					if storageInfo.Backend.Sets[i][j].Endpoint == "" {
						storageInfo.Backend.Sets[i][j] = drive
						break
					}
				}
			}
		}
	}

	return storageInfo
}

// Shutdown shutsdown all erasure coded sets in parallel
// returns error upon first error.
func (s *xlSets) Shutdown(ctx context.Context) error {
	g := errgroup.WithNErrs(len(s.sets))

	for index := range s.sets {
		index := index
		g.Go(func() error {
			return s.sets[index].Shutdown(ctx)
		}, index)
	}

	for _, err := range g.Wait() {
		if err != nil {
			return err
		}
	}

	return nil
}

// MakeBucketLocation - creates a new bucket across all sets simultaneously
// even if one of the sets fail to create buckets, we proceed to undo a
// successful operation.
func (s *xlSets) MakeBucketWithLocation(ctx context.Context, bucket, location string) error {
	g := errgroup.WithNErrs(len(s.sets))

	// Create buckets in parallel across all sets.
	for index := range s.sets {
		index := index
		g.Go(func() error {
			return s.sets[index].MakeBucketWithLocation(ctx, bucket, location)
		}, index)
	}

	errs := g.Wait()
	// Upon even a single write quorum error we undo all previously created buckets.
	for _, err := range errs {
		if err != nil {
			if _, ok := err.(InsufficientWriteQuorum); ok {
				undoMakeBucketSets(bucket, s.sets, errs)
			}
			return err
		}
	}

	// Success.
	return nil
}

// This function is used to undo a successful MakeBucket operation.
func undoMakeBucketSets(bucket string, sets []*xlObjects, errs []error) {
	g := errgroup.WithNErrs(len(sets))

	// Undo previous make bucket entry on all underlying sets.
	for index := range sets {
		index := index
		if errs[index] == nil {
			g.Go(func() error {
				return sets[index].DeleteBucket(context.Background(), bucket)
			}, index)
		}
	}

	// Wait for all delete bucket to finish.
	g.Wait()
}

// hashes the key returning an integer based on the input algorithm.
// This function currently supports
// - CRCMOD
// - all new algos.
func crcHashMod(key string, cardinality int) int {
	if cardinality <= 0 {
		return -1
	}
	keyCrc := crc32.Checksum([]byte(key), crc32.IEEETable)
	return int(keyCrc % uint32(cardinality))
}

func hashKey(algo string, key string, cardinality int) int {
	switch algo {
	case formatXLVersionV2DistributionAlgo:
		return crcHashMod(key, cardinality)
	default:
		// Unknown algorithm returns -1, also if cardinality is lesser than 0.
		return -1
	}
}

// Returns always a same erasure coded set for a given input.
func (s *xlSets) getHashedSet(input string) (set *xlObjects) {
	return s.sets[hashKey(s.distributionAlgo, input, len(s.sets))]
}

// GetBucketInfo - returns bucket info from one of the erasure coded set.
func (s *xlSets) GetBucketInfo(ctx context.Context, bucket string) (bucketInfo BucketInfo, err error) {
	return s.getHashedSet(bucket).GetBucketInfo(ctx, bucket)
}

// ListObjectsV2 lists all objects in bucket filtered by prefix
func (s *xlSets) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (result ListObjectsV2Info, err error) {
	marker := continuationToken
	if marker == "" {
		marker = startAfter
	}
	loi, err := s.ListObjects(ctx, bucket, prefix, marker, delimiter, maxKeys)
	if err != nil {
		return result, err
	}

	listObjectsV2Info := ListObjectsV2Info{
		IsTruncated:           loi.IsTruncated,
		ContinuationToken:     continuationToken,
		NextContinuationToken: loi.NextMarker,
		Objects:               loi.Objects,
		Prefixes:              loi.Prefixes,
	}
	return listObjectsV2Info, err
}

// SetBucketPolicy persist the new policy on the bucket.
func (s *xlSets) SetBucketPolicy(ctx context.Context, bucket string, policy *policy.Policy) error {
	return savePolicyConfig(ctx, s, bucket, policy)
}

// GetBucketPolicy will return a policy on a bucket
func (s *xlSets) GetBucketPolicy(ctx context.Context, bucket string) (*policy.Policy, error) {
	return getPolicyConfig(s, bucket)
}

// DeleteBucketPolicy deletes all policies on bucket
func (s *xlSets) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	return removePolicyConfig(ctx, s, bucket)
}

// IsNotificationSupported returns whether bucket notification is applicable for this layer.
func (s *xlSets) IsNotificationSupported() bool {
	return s.getHashedSet("").IsNotificationSupported()
}

// IsListenBucketSupported returns whether listen bucket notification is applicable for this layer.
func (s *xlSets) IsListenBucketSupported() bool {
	return true
}

// IsEncryptionSupported returns whether server side encryption is implemented for this layer.
func (s *xlSets) IsEncryptionSupported() bool {
	return s.getHashedSet("").IsEncryptionSupported()
}

// IsCompressionSupported returns whether compression is applicable for this layer.
func (s *xlSets) IsCompressionSupported() bool {
	return s.getHashedSet("").IsCompressionSupported()
}

// DeleteBucket - deletes a bucket on all sets simultaneously,
// even if one of the sets fail to delete buckets, we proceed to
// undo a successful operation.
func (s *xlSets) DeleteBucket(ctx context.Context, bucket string) error {
	g := errgroup.WithNErrs(len(s.sets))

	// Delete buckets in parallel across all sets.
	for index := range s.sets {
		index := index
		g.Go(func() error {
			return s.sets[index].DeleteBucket(ctx, bucket)
		}, index)
	}

	errs := g.Wait()
	// For any write quorum failure, we undo all the delete buckets operation
	// by creating all the buckets again.
	for _, err := range errs {
		if err != nil {
			if _, ok := err.(InsufficientWriteQuorum); ok {
				undoDeleteBucketSets(bucket, s.sets, errs)
			}
			return err
		}
	}

	// Delete all bucket metadata.
	deleteBucketMetadata(ctx, bucket, s)

	// Success.
	return nil
}

// This function is used to undo a successful DeleteBucket operation.
func undoDeleteBucketSets(bucket string, sets []*xlObjects, errs []error) {
	g := errgroup.WithNErrs(len(sets))

	// Undo previous delete bucket on all underlying sets.
	for index := range sets {
		index := index
		if errs[index] == nil {
			g.Go(func() error {
				return sets[index].MakeBucketWithLocation(context.Background(), bucket, "")
			}, index)
		}
	}

	g.Wait()
}

// List all buckets from one of the set, we are not doing merge
// sort here just for simplification. As per design it is assumed
// that all buckets are present on all sets.
func (s *xlSets) ListBuckets(ctx context.Context) (buckets []BucketInfo, err error) {
	// Always lists from the same set signified by the empty string.
	return s.getHashedSet("").ListBuckets(ctx)
}

// --- Object Operations ---

// GetObjectNInfo - returns object info and locked object ReadCloser
func (s *xlSets) GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, lockType LockType, opts ObjectOptions) (gr *GetObjectReader, err error) {
	return s.getHashedSet(object).GetObjectNInfo(ctx, bucket, object, rs, h, lockType, opts)
}

// GetObject - reads an object from the hashedSet based on the object name.
func (s *xlSets) GetObject(ctx context.Context, bucket, object string, startOffset int64, length int64, writer io.Writer, etag string, opts ObjectOptions) error {
	return s.getHashedSet(object).GetObject(ctx, bucket, object, startOffset, length, writer, etag, opts)
}

// PutObject - writes an object to hashedSet based on the object name.
func (s *xlSets) PutObject(ctx context.Context, bucket string, object string, data *PutObjReader, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	return s.getHashedSet(object).PutObject(ctx, bucket, object, data, opts)
}

// GetObjectInfo - reads object metadata from the hashedSet based on the object name.
func (s *xlSets) GetObjectInfo(ctx context.Context, bucket, object string, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	return s.getHashedSet(object).GetObjectInfo(ctx, bucket, object, opts)
}

// DeleteObject - deletes an object from the hashedSet based on the object name.
func (s *xlSets) DeleteObject(ctx context.Context, bucket string, object string) (err error) {
	return s.getHashedSet(object).DeleteObject(ctx, bucket, object)
}

// CopyObject - copies objects from one hashedSet to another hashedSet, on server side.
func (s *xlSets) CopyObject(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (objInfo ObjectInfo, err error) {
	srcSet := s.getHashedSet(srcObject)
	destSet := s.getHashedSet(destObject)

	// Check if this request is only metadata update.
	cpSrcDstSame := isStringEqual(pathJoin(srcBucket, srcObject), pathJoin(destBucket, destObject))
	if cpSrcDstSame && srcInfo.metadataOnly {
		return srcSet.CopyObject(ctx, srcBucket, srcObject, destBucket, destObject, srcInfo, srcOpts, dstOpts)
	}

	if !cpSrcDstSame {
		objectDWLock := destSet.nsMutex.NewNSLock(destBucket, destObject)
		if err := objectDWLock.GetLock(globalObjectTimeout); err != nil {
			return objInfo, err
		}
		defer objectDWLock.Unlock()
	}
	putOpts := ObjectOptions{ServerSideEncryption: dstOpts.ServerSideEncryption, UserDefined: srcInfo.UserDefined}
	return destSet.putObject(ctx, destBucket, destObject, srcInfo.PutObjReader, putOpts)
}

// Returns function "listDir" of the type listDirFunc.
// isLeaf - is used by listDir function to check if an entry is a leaf or non-leaf entry.
// disks - used for doing disk.ListDir(). Sets passes set of disks.
func listDirSetsFactory(ctx context.Context, isLeaf isLeafFunc, isLeafDir isLeafDirFunc, sets ...*xlObjects) listDirFunc {
	listDirInternal := func(bucket, prefixDir, prefixEntry string, disks []StorageAPI) (mergedEntries []string) {
		var diskEntries = make([][]string, len(disks))
		var wg sync.WaitGroup
		for index, disk := range disks {
			if disk == nil {
				continue
			}
                        if (!globalMinio_on_kv || globalNoEC) {
			  wg.Add(1)
			  go func(index int, disk StorageAPI) {
				defer wg.Done()
				diskEntries[index], _ = disk.ListDir(bucket, prefixDir, -1)
			  }(index, disk)
                        } else {
                          var err error
                          //fmt.Println("#### calling List dir on disk = ", disk)
                          diskEntries[0], err = disk.ListDir(bucket, prefixDir, -1)
                          if (err == nil) {
                            break;
                          }
                        }
		}
                if (!globalMinio_on_kv || globalNoEC) {
		  wg.Wait()
                }

                track_dup := make(map[string]bool)
		// Find elements in entries which are not in mergedEntries
		for _, entries := range diskEntries {
			//var newEntries []string

			for _, entry := range entries {
                                /*if (!globalMinio_on_kv || globalNoEC) {
				  idx := sort.SearchStrings(newEntries, entry)
                                  //fmt.Println("listDirInternal = ", entry, idx, newEntries)
				  // if entry is already present in mergedEntries don't add.
				  //if idx < len(mergedEntries) && mergedEntries[idx] == entry {
                                        //fmt.Println("listDirInternal,duplicate = ", entry)
					continue
				  }
                                }
				newEntries = append(newEntries, entry)
                                */
                                if track_dup[entry] == true {
                                  continue
                                }
                                mergedEntries = append(mergedEntries, entry)
                                track_dup[entry] = true

			}

			/*if len(newEntries) > 0 {
				// Merge the entries and sort it.
				mergedEntries = append(mergedEntries, newEntries...)
                                if (!globalMinio_on_kv) {
				  sort.Strings(mergedEntries)
                                }
			}*/
		}
                //fmt.Println("listDirInternal = ", mergedEntries)

		return mergedEntries
	}
        //To remove duplicates when on KV
        //encountered := map[string]bool{}
        //encountered := make(map[string]bool)
	// listDir - lists all the entries at a given prefix and given entry in the prefix.
	listDir := func(bucket, prefixDir, prefixEntry string) (mergedEntries []string, delayIsLeaf bool) {
                encountered := make(map[string]bool)
		for _, set := range sets {
			var newEntries []string
			// Find elements in entries which are not in mergedEntries
			for _, entry := range listDirInternal(bucket, prefixDir, prefixEntry, set.getLoadBalancedDisks()) {
                                /*if (!globalNkvShared) {
				  idx := sort.SearchStrings(mergedEntries, entry)
				  // if entry is already present in mergedEntries don't add.
				  if idx < len(mergedEntries) && mergedEntries[idx] == entry {
					continue
				  }
                                } else {
                                  if encountered[entry] == true {
                                    continue
                                  }
                                }
				newEntries = append(newEntries, entry)
                                if (globalMinio_on_kv) {
                                  encountered[entry] = true
                                }*/

                                if encountered[entry] == true {
                                  continue
                                }
                                newEntries = append(newEntries, entry)
                                encountered[entry] = true
                                
			}

			if len(newEntries) > 0 {
				// Merge the entries and sort it.
				mergedEntries = append(mergedEntries, newEntries...)
                                if (!globalMinio_on_kv) {
				  sort.Strings(mergedEntries)
                                }
			}
                        //Assumption is for NKV shared both EC set going to same subsystem but from different path
                        /*if (globalNkvShared) {
                          break;
                        }*/
		}
		return filterListEntries(bucket, prefixDir, mergedEntries, prefixEntry, isLeaf)
	}
	return listDir
}

// ListObjects - implements listing of objects across sets, each set is independently
// listed and subsequently merge lexically sorted inside listDirSetsFactory(). Resulting
// value through the walk channel receives the data properly lexically sorted.
func (s *xlSets) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, error) {
	var result ListObjectsInfo
	// validate all the inputs for listObjects
	if err := checkListObjsArgs(ctx, bucket, prefix, marker, delimiter, s); err != nil {
                fmt.Println("### In ListObjects returning ::", bucket, prefix, err)
		return result, err
	}

	var objInfos []ObjectInfo
        var listMu sync.Mutex
	var eof bool
	var nextMarker string

	recursive := true
	if delimiter == slashSeparator {
		recursive = false
	}
        //fmt.Printf("### ListObjects:: bucket = %s, prefix = %s, delimiter = %s, recursive = %d\n", bucket, prefix, delimiter, recursive)
	walkResultCh, endWalkCh := s.listPool.Release(listParams{bucket, recursive, marker, prefix})
	if walkResultCh == nil {
		endWalkCh = make(chan struct{})
		isLeaf := func(bucket, entry string) bool {
			entry = strings.TrimSuffix(entry, slashSeparator)

			// Verify if we are at the leaf, a leaf is where we
			// see `xl.json` inside a directory.
			//return s.getHashedSet(hashPrefix).isObject(bucket, entry)
                        var ok bool

                        for _, set := range s.sets {
                                ok = set.isObject(bucket, entry)
                                if ok {
                                        return true
                                }
                        }
                        return false

		}

		isLeafDir := func(bucket, entry string) bool {
			// Verify prefixes in all sets.
			var ok bool
			for _, set := range s.sets {
				ok = set.isObjectDir(bucket, entry)
				if !ok {
					return false
				}
			}
			return true
		}

		listDir := listDirSetsFactory(ctx, isLeaf, isLeafDir, s.sets...)
		walkResultCh = startTreeWalk(ctx, bucket, prefix, marker, recursive, listDir, isLeaf, isLeafDir, endWalkCh)
	}
     
        var wg = &sync.WaitGroup{}
	for i := 0; i < maxKeys; {
		walkResult, ok := <-walkResultCh
		if !ok {
			// Closed channel.
			eof = true
			break
		}

		// For any walk error return right away.
		if walkResult.err != nil {
			return result, toObjectErr(walkResult.err, bucket, prefix)
		}

		var objInfo ObjectInfo
		var err error
		if hasSuffix(walkResult.entry, slashSeparator) {
			// Verify prefixes in all sets.
			// for _, set := range s.sets {
			// 	objInfo, err = set.getObjectInfoDir(ctx, bucket, walkResult.entry)
			// 	if err == nil {
			// 		break
			// 	}
			// }
                        objInfo.Name = walkResult.entry
                        objInfo.IsDir = true

                        if (!globalNoReadVerifyList) {
                          listMu.Lock()
                          objInfos = append(objInfos, objInfo)
                          listMu.Unlock()
                        } else {
                          objInfos = append(objInfos, objInfo)
                        }
		} else {
                        if (!globalNoReadVerifyList) {
			  //objInfo, err = s.getHashedSet(walkResult.entry).getObjectInfo(ctx, bucket, walkResult.entry)
                          err = nil
                          var keyName = walkResult.entry
                          wg.Add(1)
                          //go func(keyName string, bucket string, ctx context.Context, listMu sync.Mutex, objInfos []ObjectInfo, s *xlSets) {
                          go func(keyName string) {
                            defer wg.Done()
                            //fmt.Println("Calling Getinfo::", bucket, keyName)
			    objInfo, err := s.getHashedSet(keyName).getObjectInfo(ctx, bucket, keyName)
                            if err == nil {
                              listMu.Lock()
                              objInfos = append(objInfos, objInfo)
                              nextMarker = objInfo.Name
                              listMu.Unlock()
                            } else {
                              fmt.Println(" ## Error :: Getinfo::", bucket, keyName)
                            }
                          }(keyName)
                          
                        } else {
                          err = nil
                          objInfo.Name = walkResult.entry
                          objInfos = append(objInfos, objInfo)
                          nextMarker = objInfo.Name
                        }
		}
		if err != nil {
			// Ignore errFileNotFound as the object might have got
			// deleted in the interim period of listing and getObjectInfo(),
			// ignore quorum error as it might be an entry from an outdated disk.
			if IsErrIgnored(err, []error{
				errFileNotFound,
				errXLReadQuorum,
			}...) {
				continue
			}
			return result, toObjectErr(err, bucket, prefix)
		}

                //nextMarker = walkResult.entry
		//objInfos = append(objInfos, objInfo)
		i++
		if walkResult.end {
			eof = true
			break
		}
                //fmt.Println("End of loop", nextMarker, i, maxKeys)
	}
        if (!globalNoReadVerifyList) {
          wg.Wait()
        }
        //fmt.Println("Wait done::", nextMarker, len(objInfos))

	params := listParams{bucket, recursive, nextMarker, prefix}
	if !eof {
		s.listPool.Set(params, walkResultCh, endWalkCh)
	}

	result = ListObjectsInfo{IsTruncated: !eof}
	for _, objInfo := range objInfos {
		result.NextMarker = objInfo.Name
		if objInfo.IsDir && delimiter == slashSeparator {
			result.Prefixes = append(result.Prefixes, objInfo.Name)
			continue
		}
		result.Objects = append(result.Objects, objInfo)
	}
        //fmt.Printf("@@@@@@@ ListObjects done:: bucket = %s, prefix = %s, delimiter = %s\n", bucket, prefix, delimiter, result)
	return result, nil
}

func (s *xlSets) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result ListMultipartsInfo, err error) {
	// In list multipart uploads we are going to treat input prefix as the object,
	// this means that we are not supporting directory navigation.
	return s.getHashedSet(prefix).ListMultipartUploads(ctx, bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
}

// Initiate a new multipart upload on a hashedSet based on object name.
func (s *xlSets) NewMultipartUpload(ctx context.Context, bucket, object string, opts ObjectOptions) (uploadID string, err error) {
	return s.getHashedSet(object).NewMultipartUpload(ctx, bucket, object, opts)
}

// Copies a part of an object from source hashedSet to destination hashedSet.
func (s *xlSets) CopyObjectPart(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, uploadID string, partID int,
	startOffset int64, length int64, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (partInfo PartInfo, err error) {
	destSet := s.getHashedSet(destObject)

	return destSet.PutObjectPart(ctx, destBucket, destObject, uploadID, partID, NewPutObjReader(srcInfo.Reader, nil, nil), dstOpts)
}

// PutObjectPart - writes part of an object to hashedSet based on the object name.
func (s *xlSets) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data *PutObjReader, opts ObjectOptions) (info PartInfo, err error) {
	return s.getHashedSet(object).PutObjectPart(ctx, bucket, object, uploadID, partID, data, opts)
}

// ListObjectParts - lists all uploaded parts to an object in hashedSet.
func (s *xlSets) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker int, maxParts int, opts ObjectOptions) (result ListPartsInfo, err error) {
	return s.getHashedSet(object).ListObjectParts(ctx, bucket, object, uploadID, partNumberMarker, maxParts, opts)
}

// Aborts an in-progress multipart operation on hashedSet based on the object name.
func (s *xlSets) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string) error {
	return s.getHashedSet(object).AbortMultipartUpload(ctx, bucket, object, uploadID)
}

// CompleteMultipartUpload - completes a pending multipart transaction, on hashedSet based on object name.
func (s *xlSets) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, uploadedParts []CompletePart, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	return s.getHashedSet(object).CompleteMultipartUpload(ctx, bucket, object, uploadID, uploadedParts, opts)
}

/*

All disks online
-----------------
- All Unformatted - format all and return success.
- Some Unformatted - format all and return success.
- Any JBOD inconsistent - return failure
- Some are corrupt (missing format.json) - return failure
- Any unrecognized disks - return failure

Some disks are offline and we have quorum.
-----------------
- Some unformatted - format all and return success,
  treat disks offline as corrupted.
- Any JBOD inconsistent - return failure
- Some are corrupt (missing format.json)
- Any unrecognized disks - return failure

No read quorum
-----------------
failure for all cases.

// Pseudo code for managing `format.json`.

// Generic checks.
if (no quorum) return error
if (any disk is corrupt) return error // Always error
if (jbod inconsistent) return error // Always error.
if (disks not recognized) // Always error.

// Specific checks.
if (all disks online)
  if (all disks return format.json)
     if (jbod consistent)
        if (all disks recognized)
          return
  else
     if (all disks return format.json not found)
        return error
     else (some disks return format.json not found)
        (heal format)
        return
     fi
   fi
else
   if (some disks return format.json not found)
        // Offline disks are marked as dead.
        (heal format) // Offline disks should be marked as dead.
        return success
   fi
fi
*/

func formatsToDrivesInfo(endpoints EndpointList, formats []*formatXLV3, sErrs []error) (beforeDrives []madmin.DriveInfo) {
	// Existing formats are available (i.e. ok), so save it in
	// result, also populate disks to be healed.
	for i, format := range formats {
		drive := endpoints.GetString(i)
		switch {
		case format != nil:
			beforeDrives = append(beforeDrives, madmin.DriveInfo{
				UUID:     format.XL.This,
				Endpoint: drive,
				State:    madmin.DriveStateOk,
			})
		case sErrs[i] == errUnformattedDisk:
			beforeDrives = append(beforeDrives, madmin.DriveInfo{
				UUID:     "",
				Endpoint: drive,
				State:    madmin.DriveStateMissing,
			})
		case sErrs[i] == errCorruptedFormat:
			beforeDrives = append(beforeDrives, madmin.DriveInfo{
				UUID:     "",
				Endpoint: drive,
				State:    madmin.DriveStateCorrupt,
			})
		default:
			beforeDrives = append(beforeDrives, madmin.DriveInfo{
				UUID:     "",
				Endpoint: drive,
				State:    madmin.DriveStateOffline,
			})
		}
	}

	return beforeDrives
}

// Reloads the format from the disk, usually called by a remote peer notifier while
// healing in a distributed setup.
func (s *xlSets) ReloadFormat(ctx context.Context, dryRun bool) (err error) {
	// Acquire lock on format.json
	formatLock := s.getHashedSet(formatConfigFile).nsMutex.NewNSLock(minioMetaBucket, formatConfigFile)
	if err = formatLock.GetRLock(globalHealingTimeout); err != nil {
		return err
	}
	defer formatLock.RUnlock()

	storageDisks, err := initStorageDisks(s.endpoints)
	if err != nil {
		return err
	}
	defer func(storageDisks []StorageAPI) {
		if err != nil {
			closeStorageDisks(storageDisks)
		}
	}(storageDisks)

	formats, sErrs := loadFormatXLAll(storageDisks)
	if err = checkFormatXLValues(formats); err != nil {
		return err
	}

	for index, sErr := range sErrs {
		if sErr != nil {
			// Look for acceptable heal errors, for any other
			// errors we should simply quit and return.
			if _, ok := formatHealErrors[sErr]; !ok {
				return fmt.Errorf("Disk %s: %s", s.endpoints[index], sErr)
			}
		}
	}

	refFormat, err := getFormatXLInQuorum(formats)
	if err != nil {
		return err
	}

	// kill the monitoring loop such that we stop writing
	// to indicate that we will re-initialize everything
	// with new format.
	s.disksConnectDoneCh <- struct{}{}

	// Replace the new format.
	s.format = refFormat

	s.xlDisksMu.Lock()
	{
		// Close all existing disks.
		s.xlDisks.Close()

		// Re initialize disks, after saving the new reference format.
		s.xlDisks = s.reInitDisks(refFormat, storageDisks, formats)
	}
	s.xlDisksMu.Unlock()

	// Restart monitoring loop to monitor reformatted disks again.
	go s.monitorAndConnectEndpoints(defaultMonitorConnectEndpointInterval)

	return nil
}

// If it is a single node XL and all disks are root disks, it is most likely a test setup, else it is a production setup.
// On a test setup we allow creation of format.json on root disks to help with dev/testing.
func isTestSetup(infos []DiskInfo, errs []error) bool {
	rootDiskCount := 0
	for i := range errs {
		if errs[i] != nil {
			// On error it is safer to assume that this is not a test setup.
			return false
		}
		if infos[i].RootDisk {
			rootDiskCount++
		}
	}
	// It is a test setup if all disks are root disks.
	return rootDiskCount == len(infos)
}

func getAllDiskInfos(storageDisks []StorageAPI) ([]DiskInfo, []error) {
	infos := make([]DiskInfo, len(storageDisks))
	errs := make([]error, len(storageDisks))
	var wg sync.WaitGroup
	for i := range storageDisks {
		if storageDisks[i] == nil {
			errs[i] = errDiskNotFound
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			infos[i], errs[i] = storageDisks[i].DiskInfo()
		}(i)
	}
	wg.Wait()
	return infos, errs
}

// Mark root disks as down so as not to heal them.
func markRootDisksAsDown(storageDisks []StorageAPI) {
	infos, errs := getAllDiskInfos(storageDisks)
	if isTestSetup(infos, errs) {
		// Allow healing of disks for test setups to help with testing.
		return
	}
	for i := range storageDisks {
		if errs[i] != nil {
			storageDisks[i] = nil
			continue
		}
		if infos[i].RootDisk {
			// We should not heal on root disk. i.e in a situation where the minio-administrator has unmounted a
			// defective drive we should not heal a path on the root disk.
			storageDisks[i] = nil
		}
	}
}

// HealFormat - heals missing `format.json` on fresh unformatted disks.
// TODO: In future support corrupted disks missing format.json but has erasure
// coded data in it.
func (s *xlSets) HealFormat(ctx context.Context, dryRun bool) (res madmin.HealResultItem, err error) {
	// Acquire lock on format.json
	formatLock := s.getHashedSet(formatConfigFile).nsMutex.NewNSLock(minioMetaBucket, formatConfigFile)
	if err = formatLock.GetLock(globalHealingTimeout); err != nil {
		return madmin.HealResultItem{}, err
	}
	defer formatLock.Unlock()

	storageDisks, err := initStorageDisks(s.endpoints)
	if err != nil {
		return madmin.HealResultItem{}, err
	}

	defer func(storageDisks []StorageAPI) {
		if err != nil {
			closeStorageDisks(storageDisks)
		}
	}(storageDisks)

	markRootDisksAsDown(storageDisks)

	formats, sErrs := loadFormatXLAll(storageDisks)
	if err = checkFormatXLValues(formats); err != nil {
		return madmin.HealResultItem{}, err
	}

	// Prepare heal-result
	res = madmin.HealResultItem{
		Type:      madmin.HealItemMetadata,
		Detail:    "disk-format",
		DiskCount: s.setCount * s.drivesPerSet,
		SetCount:  s.setCount,
	}

	// Fetch all the drive info status.
	beforeDrives := formatsToDrivesInfo(s.endpoints, formats, sErrs)

	res.After.Drives = make([]madmin.HealDriveInfo, len(beforeDrives))
	res.Before.Drives = make([]madmin.HealDriveInfo, len(beforeDrives))
	// Copy "after" drive state too from before.
	for k, v := range beforeDrives {
		res.Before.Drives[k] = madmin.HealDriveInfo(v)
		res.After.Drives[k] = madmin.HealDriveInfo(v)
	}

	for index, sErr := range sErrs {
		if sErr != nil {
			// Look for acceptable heal errors, for any other
			// errors we should simply quit and return.
			if _, ok := formatHealErrors[sErr]; !ok {
				return res, fmt.Errorf("Disk %s: %s", s.endpoints[index], sErr)
			}
		}
	}

	if !hasAnyErrorsUnformatted(sErrs) {
		// No unformatted disks found disks are either offline
		// or online, no healing is required.
		return res, errNoHealRequired
	}

	// All disks are unformatted, return quorum error.
	if shouldInitXLDisks(sErrs) {
		return res, errXLReadQuorum
	}

	refFormat, err := getFormatXLInQuorum(formats)
	if err != nil {
		return res, err
	}

	// Mark all UUIDs which might be offline, use list
	// of formats to mark them appropriately.
	markUUIDsOffline(refFormat, formats)

	// Initialize a new set of set formats which will be written to disk.
	newFormatSets := newHealFormatSets(refFormat, s.setCount, s.drivesPerSet, formats, sErrs)

	// Look for all offline/unformatted disks in our reference format,
	// such that we can fill them up with new UUIDs, this looping also
	// ensures that the replaced disks allocated evenly across all sets.
	// Making sure that the redundancy is not lost.
	for i := range refFormat.XL.Sets {
		for j := range refFormat.XL.Sets[i] {
			if refFormat.XL.Sets[i][j] == offlineDiskUUID {
				for l := range newFormatSets[i] {
					if newFormatSets[i][l] == nil {
						continue
					}
					if newFormatSets[i][l].XL.This == "" {
						newFormatSets[i][l].XL.This = mustGetUUID()
						refFormat.XL.Sets[i][j] = newFormatSets[i][l].XL.This
						for m, v := range res.After.Drives {
							if v.Endpoint == s.endpoints.GetString(i*s.drivesPerSet+l) {
								res.After.Drives[m].UUID = newFormatSets[i][l].XL.This
								res.After.Drives[m].State = madmin.DriveStateOk
							}
						}
						break
					}
				}
			}
		}
	}

	if !dryRun {
		var tmpNewFormats = make([]*formatXLV3, s.setCount*s.drivesPerSet)
		for i := range newFormatSets {
			for j := range newFormatSets[i] {
				if newFormatSets[i][j] == nil {
					continue
				}
				tmpNewFormats[i*s.drivesPerSet+j] = newFormatSets[i][j]
				tmpNewFormats[i*s.drivesPerSet+j].XL.Sets = refFormat.XL.Sets
			}
		}

		// Initialize meta volume, if volume already exists ignores it, all disks which
		// are not found are ignored as well.
		if err = initFormatXLMetaVolume(storageDisks, tmpNewFormats); err != nil {
			return madmin.HealResultItem{}, fmt.Errorf("Unable to initialize '.minio.sys' meta volume, %s", err)
		}

		// Save formats `format.json` across all disks.
		if err = saveFormatXLAll(ctx, storageDisks, tmpNewFormats); err != nil {
			return madmin.HealResultItem{}, err
		}

		// kill the monitoring loop such that we stop writing
		// to indicate that we will re-initialize everything
		// with new format.
		s.disksConnectDoneCh <- struct{}{}

		// Replace with new reference format.
		s.format = refFormat

		s.xlDisksMu.Lock()
		{
			// Disconnect/relinquish all existing disks.
			s.xlDisks.Close()

			// Re initialize disks, after saving the new reference format.
			s.xlDisks = s.reInitDisks(refFormat, storageDisks, tmpNewFormats)
		}
		s.xlDisksMu.Unlock()

		// Restart our monitoring loop to start monitoring newly formatted disks.
		go s.monitorAndConnectEndpoints(defaultMonitorConnectEndpointInterval)
	}

	return res, nil
}

// HealBucket - heals inconsistent buckets and bucket metadata on all sets.
func (s *xlSets) HealBucket(ctx context.Context, bucket string, dryRun, remove bool) (result madmin.HealResultItem, err error) {
	bucketLock := globalNSMutex.NewNSLock(bucket, "")
	if err := bucketLock.GetLock(globalHealingTimeout); err != nil {
		return result, err
	}
	defer bucketLock.Unlock()

	// Initialize heal result info
	result = madmin.HealResultItem{
		Type:      madmin.HealItemBucket,
		Bucket:    bucket,
		DiskCount: s.setCount * s.drivesPerSet,
		SetCount:  s.setCount,
	}

	for _, s := range s.sets {
		var healResult madmin.HealResultItem
		healResult, err = s.HealBucket(ctx, bucket, dryRun, remove)
		if err != nil {
			return result, err
		}
		result.Before.Drives = append(result.Before.Drives, healResult.Before.Drives...)
		result.After.Drives = append(result.After.Drives, healResult.After.Drives...)
	}

	for _, endpoint := range s.endpoints {
		var foundBefore bool
		for _, v := range result.Before.Drives {
			if endpoint.IsLocal {
				if v.Endpoint == endpoint.Path {
					foundBefore = true
				}
			} else {
				if v.Endpoint == endpoint.String() {
					foundBefore = true
				}
			}
		}
		if !foundBefore {
			result.Before.Drives = append(result.Before.Drives, madmin.HealDriveInfo{
				UUID:     "",
				Endpoint: endpoint.String(),
				State:    madmin.DriveStateOffline,
			})
		}
		var foundAfter bool
		for _, v := range result.After.Drives {
			if endpoint.IsLocal {
				if v.Endpoint == endpoint.Path {
					foundAfter = true
				}
			} else {
				if v.Endpoint == endpoint.String() {
					foundAfter = true
				}
			}
		}
		if !foundAfter {
			result.After.Drives = append(result.After.Drives, madmin.HealDriveInfo{
				UUID:     "",
				Endpoint: endpoint.String(),
				State:    madmin.DriveStateOffline,
			})
		}
	}

	// Check if we had quorum to write, if not return an appropriate error.
	_, afterDriveOnline := result.GetOnlineCounts()
	if afterDriveOnline < ((s.setCount*s.drivesPerSet)/2)+1 {
		return result, toObjectErr(errXLWriteQuorum, bucket)
	}

	return result, nil
}

// HealObject - heals inconsistent object on a hashedSet based on object name.
func (s *xlSets) HealObject(ctx context.Context, bucket, object string, dryRun, remove bool, scanMode madmin.HealScanMode) (madmin.HealResultItem, error) {
	return s.getHashedSet(object).HealObject(ctx, bucket, object, dryRun, remove, scanMode)
}

// Lists all buckets which need healing.
func (s *xlSets) ListBucketsHeal(ctx context.Context) ([]BucketInfo, error) {
	listBuckets := []BucketInfo{}
	var healBuckets = map[string]BucketInfo{}
	for _, set := range s.sets {
		buckets, _, err := listAllBuckets(set.getDisks())
		if err != nil {
			return nil, err
		}
		for _, currBucket := range buckets {
			healBuckets[currBucket.Name] = BucketInfo(currBucket)
		}
	}
	for _, bucketInfo := range healBuckets {
		listBuckets = append(listBuckets, bucketInfo)
	}
	return listBuckets, nil
}

// HealObjects - Heal all objects recursively at a specified prefix, any
// dangling objects deleted as well automatically.
func (s *xlSets) HealObjects(ctx context.Context, bucket, prefix string, healObjectFn func(string, string) error) (err error) {
	recursive := true

	endWalkCh := make(chan struct{})
	isLeaf := func(bucket, entry string) bool {
		entry = strings.TrimSuffix(entry, slashSeparator)
		// Verify if we are at the leaf, a leaf is where we
		// see `xl.json` inside a directory.
		return s.getHashedSet(entry).isObject(bucket, entry)
	}

	isLeafDir := func(bucket, entry string) bool {
		var ok bool
		for _, set := range s.sets {
			ok = set.isObjectDir(bucket, entry)
			if ok {
				return true
			}
		}
		return false
	}

	listDir := listDirSetsFactory(ctx, isLeaf, isLeafDir, s.sets...)
	walkResultCh := startTreeWalk(ctx, bucket, prefix, "", recursive, listDir, isLeaf, isLeafDir, endWalkCh)
	for {
		walkResult, ok := <-walkResultCh
		if !ok {
			break
		}
		// For any walk error return right away.
		if walkResult.err != nil {
			return toObjectErr(walkResult.err, bucket, prefix)
		}
		if err := healObjectFn(bucket, walkResult.entry); err != nil {
			return toObjectErr(err, bucket, walkResult.entry)
		}
		if walkResult.end {
			break
		}
	}

	return nil
}

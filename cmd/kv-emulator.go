package cmd

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
)

type KVEmulator struct {
	path string
}

func (k *KVEmulator) Put(keyStr string, value []byte) error {
	if !strings.HasPrefix(keyStr, kvDataDir) {
		keyStr = pathJoin(kvMetaDir, keyStr)
	}
	fullPath := pathJoin(k.path, keyStr)
	os.MkdirAll(path.Dir(fullPath), 0777)
	if err := ioutil.WriteFile(fullPath, value, 0644); err != nil {
		return errFileNotFound
	}
	return nil
}

func (k *KVEmulator) Get(keyStr string, value []byte) ([]byte, error) {
	if !strings.HasPrefix(keyStr, kvDataDir) {
		keyStr = pathJoin(kvMetaDir, keyStr)
	}
	b, err := ioutil.ReadFile(pathJoin(k.path, keyStr))
	if err != nil {
		return nil, errFileNotFound
	}
	n := copy(value, b)
	return value[:n], nil
}

func (k *KVEmulator) Delete(keyStr string) error {
	if !strings.HasPrefix(keyStr, kvDataDir) {
		keyStr = pathJoin(kvMetaDir, keyStr)
	}
	if err := os.Remove(pathJoin(k.path, keyStr)); err != nil {
		return errFileNotFound
	}
	return nil
}

func (k *KVEmulator) List(prefix string, buf []byte) ([]string, error) {
	if !strings.HasPrefix(prefix, kvDataDir) {
		prefix = pathJoin(kvMetaDir, prefix)
	}
	fullPath := pathJoin(k.path, prefix)
	entries, err := readDir(fullPath)
	fmt.Println("List", fullPath, entries, err)
	return entries, err
}

func (k *KVEmulator) DiskInfo() (DiskInfo, error) {
	return DiskInfo{}, nil
}

func (k *KVEmulator) UpdateStats() error {
        return nil
}


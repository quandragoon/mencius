package main

import (
	"io"
	"io/ioutil"
	"os"
	"fmt"
	"sync"
	"strconv"
//	"encoding/gob"
)

type DiskIO struct {
	BasePath string
	mu sync.Mutex
}

type simpleWriteCloser struct {
	w io.Writer
}

func (wc *simpleWriteCloser) Write(p []byte) (int, error) { return wc.w.Write(p) }
func (wc *simpleWriteCloser) Close() error                { return nil }

func (d *DiskIO) write(config int, key string, val string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	dirpath := d.BasePath + "/" + strconv.Itoa(config)
	fullpath := dirpath + "/" + key

	var pathPerm os.FileMode = 0777
	var filePerm os.FileMode = 0666

	if err := os.MkdirAll(dirpath, pathPerm); err != nil {
		return err
	}

	ioutil.WriteFile(fullpath, []byte(val), filePerm)

	return nil
}

func (d *DiskIO) read(config int, key string) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	path := d.BasePath + "/" + strconv.Itoa(config) + "/" + key

	fi, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	if fi.IsDir() {
		return "", os.ErrNotExist
	}

	dat, err := ioutil.ReadFile(path)
	if err != nil {
		return "", err 
	}

	return string(dat), nil
}

func (d *DiskIO) export(config int) (map[string]string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	retMap := make(map[string]string)

	path := d.BasePath + "/" + strconv.Itoa(config)

	files, err := ioutil.ReadDir(path)

	if err != nil {

	}

	for _, file := range files {
		key := file.Name()
		val, e := ioutil.ReadFile(path + "/" + key)
		if e != nil {
			// Some file could not be written...
		}
		retMap[key] = string(val)
	}

	return retMap, nil
}

func main() {
	basePath := "/tmp/Data"
	d := new(DiskIO)
	d.BasePath = basePath

	d.write(1,"testkey123","testval123")
	val, err := d.read(1,"testkey123")
	if err != nil {
		fmt.Printf("Something went wrong")
		return
	}

	fmt.Println("Received val:", val)

	testMap, _ := d.export(1)

	fmt.Println("Map val:", testMap["testkey123"])

}

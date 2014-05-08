package main

import (
	"io"
	"io/ioutil"
	"os"
	"strings"
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

func (d *DiskIO) writeIOUtil(config int, key string, val string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	path := d.BasePath + "/" + strconv.Itoa(config) + "/" + key

	var perm os.FileMode = 0666

	ioutil.WriteFile(path, []byte(key), perm)

	return nil
}

func (d *DiskIO) write(config int, key string, val string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	path := d.BasePath + "/" + strconv.Itoa(config) + "/" + key

	var perm os.FileMode = 0666
	mode := os.O_WRONLY | os.O_CREATE | os.O_TRUNC // overwrite if exists

	f, err := os.OpenFile(path, mode, perm)
	if err != nil {
		return err
	}

	r := strings.NewReader(val)

	var wc io.WriteCloser = &simpleWriteCloser{f}

	if _, err := io.Copy(wc, r); err != nil {
		f.Close() // error deliberately ignored
		return err
	}
	if err := wc.Close(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

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

	d.write(0,"testkey123","testval123")
	val, err := d.read(0,"testkey123")
	if err != nil {
		fmt.Printf("Something went wrong")
		return
	}

	fmt.Println("Received val:", val)

	testMap, _ := d.export(0)

	fmt.Println("Map val:", testMap["testkey123"])

}

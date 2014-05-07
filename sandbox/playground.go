package main

import (
	"io"
	"io/ioutil"
	"os"
	"strings"
	"fmt"
	"sync"
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

func (d *DiskIO) write(key string, val string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	path := d.BasePath + "/" + key

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

	fmt.Printf("writing to %s \n", path)
	return nil
}

func (d *DiskIO) read(key string) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	path := d.BasePath + "/" + key

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

func main() {
	basePath = "/tmp"
	d := new(DiskIO)
	d.BasePath = basePath

	d.write("testkey123","testval123")
	val, err := d.read("testkey123")
	if err != nil {
		fmt.Printf("Something went wrong")
		return
	}

	fmt.Println("Received val:", val)
}

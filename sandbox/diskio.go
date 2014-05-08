package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"strconv"
)

type MetaData struct {
	Config int
}

type DiskIO struct {
	BasePath string
	metaFileName string
	mu sync.Mutex
}

type simpleWriteCloser struct {
	w io.Writer
}

func (wc *simpleWriteCloser) Write(p []byte) (int, error) { return wc.w.Write(p) }
func (wc *simpleWriteCloser) Close() error                { return nil }

/* 
	Updates saves metadata and writes to disk
*/
func (d *DiskIO) writePaxosLogOnDisk(config int, paxosLog interface{}) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	var network bytes.Buffer
	enc := gob.NewEncoder(&network)
	err := enc.Encode(paxosLog)

	if err != nil {

	}

	dirpath := d.BasePath + "/" + strconv.Itoa(config)
	fullpath := dirpath + "/paxos_state"

	var pathPerm os.FileMode = 0777
	var filePerm os.FileMode = 0666

	if err := os.MkdirAll(dirpath, pathPerm); err != nil {
		return err
	}

	ioutil.WriteFile(fullpath, network.Bytes(), filePerm)

	return err
}

func (d *DiskIO) readPaxosLogOnDisk(config int) (MetaData, error) {
	path := d.BasePath + "/" + strconv.Itoa(config) + "/paxos_state"

	fi, err := os.Stat(path)
	if err != nil {
		return null, err
	}
	if fi.IsDir() {
		return nil, os.ErrNotExist
	}

	dat, err := ioutil.ReadFile(path)
	network := bytes.NewBuffer(dat)
	dec := gob.NewDecoder(network)

	// We have to define concrete datatype here
	log := MetaData{}
	err = dec.Decode(&log)

	if err != nil {
		fmt.Println("Error decoding ", err.Error())
		panic(err)

	}

	return log, nil
}

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
	d := new(DiskIO)
	d.BasePath = "/tmp/Data"

	d.write(1,"testkey123","testval123")
	val, err := d.read(1,"testkey123")
	if err != nil {
		fmt.Printf("Something went wrong")
		return
	}

	fmt.Println("Received val:", val)

	testMap, _ := d.export(1)

	fmt.Println("Map val:", testMap["testkey123"])

	px := new(MetaData)
	px.Config = 1
	d.writePaxosLogOnDisk(1, px)

	npx, err := d.readPaxosLogOnDisk(1)
	fmt.Println("State config: ",npx.(MetaData).Config)
}

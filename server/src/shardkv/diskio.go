package shardkv

import (
	"bytes"
	"container/list"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"strconv"
	"strings"
	"shardmaster"
)

var stateFileNames = map[string]bool {
	"paxosState": true,
	"peersDoneState": true,
	"configState": true,
	"getRequestState": true,
	"putRequestState": true,
}

type MetaData struct {
	Config int
}

type DiskIO struct {
	BasePath string
	me string
	mu sync.Mutex
}

func (d *DiskIO) flushWriteBuffer(writeBuffer *list.List) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	//writeBuffer is a list of RequestValue
	var pathPerm os.FileMode = 0777
	var filePerm os.FileMode = 0666

	// Iterate through list and write its contents.
	for e := writeBuffer.Front(); e != nil; e = e.Next() {
		rv := e.Value.(RequestValue)
		var network bytes.Buffer
		enc := gob.NewEncoder(&network)
		err := enc.Encode(rv)

		if err != nil {
			fmt.Println("Could not encode key: ",rv.Key)
		}

		dirpath := d.BasePath + "/" + d.me + "_" + strconv.Itoa(rv.ConfigNum)
		fullpath := dirpath + "/" + rv.Key


		if err := os.MkdirAll(dirpath, pathPerm); err != nil {
			fmt.Println("Could not make dir",rv.Key)
			return err
		}

		ioutil.WriteFile(fullpath, network.Bytes(), filePerm)
	}

	return nil
}

/* 
	Updates saves metadata and writes to disk
*/
func (d *DiskIO) writeEncode(config int, key string, valToWrite interface{}) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	var network bytes.Buffer
	enc := gob.NewEncoder(&network)
	err := enc.Encode(valToWrite)

	if err != nil {
		fmt.Println("Could not encode key: ",key)
	}

	dirpath := d.BasePath + "/" + d.me + "_" + strconv.Itoa(config)
	fullpath := dirpath + "/" + key

	var pathPerm os.FileMode = 0777
	var filePerm os.FileMode = 0666

	if err := os.MkdirAll(dirpath, pathPerm); err != nil {
		fmt.Println("Could not make dir",key)
		return err
	}

	err = ioutil.WriteFile(fullpath, network.Bytes(), filePerm)
	return err

}

func (d *DiskIO) readPaxosState(config int) (MetaData, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	val := MetaData{}

	path := d.BasePath + "/" + d.me + "_" + strconv.Itoa(config) + "/paxosState"

	fi, err := os.Stat(path)
	if err != nil {
		return val, err
	}
	if fi.IsDir() {
		return val, os.ErrNotExist
	}

	dat, err := ioutil.ReadFile(path)
	network := bytes.NewBuffer(dat)
	dec := gob.NewDecoder(network)
	err = dec.Decode(&val)

	if err != nil {
		panic(err)
	}

	return val, nil
}

func (d *DiskIO) readPeersState(config int) (map[string]int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	val := make(map[string]int)

	path := d.BasePath + "/" + d.me + "_" + strconv.Itoa(config) + "/peersState"

	fi, err := os.Stat(path)
	if err != nil {
		return val, err
	}
	if fi.IsDir() {
		return val, os.ErrNotExist
	}

	dat, err := ioutil.ReadFile(path)
	network := bytes.NewBuffer(dat)
	dec := gob.NewDecoder(network)
	err = dec.Decode(&val)

	if err != nil {
		panic(err)
	}

	return val, nil
}

func (d *DiskIO) readConfigState(config int) (shardmaster.Config, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	val := shardmaster.Config{}

	path := d.BasePath + "/" + d.me + "_" + strconv.Itoa(config) + "/configState"

	fi, err := os.Stat(path)
	if err != nil {
		return val, err
	}
	if fi.IsDir() {
		return val, os.ErrNotExist
	}

	dat, err := ioutil.ReadFile(path)
	network := bytes.NewBuffer(dat)
	dec := gob.NewDecoder(network)
	err = dec.Decode(&val)

	if err != nil {
		panic(err)
	}

	return val, nil
}

func (d *DiskIO) readGetState(config int) (map[int64]DuplicateGet, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	val := make(map[int64]DuplicateGet)

	path := d.BasePath + "/" + d.me + "_" + strconv.Itoa(config) + "/getRequestState"

	fi, err := os.Stat(path)
	if err != nil {
		return val, err
	}
	if fi.IsDir() {
		return val, os.ErrNotExist
	}

	dat, err := ioutil.ReadFile(path)
	network := bytes.NewBuffer(dat)
	dec := gob.NewDecoder(network)
	err = dec.Decode(&val)

	if err != nil {
		panic(err)
	}

	return val, nil
}

func (d *DiskIO) readPutState(config int) (map[int64]DuplicatePut, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	val := make(map[int64]DuplicatePut)

	path := d.BasePath + "/" + d.me + "_" + strconv.Itoa(config) + "/putRequestState"

	fi, err := os.Stat(path)
	if err != nil {
		return val, err
	}
	if fi.IsDir() {
		return val, os.ErrNotExist
	}

	dat, err := ioutil.ReadFile(path)
	network := bytes.NewBuffer(dat)
	dec := gob.NewDecoder(network)
	err = dec.Decode(&val)

	if err != nil {
		panic(err)
	}

	return val, nil
}

func (d *DiskIO) readValue(config int, key string) (RequestValue, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	path := d.BasePath + "/" + d.me + "_" + strconv.Itoa(config) + "/" + key

	fi, err := os.Stat(path)
	if err != nil {
		return RequestValue{}, err
	}
	if fi.IsDir() {
		return RequestValue{}, os.ErrNotExist
	}

	dat, err := ioutil.ReadFile(path)
	network := bytes.NewBuffer(dat)
	dec := gob.NewDecoder(network)

	// We have to define concrete datatype here
	val := RequestValue{}
	err = dec.Decode(&val)

	if err != nil {
		fmt.Println("Error decoding ",key," err:", err.Error())
		panic(err)
	}

	return val, nil
}

func (d *DiskIO) write(config int, key string, val string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	dirpath := d.BasePath + "/" + d.me + "_" + strconv.Itoa(config)
	fullpath := dirpath + "/" + key

	var pathPerm os.FileMode = 0777
	var filePerm os.FileMode = 0666

	if err := os.MkdirAll(dirpath, pathPerm); err != nil {
		return err
	}

	ioutil.WriteFile(fullpath, []byte(val), filePerm)

	return nil
}

func (d *DiskIO) writeKVMap(config int, mapToWrite map[string]RequestValue) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	dirpath := d.BasePath + "/" + d.me + "_" + strconv.Itoa(config)
	var pathPerm os.FileMode = 0777
	var filePerm os.FileMode = 0666

	if err := os.MkdirAll(dirpath, pathPerm); err != nil {
		return err
	}

	for key, val := range mapToWrite {
		fullpath := dirpath + "/" + key
		fmt.Println("Diskio, writing map: ",key," val: ",val.Value)
		var network bytes.Buffer
		enc := gob.NewEncoder(&network)
		enc.Encode(val)
		ioutil.WriteFile(fullpath, network.Bytes(), filePerm)
	}

	return nil
}

func (d *DiskIO) read(config int, key string) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	path := d.BasePath + "/" + d.me + "_" + strconv.Itoa(config) + "/" + key

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

//
// For now, return current config for which we have information
func (d *DiskIO) latestConfigNum() (int, error) {
	path := d.BasePath + "/" 
	matches, _ := filepath.Glob(path + "*")

	maxConfig := 0
	for _, filename := range matches {

		pathtok := strings.Split(filename, "/")
		dirname := pathtok[len(pathtok)-1]
		split := strings.Split(dirname, "_")
		if split[0] != d.me {
			continue
		}
		configNum, _ := strconv.Atoi(split[1])
		if configNum > maxConfig {
			maxConfig = configNum
		}
	}

	return maxConfig, nil
}

func (d *DiskIO) export(config int) (map[string]RequestValue, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	retMap := make(map[string]RequestValue)

	path := d.BasePath + "/" + d.me + "_" + strconv.Itoa(config)

	files, err := ioutil.ReadDir(path)

	if err != nil {

	}

	for _, file := range files {
		key := file.Name()
		if _, isState := stateFileNames[key]; isState {
			continue
		}
		dat, e := ioutil.ReadFile(path + "/" + key)
		network := bytes.NewBuffer(dat)
		dec := gob.NewDecoder(network)

		// We have to define concrete datatype here
		val := RequestValue{}
		err = dec.Decode(&val)

		if e != nil {
			// Some file could not be written...
		}
		retMap[key] = val
	}

	return retMap, nil
}

//
// Server was gracefully shut down, wipe kv from disk
//
func (d *DiskIO) cleanState() {
	os.RemoveAll(d.BasePath)
}

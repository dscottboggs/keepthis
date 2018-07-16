package data

/*
data

This package is a simple wrapper around steamrail's concurrent-map, which
writes the contents to a file every 3 seconds. It is intended to be a high-
speed alternative to a nosql database. You must only use one instance of it,
but it can be written to and read from concurrently, and will sync the current
state to a file, albeit, up to 3 seconds later.
*/

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/streamrail/concurrent-map"
)

// TData -- struct to hold the database, its
// filepath, and some custom methods.
type TData struct {
	filepath string
	cmap.ConcurrentMap
}

// Init the database at the given filepath
func Init(filepath string) (TData, error) {
	var this TData
	_, err := os.Stat(filepath)
	if os.IsNotExist(err) {
		// File doesn't exist, create it.
		this = TData{filepath, cmap.New()}
		return this, nil
	}
	if err != nil && !os.IsExist(err) {
		return TData{}, err
	}
	// File exists already, read into Data.
	var readData map[string]interface{}
	f, err := os.Open(filepath)
	if err != nil && !os.IsNotExist(err) {
		return TData{}, err
	}
	if err := json.NewDecoder(f).Decode(&readData); err != nil {
		return TData{}, err
	}
	d := cmap.New()

	d.MSet(readData)
	this = TData{filepath, d}
	return this, nil
}

// DoInit -- calls the regular Init, then starts the filesync thread before
// returning.
func DoInit(
	filepath string,
	quitter chan bool,
	databaseError chan error) (TData, error) {
	this, err := Init(filepath)
	if err != nil {
		return TData{}, err
	}
	go this.StartSync(quitter, databaseError)
	return this, nil
}

// Write the existing data to a file. Uses a
// lock file to avoid concurrent writes.
func (d *TData) store() error {
	dataChan := make(chan []byte)
	errChan := make(chan error)
	go func(errChan chan error) {
		d, err := d.MarshalJSON()
		if err != nil {
			errChan <- err
		}
		errChan <- nil
		dataChan <- d
	}(errChan)
	lock, err := os.Create(d.filepath + ".lock")
	if err != nil {
		lock.Close()
		return fmt.Errorf("creating lock file: %v", err)
	}
	dbFile, err := os.Create(d.filepath)
	if os.IsNotExist(err) && err != nil {
		dbFile.Close()
		lock.Close()
		if e := os.Remove(d.filepath + ".lock"); e != nil {
			return fmt.Errorf(
				"opening json file: %v, and while removing lock file: %v",
				err,
				e)
		}
		return fmt.Errorf("opening json file: %v", err)
	}
	if err = <-errChan; err != nil {
		return fmt.Errorf("interpreting db to json: %v", err)
	}
	data := <-dataChan
	num, err := dbFile.Write(append(data, '\n'))
	dbFile.Close()
	if err != nil {
		return fmt.Errorf("writing json file: %v", err)
	}
	if num < len(data) {
		return fmt.Errorf("wrote %d bytes instead of %d to json file", num, len(data))
	}
	lock.Close()
	if err = os.Remove(d.filepath + ".lock"); err != nil {
		return fmt.Errorf("deleting lock file: %v", err)
	}
	return nil
}

// StartSync --
// Have d call store() on itself.every interval seconds, until the quit
// channel receives true. The library sets interval to once every 3 seconds.
// This func must have its own goroutine, e.g...
//
//     go data.StartSync(syncQuitter, syncError)
//
// ...where data is a TData pointer and syncQuitter is a channel that will be
// written to when it's time to quit. If an error occurs it will be pushed
// out on the error channel.
//
func (d *TData) StartSync(quit chan bool, err chan error) {
	interval := time.NewTicker(3 * time.Second)
	for {
		select {
		case <-quit:
			interval.Stop()
			return
		case <-interval.C:
			e := d.store()
			if e != nil && !os.IsExist(e) {
				err <- e
			}
		}
	}
}

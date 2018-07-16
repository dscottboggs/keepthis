package data

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/dscottboggs/attest"
)

func TestDataSync(t *testing.T) {
	test := attest.Test{NativeTest: t}
	d, err := Init("./testdataSync.json")
	defer os.Remove(d.filepath)
	if err != nil {
		t.Errorf("during db init: %v", err)
	}
	d.Set("1", "Test1")
	d.Set("2", "Test2")
	if err = d.store(); err != nil {
		t.Fatal(err)
	}
	dFile, err := os.Open(d.filepath)
	if err != nil {
		t.Errorf("while opening db file: %v", err)
	}
	defer dFile.Close()
	fromFile := make([]byte, 100)
	dFile.Read(fromFile)
	test.Equals(
		`{"1":"Test1","2":"Test2"}`,
		string(bytes.Trim(fromFile, "\x00\n")))
}

func TestDoInit(t *testing.T) {
	test := attest.Test{NativeTest: t}
	quit := make(chan bool)
	databaseError := make(chan error)
	d, err := DoInit("./TestDoInitData.json", quit, databaseError)
	test.Handle(err)
	defer os.Remove(d.filepath)
	go func() {
		for {
			test.Handle(<-databaseError)
		}
	}()
	for i := 0; i < 50; i++ {
		go test.Attest(
			d.SetIfAbsent(fmt.Sprintf("%d", i), fmt.Sprintf("Test %d", i)),
			"Value %d was already set!",
			i)
	}
	verify := make(map[string]string)
	for i := 0; i < 50; i++ {
		verify[fmt.Sprintf("%d", i)] = fmt.Sprintf("Test %d", i)
	}
	beforeWrite := make(map[string]interface{})
	d.IterCb(func(key string, value interface{}) {
		// this, hypothetically, could not finish before the write happens, but
		// really the two are unrelated.
		beforeWrite[key] = value
	})
	time.Sleep(5 * time.Second)
	fromFile := make(map[string]interface{})
	dataFile, err := os.Open("./TestDoInitData.json")
	test.Handle(err, json.NewDecoder(dataFile).Decode(&fromFile))
	afterWrite := make(map[string]interface{})
	d.IterCb(func(key string, value interface{}) {
		afterWrite[key] = value
	})
	for k, v := range fromFile {
		test.Equals(verify[k], v)
		test.Equals(v, beforeWrite[k])
		test.Equals(v, afterWrite[k])
	}
	quit <- true
}

func TestExistingFileIsReadOnInit(t *testing.T) {
	test := attest.Test{NativeTest: t}
	d1, err := Init("./TestExistingFileIsReadOnInit.json")
	defer os.Remove(d1.filepath)
	if err != nil {
		t.Errorf("during db init: %v", err)
	}
	d1.Set("1", "Test1")
	d1.Set("2", "Test2")
	if err = d1.store(); err != nil {
		t.Fatal(err)
	}
	d1File, err := os.Open(d1.filepath)
	if err != nil {
		t.Errorf("while opening db file: %v", err)
	}
	defer d1File.Close()
	d2, err := Init("./TestExistingFileIsReadOnInit.json")
	defer os.Remove(d2.filepath)
	if err != nil {
		t.Errorf("during db init: %v", err)
	}
	d2.Set("1", "Test1")
	d2.Set("2", "Test2")
	if err = d2.store(); err != nil {
		t.Fatal(err)
	}
	d2File, err := os.Open(d2.filepath)
	if err != nil {
		t.Errorf("while opening db file: %v", err)
	}
	defer d2File.Close()
	fromFile := make([]byte, 100)
	d1File.Read(fromFile)
	d1res, d1err := d1.MarshalJSON()
	d2res, d2err := d2.MarshalJSON()
	test.Handle(d1err, d2err)
	test.Equals(string(d1res), string(d2res))
}

package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strconv"
)

func MapTask(mapf func(string, string) []KeyValue, fileName string, nth string, nReduce int) {
	//Open file and map it
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	file.Close()
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}

	kva := mapf(fileName, string(content))
	ofname := "mr-" + nth + "-"
	//Write n file back
	outFiles := []*os.File{}
	for i := 0; i < nReduce; i++ {
		//ofile, _ := os.Create(ofname + strconv.Itoa(i))
		ofile, _ := ioutil.TempFile(".", "pre")
		defer os.Rename(ofile.Name(), ofname+strconv.Itoa(i))
		outFiles = append(outFiles, ofile)
	}
	for _, kv := range kva {
		f := ihash(kv.Key) % nReduce
		enc := json.NewEncoder(outFiles[f])
		err := enc.Encode(&kv)
		if err != nil {
			fmt.Println("fail to wite in map")
		}
	}
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func ReduceTask(reducef func(string, []string) string, nth string, nMap int) {
	//Read content i data directory
	kva := []KeyValue{}
	//Read file and sort it
	for i := 0; i < nMap; i++ {
		fname := "mr-" + strconv.FormatInt(int64(i), 10) + "-" + nth
		file, err := os.Open(fname)
		if err != nil {
			log.Fatalf("cannot open %v", fname)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	//output, _ := os.Create("mr-out-" + nth)
	output, _ := ioutil.TempFile(".", "pre")
	defer os.Rename(output.Name(), "mr-out-"+nth)
	sort.Sort(ByKey(kva))
	for i := 0; i < len(kva); {
		key := kva[i].Key
		values := []string{kva[i].Value}
		for i += 1; i < len(kva) && key == kva[i].Key; i++ {
			values = append(values, kva[i].Value)
		}
		fmt.Fprintf(output, "%v %v\n", key, reducef(key, values))
	}
}

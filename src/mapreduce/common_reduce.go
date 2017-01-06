package mapreduce

import (
	"os"
	"encoding/json"
	"sort"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	kvList:=make(map[string][]string)
	kList:=make([]string,0)
	for index:=0;index<nMap;index++ {
		interFileName:=reduceName(jobName, index, reduceTaskNumber)
		file,err:=os.Open(interFileName)
		defer file.Close()
		if err!=nil {
			panic(err)
		}

		kvs:=make([]KeyValue,0)
		decoder:=json.NewDecoder(file)
		kv:=new(KeyValue)
		for {
			err=decoder.Decode(kv)
			if err!=nil {
				break
			}
			if kv.Key!="" {
				kvs=append(kvs,*kv)
			}

		}

		for _,kv:=range kvs {
			valueList,ok:=kvList[kv.Key]
			if ok {
				valueList=append(valueList,kv.Value)
				kvList[kv.Key]=valueList
			} else {
				valueList=make([]string,0)
				valueList=append(valueList,kv.Value)
				kvList[kv.Key]=valueList
				kList=append(kList,kv.Key)
			}
		}

	}

	sort.Strings(kList)
	outFile,err:=os.Create(mergeName(jobName,reduceTaskNumber))
	defer outFile.Close()
	if err!=nil {
		panic(err)
	}
	encoder:=json.NewEncoder(outFile)
	for _,key:=range kList {
		result:=reduceF(key,kvList[key])
		encoder.Encode(KeyValue{key,result})
	}
}

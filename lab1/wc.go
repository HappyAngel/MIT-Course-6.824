package main

import "container/list"
import "fmt"
import "os"
import "strconv"
import "strings"
import "unicode"

import "mapreduce"


// our simplified version of MapReduce does not supply a
// key to the Map function, as in the paper; only a value,
// which is a part of the input file content. the return
// value should be a list of key/value pairs, each represented
// by a mapreduce.KeyValue.
func Map(value string) *list.List {
    // Lei's code start here
    f := func(c rune) bool{
        return !unicode.IsLetter(c)
    }
    
    splitStrings := strings.FieldsFunc(value, f)
    l := list.New()
    
    for i:=0; i < len(splitStrings); i++ {
        var keyValuePair mapreduce.KeyValue
        keyValuePair.Key = splitStrings[i]
        keyValuePair.Value = "1"
        l.PushBack(keyValuePair)
    }
    
    return l
    // end
}

// called once for each key generated by Map, with a list
// of that key's string value. should return a single
// output value for that key.
func Reduce(key string, values *list.List) string {
    // Lei's code start here
    var outputValue int
    
    for e := values.Front(); e != nil; e = e.Next() {
        v, err := strconv.Atoi(e.Value.(string))
        
        if err == nil {
            outputValue += v
        } else {
            return "-1"
        }
    }
    
    return strconv.Itoa(outputValue)
    // end
}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master x.txt sequential)
// 2) Master (e.g., go run wc.go master x.txt localhost:7777)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
func main() {
	if len(os.Args) != 4 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	} else if os.Args[1] == "master" {
		if os.Args[3] == "sequential" {
			mapreduce.RunSingle(5, 3, os.Args[2], Map, Reduce)
		} else {
			mr := mapreduce.MakeMapReduce(5, 3, os.Args[2], os.Args[3])
			// Wait until MR is done
			<-mr.DoneChannel
		}
	} else {
		mapreduce.RunWorker(os.Args[2], os.Args[3], Map, Reduce, 100)
	}
}

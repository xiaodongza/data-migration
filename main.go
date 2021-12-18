package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
)

var dataPath *string
var dstIP *string
var dstPort *int
var dstUser *string
var dstPassword *string

//  example of parameter parse, the final binary should be able to accept specified parameters as requested
//
//  usage example:
//      ./run --data_path /tmp/data --dst_ip 127.0.0.1 --dst_port 3306 --dst_user root --dst_password 123456789
//
//  you can test this example by:
//  go run main.go --data_path /tmp/data --dst_ip 127.0.0.1 --dst_port 3306 --dst_user root --dst_password 123456789
func main() {
	dataPath = flag.String("data_path", "/tmp/data", "dir path of source data")
	dstIP = flag.String("dst_ip", "", "ip of dst database address")
	dstPort = flag.Int("dst_port", 0, "port of dst database address")
	dstUser = flag.String("dst_user", "", "user name of dst database")
	dstPassword = flag.String("dst_password", "", "password of dst database")

	flag.Parse()
	fmt.Printf("data path:%v\n", *dataPath)
	fmt.Printf("dst ip:%v\n", *dstIP)
	fmt.Printf("dst port:%v\n", *dstPort)
	fmt.Printf("dst user:%v\n", *dstUser)
	fmt.Printf("dst password:%v\n", *dstPassword)
	fmt.Println(*dataPath)
	dbs := make([]string, 0)
	dbs = append(dbs, "a", "b", "c", "d", "e", "f", "g")
	for i := 1; i <= 28; i++ {
		for _, db := range dbs {
			csvfile, err := os.Open(*dataPath + "/src_a/" + db + "/" + strconv.Itoa(i) +".csv")
			//fmt.Println(*dataPath + "src_a/a/1.csv")
			if err != nil {
				log.Fatalln("Couldn't open the csv file", err)
			}
			defer csvfile.Close()
			// Parse the file
			r := csv.NewReader(csvfile)
			// Iterate through the records
			for {
				// Read each record from csv
				record, err := r.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					log.Fatal(err)
				}
				for rec := range record {
					log.Println("%s\n", rec)
					fmt.Printf("%s\n", rec)
				}
			}
		}
	}

}

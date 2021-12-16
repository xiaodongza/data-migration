package main


import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	//iconv "github.com/djimenez/iconv-go"
)

var data_path = "F:\\data\\src_a"

func read() {
	// Op en the file
	csvfile, err := os.Open(path("a","1"))
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
		fmt.Printf("Record has %d columns.\n", len(record))
		//city, _ := iconv.ConvertString(record[2], "gb2312", "utf-8")
		//fmt.Printf("%s %s %s \n", record[0], record[1], city)
	}
}

func path(database, table string) string {
	return data_path + "\\" + database + "\\" + table + ".csv"
}

package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"time"
)


const (
	data_path 	= 	"F:\\data\\src_a"
	layout 		= 	"2006-01-02 15:04:05"
)

//读取csv文件中的数据
func read() {
	// Op en the file
	csvfile, err := os.Open(tablepath("a","1"))
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
		//fmt.Printf("Record has %d columns.\n", len(record))
		//city, _ := iconv.ConvertString(record[2], "gb2312", "utf-8")
		fmt.Printf("%s %s %s %s \n", record[0], record[1], record[2], record[3])
	}
}




//表的路径
func tablepath(database, table string) string {
	return data_path + "\\" + database + "\\" + table + ".csv"
}




//由于键冲突时需要取时间早的数据，因此我们用这个函数来判断哪个时间比较早
func early(time1, time2 string) bool {
	t1, err := time.Parse(layout,time1)
	t2, err := time.Parse(layout,time2)
	if err != nil {
		fmt.Printf("compare time error:" ,err)
	}
	if err == nil && t1.Before(t2) {
		fmt.Printf("time1 is early")
		return true
	} else {
		fmt.Printf("time2 is early")
		return false
	}
}

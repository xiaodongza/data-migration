package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"time"
)


const (
	layout 		= 	"2006-01-02 15:04:05"
)

//读取csv文件中的数据
func reader(folder, database, table string) (*csv.Reader, error) {
	// Op en the file
	csvfile, err := os.Open("F:\\data\\" + folder + "\\" + database + "\\" + table + ".csv")
	if err != nil {
		log.Fatalln("Couldn't open the csv file", err)
		return nil, err
	}
	defer csvfile.Close()
	// Parse the file
	r := csv.NewReader(csvfile)
	// Iterate through the records
	//for {
	//	record, err := r.Read()
	//	if err == io.EOF {
	//		break
	//	}
	//	if err != nil {
	//		log.Fatal(err)
	//	}
	//	for _, rec := range record {
	//		fmt.Print("%s\n", rec)
	//	}
	//}
	return r, nil
}




//表的路径
func tablepath(database, table string) string {
	return *dataPath + "/src_a" + "/" + database + "/" + table + ".csv"
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

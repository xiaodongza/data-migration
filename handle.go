package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
)

func handleData() {
	dbs := make([]string, 0)
	//dbs = append(dbs, "a", "b", "c", "d", "e", "f", "g")
	dbs = append(dbs, "a", "b", "c", "d")
	//for i := 1; i <= 28; i++ {
	for i := 1; i <= 4; i++ {
		//var ch chan *[]string queue = []
		var queue []*[]string
		for _, db := range dbs {
			csvfile, err := os.Open("F:\\data\\" + "src_a" + "\\" + db + "\\" + strconv.Itoa(i) + ".csv")
			if err != nil {
				log.Fatalln("Couldn't open the csv file", err)
			}
			defer csvfile.Close()
			r := csv.NewReader(csvfile)
			for {
				record, err := r.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					log.Fatal(err)
				}
				queue = append(queue, &record)
			}
		}
		_, _, unique_column_index, primary_column_index :=  HandleSql("src_a", "a", strconv.Itoa(i))
		for index := range primary_column_index {
			m := make(map[string]*[]string, 0)
			for len(queue) > 0 {
				rec := queue[0]
				queue =  queue[1: len(queue)]
				rec1 := *rec
				cur := rec1[index]
				value, ok := m[cur]
				if ok {
					value1 := *value
					time_pre := value1[len(value1) - 1]
					time_cur := rec1[len(rec1) - 1]
					if(time_cur < time_pre) {
						m[cur] = rec
					}
				} else {
					m[cur] = rec
				}
			}
			for _, rec := range m {
				rec1 := *rec
				queue = append(queue, &rec1)
			}
		}
		for index := range unique_column_index {
			m := make(map[string]*[]string, 0)
			for len(queue) > 0 {
				rec := queue[0]
				queue =  queue[1: len(queue)]
				rec1 := *rec
				cur := rec1[index]
				value, ok := m[cur]
				if ok {
					value1 := *value
					time_pre := value1[len(value1) - 1]
					time_cur := rec1[len(rec1) - 1]
					if(time_cur < time_pre) {
						m[cur] = rec
					}
				} else {
					m[cur] = rec
				}
			}
			for _, rec := range m {
				rec1 := *rec
				queue = append(queue, &rec1)
			}
		}
		for _, rec := range queue {
			fmt.Println(*rec)
		}
	}

}




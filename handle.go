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
	dbs = append(dbs, "a", "b", "c", "d", "e", "f", "g")
	srcs := make([]string, 0)
	srcs = append(srcs, "src_a", "src_b")
	for _, db := range dbs {
		createDatabase(db)
	}
	for i := 1; i <= 4; i++ {
		var queue []*[]string
		for _, db := range dbs {
			for _, src := range srcs {
				//csvfile, err := os.Open("F:\\data\\" + src + "\\" + db + "\\" + strconv.Itoa(i) + ".csv")
				csvfile, err := os.Open(*dataPath + "/"+ src + "/" + db + "/" + strconv.Itoa(i) + ".csv")
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
			for _, index := range unique_column_index {
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
						if time_cur < time_pre {
							m[cur] = rec
						}
					} else {
						m[cur] = rec
					}
				}
				for _, rec := range m {
					queue = append(queue, rec)
				}
				for _, rec := range queue {
					fmt.Println("%v", rec)
				}
			}
			if len(primary_column_index) == 1 {
				index := primary_column_index[0]
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
			} else if len(primary_column_index) > 1{
				index := primary_column_index[0]
				m := make(map[string]*[]string, 0)
				for len(queue) > 0 {
					rec := queue[0]
					queue =  queue[1: len(queue)]
					rec1 := *rec
					cur := rec1[index]
					value, ok := m[cur]
					if ok {
						value1 := *value
						if euqals(rec1, value1, len(primary_column_index)){
							timePre := value1[len(value1) - 1]
							timeCur := rec1[len(rec1) - 1]
							if(timeCur < timePre) {
								m[cur] = rec
							}
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
			sqlExec(db, i, queue)
		}
	}
}

func euqals(a, b []string, target int) bool {
	num := 0
	for i := 0; i < len(a); i++ {
		if a[i] == b[i] {
			num++
			if num == target {
				return true
			}
		}
	}
	return false
}




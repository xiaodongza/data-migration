package main

import (
	"encoding/csv"
	"io"
	"log"
	"os"
	"strconv"
)

func handleData() {
	dbs := make([]string, 0)
	dbs = append(dbs, "a", "b", "c", "d", "e", "f", "g")
	//dbs = append(dbs, "a", "b", "c", "d")

	srcs := make([]string, 0)
	srcs = append(srcs, "src_a", "src_b")
	for _, db := range dbs {
		createDatabase(db)
	}
	for i := 1; i <= 4; i++ {
		var queue []*[]string
		for _, db := range dbs {
			for _, src := range srcs {
				csvfile, err := os.Open("F:\\data\\" + src + "\\" + db + "\\" + strconv.Itoa(i) + ".csv")
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
				m_help := make(map[*string]*[]string, 0)
				for _, rec := range queue {
					rec1 := *rec
					m_help[&rec1[primary_column_index[0]]] = rec
				}
				index := primary_column_index[0]
				m := make(map[string]*[]string, 0)
				for len(queue) > 0 {
					rec := queue[0]
					queue =  queue[1: len(queue)]
					rec1 := *rec
					cur := rec1[index]
					value, ok := m[cur]
					if ok {
						if euqals(m_help[&cur], m_help[&rec1[index]]) {
							value1 := *value
							time_pre := value1[len(value1) - 1]
							time_cur := rec1[len(rec1) - 1]
							if(time_cur >= time_pre) {
								delete(m_help, &cur)
							} else {
								delete(m_help, &rec1[index])
							}
						}
					} else {
						m[cur] = rec
					}
				}
				for _, rec := range m_help {
					queue = append(queue, rec)
				}
			}
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
						if(time_cur < time_pre) {
							m[cur] = rec
						}
					} else {
						m[cur] = rec
					}
				}
				for _, rec := range m {
					queue = append(queue, rec)
				}
			}
			sqlExec(db, i, queue)
		}
	}
}

func euqals(a, b *[]string) bool {
	a1, b1 := *a, *b
	for i := 0; i < len(a1); i++ {
		if a1[i] != b1[i] {
			return false
		}
	}
	return true
}




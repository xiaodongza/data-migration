package main

import (
	"fmt"
	"io"
	"log"
	"sort"
)

/*slice 排序示例*/
type Row struct {
	data 			[]string
	primaryIndex 	int
}

type RowSlice []Row

func (s RowSlice) Len() int           { return len(s) }
func (s RowSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s RowSlice) Less(i, j int) bool { return s[i].data[s[i].primaryIndex] < s[j].data[s[j].primaryIndex] }

func sort_data() {
	r, _ := reader("src_a", "a","1")
	_, _, _, primaryColumnIndex := HandleSql("src_a", "a", "1")
	primaryIndex := -1
	if len(primaryColumnIndex) != 0 {
		primaryIndex = primaryColumnIndex[0]
	}
	rowSlice := make(RowSlice, 0)
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		row := Row{record, primaryIndex}
		rowSlice = append(rowSlice, row)
	}
	sort.Sort(rowSlice)
	fmt.Printf("%v", rowSlice)
}
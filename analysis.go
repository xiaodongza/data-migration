package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
)

func HandleSql() (string, []string, []int, []int) {
	var table_name string
	//0表示普通键，1表示时间，2表示非空键，3表示主键
	column_name := make([]string,0)
	primary_column_name := make([]string, 0)
	primary_column_index := make([]int, 0)
	unique_column_index := make([]int, 0)
	file, err := os.Open(sqlpath("a", "1"))
	if err != nil {
		log.Printf("Cannot open text file: %s, err: [%v]", sqlpath("a", "1"), err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	num_column := 0
	for scanner.Scan() {
		line := scanner.Text()
		if num_column == 0 {
			table_name = get_table_name(line)
			num_column++
		} else if is_column(line) {
			column_name_cur := get_column_name(line)
			column_name = append(column_name, column_name_cur)
			if is_unique(line) {
				unique_column_index = append(unique_column_index, num_column)
			}
			num_column++
		} else if !is_lastline(line){
			get_primary_name(&primary_column_name, line)
		}
		//fmt.Printf("%s\n", line)
	}
	for i := 0; i < len(primary_column_name); i++ {
		for j := 0; j < len(column_name); j++ {
			if strings.Compare(primary_column_name[i], column_name[j]) == 0 {
				primary_column_index = append(primary_column_index, j)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		log.Printf("Cannot scanner text file: %s, err: [%v]", sqlpath("a", "1"), err)
	}
	fmt.Println(table_name)
	fmt.Println(column_name)
	fmt.Println(unique_column_index)
	fmt.Println(primary_column_index)
	return table_name, column_name, unique_column_index, primary_column_index
}



func sqlpath(database, table string) string {
	return data_path + "\\" + database + "\\" + table + ".sql"
}

func get_table_name(former string) string {
	start, end := 0, len(former)
	for i := 0; i < len(former); i++ {
		if former[i] == '`' {
			start = i
			break
		}
	}
	for i := len(former) - 1; i >= 0; i-- {
		if former[i] == '`' {
			end = i
			break
		}
	}
	return former[start + 1: end]
}

func get_column_name(former string) string {
	num, pre := 0, 0
	for i := 0; i < len(former); i++ {
		if former[i] == '`' {
			num++
			if num%2 == 1 {
				pre = i
			}
			if num%2 == 0 {
				return former[pre + 1: i]
			}
		}
	}
	return ""
}

func get_primary_name(ans *[]string, former string) {
	num, pre := 0, 0
	for i := 0; i < len(former); i++ {
		if former[i] == '`' {
			num++
			if num%2 == 1 {
				pre = i
			}
			if num%2 == 0 {
				*ans = append(*ans, former[pre + 1: i])
			}
		}
	}
}

func is_column(former string) bool {
	for i := 0; i < len(former); i++ {
		if former[i] == ' ' {
			continue
		} else if former[i] == '`' {
			return true;
		} else {
			return false
		}
	}
	return false
}

func is_lastline(former string) bool {
	if former[0] == '(' {
		return true
	}
	return false
}

func is_unique(former string) bool {
	if strings.Index(former, "unique") == -1 {
		return false
	}
	return true
}

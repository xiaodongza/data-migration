package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
)

func HandleSql() (string, []int, int) {
	var table_name string
	//0表示普通键，1表示时间，2表示非空键，3表示主键
	data_category := make([]int,0)
	data_category_num := 0
	file, err := os.Open(sqlpath("a", "1"))
	if err != nil {
		log.Printf("Cannot open text file: %s, err: [%v]", sqlpath("a", "1"), err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	index := 0
	for scanner.Scan() {
		line := scanner.Text()  // or
		//line := scanner.Bytes()
		if index == 0 {
			table_name = get_table_name(line)
		} else if {

		}

		//do_your_function(line)
		fmt.Printf("%s\n", line)
	}

	if err := scanner.Err(); err != nil {
		log.Printf("Cannot scanner text file: %s, err: [%v]", sqlpath("a", "1"), err)
	}

	return table_name, data_category, data_category_num
}



func sqlpath(database, table string) string {
	return data_path + "\\" + database + "\\" + table + ".sql"
}

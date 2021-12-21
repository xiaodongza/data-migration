package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
)

type info struct{
	id 				string 	`db:"id"`
	a 			string 	`db:"a"`
	b 			string 	`db:"b"`
	updated_at  string  `db:"updated_at"`
}


func createDatabase(database string) {
	db, err := sql.Open("mysql", "root:134676@tcp(localhost:3306)/?charset=utf8")
	//db, err := sql.Open("mysql", "hjd:Hejundong1998.@tcp(182.254.128.133:138)/?charset=utf8")
	//db, err := sql.Open("mysql", *dstUser + ":" + *dstPassword + "@tcp("+ *dstIP + ":" + strconv.Itoa(*dstPort) + ")/?charset=utf8")
	if err != nil {
		fmt.Println("connect failed",err)
	}
	_, err = db.Exec(makeDropDatabaseSql(database))
	if err != nil {
		fmt.Println("drop database failed",err)
	}
	_, err = db.Exec(makeCreateDatabaseSql(database))
	if err != nil {
		fmt.Println("create database failed",err)
	}
}

func sqlExec(database string, i int,queue []*[]string) {
	//用户名：密码@tcp(地址:3306)/数据库
	//db, err := sql.Open("mysql", *dstUser + ":" + *dstPassword + "@tcp("+ *dstIP + ":" + strconv.Itoa(*dstPort) + ")/?charset=utf8")
	db, err := sql.Open("mysql", "root:134676@tcp(localhost:3306)/?charset=utf8")
	//db, err := sql.Open("mysql", "hjd:Hejundong1998.@tcp(182.254.128.133:138)/" + database + "?charset=utf8")
	if err != nil {
		fmt.Println("connect failed",err)
	}
	//创建表
	_, err = db.Exec(makeUseDatabaseSql(database))
	if err != nil {
		fmt.Println("use database failed",err)
	}
	_, err = db.Exec(makeCreateTableSql("src_a", database, strconv.Itoa(i)))
	if err != nil {
		fmt.Println("create table failed", err)
	}
	//for len(queue) > 0{
	//	rec := queue[0]
	//	queue =  queue[1: len(queue)]
	//	rec1 := *rec
	//	db.Exec(makeInsertSql(strconv.Itoa(i), rec1))
	//}
	sizeOfBathInsert := 1000
	for len(queue) > sizeOfBathInsert {
		_, err := db.Exec(makeBatchInsertSql(strconv.Itoa(i), sizeOfBathInsert, queue))
		if err != nil {
			fmt.Println(err)
		}
		queue =  queue[sizeOfBathInsert: len(queue)]
	}
	if len(queue) > 0 {
		db.Exec(makeBatchInsertSql(strconv.Itoa(i), len(queue), queue))
		queue =  queue[len(queue):]
	}
	//rows, err := db.Query(`select * from` + "`1`" + `;`)
	//fmt.Println("select * from \x601\x60;")
	//if err != nil {
	//	fmt.Println("query failed",err)
	//}
	//for rows.Next(){
	//	var s info
	//	err = rows.Scan(&s.id, &s.a, &s.b, &s.updated_at)
	//	if err != nil {
	//		fmt.Errorf("scan failed",err)
	//	}
	//	fmt.Println(s)
	//}
	//rows.Close()
}

func makeInsertSql(table_name string, row []string) string {
	sentence := "INSERT INTO `" + table_name + "` VALUES("
	for i, meta_data := range row {
		if i != 0 {
			sentence = sentence + ","
		}
		sentence = sentence + "\"" + meta_data + "\""
	}
	sentence = sentence + ");"
	fmt.Println(sentence)
	return sentence
}

func makeBatchInsertSql(table_name string, r int, queue []*[]string) string {
	sentence := "INSERT INTO `" + table_name + "` VALUES "
	for i := 0; i < r; i++ {
		sentence = sentence + "("
		row := queue[i]
		row1 := *row
		for j, meta_data := range row1 {
			if j != 0 {
				sentence = sentence + ","
			}
			sentence = sentence + "\"" + meta_data + "\""
		}
		sentence = sentence + ")"
		if i != r - 1 {
			sentence = sentence + ","
		}
	}
	sentence = sentence + ";"
	//fmt.Println(sentence)
	return sentence
}

func makeCreateTableSql(folder, database, table string) string {
	file, err := os.Open("F:\\data\\" + folder + "\\" + database + "\\" + table + ".sql")
	//file, err := os.Open(*dataPath + "/" + folder + "/" + database + "/" + table + ".sql")
	if err != nil {
		log.Printf("Cannot open sql file, err: [%v]", err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	sentence := ""
	for scanner.Scan() {
		line := scanner.Text()
		sentence = sentence + line
	}
	sentence = sentence + ";"
	fmt.Println(sentence)
	return sentence
}

func makeDropDatabaseSql(database string) string {
	sentence := ""
	sentence = sentence + "DROP DATABASE " + database + ";"
	fmt.Println(sentence)
	return sentence
}

func makeCreateDatabaseSql(database string) string {
	sentence := ""
	sentence = sentence + "CREATE DATABASE " + database + ";"
	fmt.Println(sentence)
	return sentence
}

func makeUseDatabaseSql(database string) string {
	sentence := ""
	sentence = sentence + "USE " + database + ";"
	fmt.Println(sentence)
	return sentence
}






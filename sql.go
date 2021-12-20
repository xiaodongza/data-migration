package main

import (
	"bufio"
	"database/sql"
	"fmt"
	_ "github.com/jinzhu/gorm/dialects/mysql"
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
	db, err := sql.Open("mysql", "root:134676@tcp(localhost:3306)/tencent_cloud?charset=utf8")
	if err != nil {
		fmt.Println("connect failed",err)
	}
	_, err = db.Exec(makeCreateDatabaseSql(database))
	if err != nil {
		fmt.Println("create database failed",err)
	}
}

func sqlExec(database string, i int,queue []*[]string) {
	//用户名：密码@tcp(地址:3306)/数据库
	//db, err := sql.Open("mysql", "hjd:Hejundong1998.@tcp(localhost:3306)/tencent_cloud?charset=utf8")
	db, err := sql.Open("mysql", "root:134676@tcp(localhost:3306)/tencent_cloud?charset=utf8")
	if err != nil {
		fmt.Println("connect failed",err)
	}
	//创建表
	_, err = db.Exec(makeCreateTableSql("src_a", database, strconv.Itoa(i)))
	if err != nil {
		fmt.Println(err)
	}
	for len(queue) > 0 {
		rec := queue[0]
		queue =  queue[1: len(queue)]
		rec1 := *rec
		db.Exec(makeInsertSql(strconv.Itoa(i), rec1))
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

func makeCreateDatabaseSql(database string) string {
	sentence := ""
	sentence = sentence + "CREATE DATABASE" + database + ";"
	return sentence
}






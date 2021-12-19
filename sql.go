package main

import (
	"database/sql"
	"fmt"
	_ "github.com/jinzhu/gorm/dialects/mysql"
)

type info struct{
	id 				string 	`db:"id"`
	a 			string 	`db:"a"`
	b 			string 	`db:"b"`
	updated_at  string  `db:"updated_at"`
}


func sqlExec() {
	//用户名：密码@tcp(地址:3306)/数据库
	//db, err := sql.Open("mysql", "hjd:Hejundong1998.@tcp(localhost:3306)/tencent_cloud?charset=utf8")
	db, err := sql.Open("mysql", "root:134676@tcp(localhost:3306)/tencent_cloud?charset=utf8")
	if err != nil {
		fmt.Println("connect failed",err)
	}
	_, err = db.Exec(makeCreateSql("src_a", "a","2") + `;`)
	if err != nil {
		fmt.Println(err)
	}
	//s := make([]string, 0)
	//s = append(s, "a", "b","c", "d")
	rows, err := db.Query(`select * from` + "`1`" + `;`)
	//fmt.Println("select * from \x601\x60;")
	if err != nil {
		fmt.Println("query failed",err)
	}
	for rows.Next(){
		var s info
		err = rows.Scan(&s.id, &s.a, &s.b, &s.updated_at)
		if err != nil {
			fmt.Errorf("scan failed",err)
		}
		fmt.Println(s)
	}
	rows.Close()


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






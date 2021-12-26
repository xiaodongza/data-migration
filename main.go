package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"encoding/csv"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/big"
	"net"
	"net/url"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

//type NullTime sql.NullTime

var collations = map[string]byte{
	"big5_chinese_ci":      1,
	"latin2_czech_cs":      2,
	"dec8_swedish_ci":      3,
	"cp850_general_ci":     4,
	"latin1_german1_ci":    5,
	"hp8_english_ci":       6,
	"koi8r_general_ci":     7,
	"latin1_swedish_ci":    8,
	"latin2_general_ci":    9,
	"swe7_swedish_ci":      10,
	"ascii_general_ci":     11,
	"ujis_japanese_ci":     12,
	"sjis_japanese_ci":     13,
	"cp1251_bulgarian_ci":  14,
	"latin1_danish_ci":     15,
	"hebrew_general_ci":    16,
	"tis620_thai_ci":       18,
	"euckr_korean_ci":      19,
	"latin7_estonian_cs":   20,
	"latin2_hungarian_ci":  21,
	"koi8u_general_ci":     22,
	"cp1251_ukrainian_ci":  23,
	"gb2312_chinese_ci":    24,
	"greek_general_ci":     25,
	"cp1250_general_ci":    26,
	"latin2_croatian_ci":   27,
	"gbk_chinese_ci":       28,
	"cp1257_lithuanian_ci": 29,
	"latin5_turkish_ci":    30,
	"latin1_german2_ci":    31,
	"armscii8_general_ci":  32,
	"utf8_general_ci":      33,
	"cp1250_czech_cs":      34,
	//"ucs2_general_ci":          35,
	"cp866_general_ci":    36,
	"keybcs2_general_ci":  37,
	"macce_general_ci":    38,
	"macroman_general_ci": 39,
	"cp852_general_ci":    40,
	"latin7_general_ci":   41,
	"latin7_general_cs":   42,
	"macce_bin":           43,
	"cp1250_croatian_ci":  44,
	"utf8mb4_general_ci":  45,
	"utf8mb4_bin":         46,
	"latin1_bin":          47,
	"latin1_general_ci":   48,
	"latin1_general_cs":   49,
	"cp1251_bin":          50,
	"cp1251_general_ci":   51,
	"cp1251_general_cs":   52,
	"macroman_bin":        53,
	//"utf16_general_ci":         54,
	//"utf16_bin":                55,
	//"utf16le_general_ci":       56,
	"cp1256_general_ci": 57,
	"cp1257_bin":        58,
	"cp1257_general_ci": 59,
	//"utf32_general_ci":         60,
	//"utf32_bin":                61,
	//"utf16le_bin":              62,
	"binary":          63,
	"armscii8_bin":    64,
	"ascii_bin":       65,
	"cp1250_bin":      66,
	"cp1256_bin":      67,
	"cp866_bin":       68,
	"dec8_bin":        69,
	"greek_bin":       70,
	"hebrew_bin":      71,
	"hp8_bin":         72,
	"keybcs2_bin":     73,
	"koi8r_bin":       74,
	"koi8u_bin":       75,
	"utf8_tolower_ci": 76,
	"latin2_bin":      77,
	"latin5_bin":      78,
	"latin7_bin":      79,
	"cp850_bin":       80,
	"cp852_bin":       81,
	"swe7_bin":        82,
	"utf8_bin":        83,
	"big5_bin":        84,
	"euckr_bin":       85,
	"gb2312_bin":      86,
	"gbk_bin":         87,
	"sjis_bin":        88,
	"tis620_bin":      89,
	//"ucs2_bin":                 90,
	"ujis_bin":            91,
	"geostd8_general_ci":  92,
	"geostd8_bin":         93,
	"latin1_spanish_ci":   94,
	"cp932_japanese_ci":   95,
	"cp932_bin":           96,
	"eucjpms_japanese_ci": 97,
	"eucjpms_bin":         98,
	"cp1250_polish_ci":    99,
	//"utf16_unicode_ci":         101,
	//"utf16_icelandic_ci":       102,
	//"utf16_latvian_ci":         103,
	//"utf16_romanian_ci":        104,
	//"utf16_slovenian_ci":       105,
	//"utf16_polish_ci":          106,
	//"utf16_estonian_ci":        107,
	//"utf16_spanish_ci":         108,
	//"utf16_swedish_ci":         109,
	//"utf16_turkish_ci":         110,
	//"utf16_czech_ci":           111,
	//"utf16_danish_ci":          112,
	//"utf16_lithuanian_ci":      113,
	//"utf16_slovak_ci":          114,
	//"utf16_spanish2_ci":        115,
	//"utf16_roman_ci":           116,
	//"utf16_persian_ci":         117,
	//"utf16_esperanto_ci":       118,
	//"utf16_hungarian_ci":       119,
	//"utf16_sinhala_ci":         120,
	//"utf16_german2_ci":         121,
	//"utf16_croatian_ci":        122,
	//"utf16_unicode_520_ci":     123,
	//"utf16_vietnamese_ci":      124,
	//"ucs2_unicode_ci":          128,
	//"ucs2_icelandic_ci":        129,
	//"ucs2_latvian_ci":          130,
	//"ucs2_romanian_ci":         131,
	//"ucs2_slovenian_ci":        132,
	//"ucs2_polish_ci":           133,
	//"ucs2_estonian_ci":         134,
	//"ucs2_spanish_ci":          135,
	//"ucs2_swedish_ci":          136,
	//"ucs2_turkish_ci":          137,
	//"ucs2_czech_ci":            138,
	//"ucs2_danish_ci":           139,
	//"ucs2_lithuanian_ci":       140,
	//"ucs2_slovak_ci":           141,
	//"ucs2_spanish2_ci":         142,
	//"ucs2_roman_ci":            143,
	//"ucs2_persian_ci":          144,
	//"ucs2_esperanto_ci":        145,
	//"ucs2_hungarian_ci":        146,
	//"ucs2_sinhala_ci":          147,
	//"ucs2_german2_ci":          148,
	//"ucs2_croatian_ci":         149,
	//"ucs2_unicode_520_ci":      150,
	//"ucs2_vietnamese_ci":       151,
	//"ucs2_general_mysql500_ci": 159,
	//"utf32_unicode_ci":         160,
	//"utf32_icelandic_ci":       161,
	//"utf32_latvian_ci":         162,
	//"utf32_romanian_ci":        163,
	//"utf32_slovenian_ci":       164,
	//"utf32_polish_ci":          165,
	//"utf32_estonian_ci":        166,
	//"utf32_spanish_ci":         167,
	//"utf32_swedish_ci":         168,
	//"utf32_turkish_ci":         169,
	//"utf32_czech_ci":           170,
	//"utf32_danish_ci":          171,
	//"utf32_lithuanian_ci":      172,
	//"utf32_slovak_ci":          173,
	//"utf32_spanish2_ci":        174,
	//"utf32_roman_ci":           175,
	//"utf32_persian_ci":         176,
	//"utf32_esperanto_ci":       177,
	//"utf32_hungarian_ci":       178,
	//"utf32_sinhala_ci":         179,
	//"utf32_german2_ci":         180,
	//"utf32_croatian_ci":        181,
	//"utf32_unicode_520_ci":     182,
	//"utf32_vietnamese_ci":      183,
	"utf8_unicode_ci":          192,
	"utf8_icelandic_ci":        193,
	"utf8_latvian_ci":          194,
	"utf8_romanian_ci":         195,
	"utf8_slovenian_ci":        196,
	"utf8_polish_ci":           197,
	"utf8_estonian_ci":         198,
	"utf8_spanish_ci":          199,
	"utf8_swedish_ci":          200,
	"utf8_turkish_ci":          201,
	"utf8_czech_ci":            202,
	"utf8_danish_ci":           203,
	"utf8_lithuanian_ci":       204,
	"utf8_slovak_ci":           205,
	"utf8_spanish2_ci":         206,
	"utf8_roman_ci":            207,
	"utf8_persian_ci":          208,
	"utf8_esperanto_ci":        209,
	"utf8_hungarian_ci":        210,
	"utf8_sinhala_ci":          211,
	"utf8_german2_ci":          212,
	"utf8_croatian_ci":         213,
	"utf8_unicode_520_ci":      214,
	"utf8_vietnamese_ci":       215,
	"utf8_general_mysql500_ci": 223,
	"utf8mb4_unicode_ci":       224,
	"utf8mb4_icelandic_ci":     225,
	"utf8mb4_latvian_ci":       226,
	"utf8mb4_romanian_ci":      227,
	"utf8mb4_slovenian_ci":     228,
	"utf8mb4_polish_ci":        229,
	"utf8mb4_estonian_ci":      230,
	"utf8mb4_spanish_ci":       231,
	"utf8mb4_swedish_ci":       232,
	"utf8mb4_turkish_ci":       233,
	"utf8mb4_czech_ci":         234,
	"utf8mb4_danish_ci":        235,
	"utf8mb4_lithuanian_ci":    236,
	"utf8mb4_slovak_ci":        237,
	"utf8mb4_spanish2_ci":      238,
	"utf8mb4_roman_ci":         239,
	"utf8mb4_persian_ci":       240,
	"utf8mb4_esperanto_ci":     241,
	"utf8mb4_hungarian_ci":     242,
	"utf8mb4_sinhala_ci":       243,
	"utf8mb4_german2_ci":       244,
	"utf8mb4_croatian_ci":      245,
	"utf8mb4_unicode_520_ci":   246,
	"utf8mb4_vietnamese_ci":    247,
	"gb18030_chinese_ci":       248,
	"gb18030_bin":              249,
	"gb18030_unicode_520_ci":   250,
	"utf8mb4_0900_ai_ci":       255,
}

var (
	serverPubKeyLock     sync.RWMutex
	serverPubKeyRegistry map[string]*rsa.PublicKey
)

var (
	scanTypeFloat32   = reflect.TypeOf(float32(0))
	scanTypeFloat64   = reflect.TypeOf(float64(0))
	scanTypeInt8      = reflect.TypeOf(int8(0))
	scanTypeInt16     = reflect.TypeOf(int16(0))
	scanTypeInt32     = reflect.TypeOf(int32(0))
	scanTypeInt64     = reflect.TypeOf(int64(0))
	scanTypeNullFloat = reflect.TypeOf(sql.NullFloat64{})
	scanTypeNullInt   = reflect.TypeOf(sql.NullInt64{})
	//scanTypeNullTime  = reflect.TypeOf(sql.NullTime{})
	scanTypeUint8    = reflect.TypeOf(uint8(0))
	scanTypeUint16   = reflect.TypeOf(uint16(0))
	scanTypeUint32   = reflect.TypeOf(uint32(0))
	scanTypeUint64   = reflect.TypeOf(uint64(0))
	scanTypeRawBytes = reflect.TypeOf(sql.RawBytes{})
	scanTypeUnknown  = reflect.TypeOf(new(interface{}))
)

const defaultBufSize = 4096
const maxCachedBufSize = 256 * 1024

type buffer struct {
	buf     []byte // buf is a byte buffer who's length and capacity are equal.
	nc      net.Conn
	idx     int
	length  int
	timeout time.Duration
	dbuf    [2][]byte // dbuf is an array with the two byte slices that back this buffer
	flipcnt uint      // flipccnt is the current buffer counter for double-buffering
}

var dataPath *string
var dstIP *string
var dstPort *int
var dstUser *string
var dstPassword *string

//  example of parameter parse, the final binary should be able to accept specified parameters as requested
//
//  usage example:
//      ./run --data_path /tmp/data --dst_ip 127.0.0.1 --dst_port 3306 --dst_user root --dst_password 123456789
//
//  you can test this example by:
//  go run main.go --data_path /tmp/data --dst_ip 127.0.0.1 --dst_port 3306 --dst_user root --dst_password 123456789
func main() {
	t := time.Now()
	dataPath = flag.String("data_path", "/tmp/data", "dir path of source data")
	dstIP = flag.String("dst_ip", "172.16.0.116", "ip of dst database address")
	dstPort = flag.Int("dst_port", 3306, "port of dst database address")
	dstUser = flag.String("dst_user", "test", "user name of dst database")
	dstPassword = flag.String("dst_password", "Henkxie1314#", "password of dst database")

	flag.Parse()
	fmt.Printf("data path:%v\n", *dataPath)
	fmt.Printf("dst ip:%v\n", *dstIP)
	fmt.Printf("dst port:%v\n", *dstPort)
	fmt.Printf("dst user:%v\n", *dstUser)
	fmt.Printf("dst password:%v\n", *dstPassword)
	//fmt.Println(*dataPath + "/"+ "src_a" + "/" + "a" + "/" + strconv.Itoa(1) + ".csv")
	handleData()
	elapsed := time.Since(t)
	fmt.Println("run time:", elapsed)
}

func createDatabase(database string) {
	//dstUser1 := *dstUser
	//dstPassword1 := "Henkxie1314#"
	//dstIP1 := *dstIP
	//dstPort1 := *dstPort
	//s := dstUser1 + ":" + dstPassword1 + "@tcp(" + dstIP1 + ":" + strconv.Itoa(dstPort1) + ")/?charset=utf8"
	//fmt.Println(s)
	//db, err := sql.Open("help", s)
	//db, err := sql.Open("help", "root:134676@tcp(localhost:3306)/")
	//db, err := sql.Open("help", "hjd:Hejundong1998.@tcp(182.254.128.133:138)/?charset=utf8")
	//db, err := sql.Open("help", "test:Henkxie1314#@tcp(172.16.0.116:3306)/?charset=utf8")
	//db, err := sql.Open("help", s)
	db, err := sql.Open("help", "hjd:AAAaaa1_@tcp(10.0.0.6:3306)/?charset=utf8")
	if err != nil {
		fmt.Println("connect failed", err)
	}
	_, err = db.Exec(makeDropDatabaseSql(database))
	if err != nil {
		fmt.Println("drop database failed", err)
	}
	_, err = db.Exec(makeCreateDatabaseSql(database))
	if err != nil {
		fmt.Println("create database failed", err)
	}
}

func handleData() {
	//wg := &sync.WaitGroup{}
	dbs := make([]string, 0)
	// dbs = append(dbs, "a", "b", "c", "d", "e", "f", "g")
	dbs = append(dbs, `a`, `b`, `c`, `d`, `e`, `f`, `g`)
	srcs := make([]string, 0)
	srcs = append(srcs, "src_a", "src_b")
	for _, db := range dbs {
		createDatabase(db)
	}
	for i := 1; i <= 4; i++ {
		for _, db := range dbs {
			_, _, uniqueColumnIndex, primaryColumnIndex, floatLine := HandleSql("src_a", db, strconv.Itoa(i))
			m1 := make(map[string]*[]string, 0)//unique
			m2 := make(map[string]*[]string, 0)//primary
			if len(uniqueColumnIndex) != 0 {
				index := uniqueColumnIndex[0]
				m := make(map[string]bool, 0)
				for _, src := range srcs {
					//csvFile, err := os.Open("F:\\data\\" + src + "\\" + db + "\\" + strconv.Itoa(i) + ".csv")
					csvFile, err := os.Open(*dataPath + "/" + src + "/" + db + "/" + strconv.Itoa(i) + ".csv")
					if err != nil {
						log.Fatalln("Couldn't open the csv file", err)
					}
					r := csv.NewReader(csvFile)
					for {
						record, err := r.Read()
						if err == io.EOF {
							break
						}
						if err != nil {
							log.Fatal(err)
						}
						unique := record[index]
						_, ok := m[unique]
						if ok {
							m1[unique] = nil
						} else {
							m[unique] = true
						}
					}
					csvFile.Close()
				}
			}
			if len(primaryColumnIndex) != 0 {
				index := primaryColumnIndex[0]
				m := make(map[string]bool, 0)
				for _, src := range srcs {
					//csvFile, err := os.Open("F:\\data\\" + src + "\\" + db + "\\" + strconv.Itoa(i) + ".csv")
					csvFile, err := os.Open(*dataPath + "/" + src + "/" + db + "/" + strconv.Itoa(i) + ".csv")
					if err != nil {
						log.Fatalln("Couldn't open the csv file", err)
					}
					r := csv.NewReader(csvFile)
					for {
						record, err := r.Read()
						if err == io.EOF {
							break
						}
						if err != nil {
							log.Fatal(err)
						}
						primary := record[index]
						_, ok := m[primary]
						if ok {
							m2[primary] = nil
						} else {
							m[primary] = true
						}
					}
					csvFile.Close()
				}
			}
			queue := make([]*[]string, 0)
			jail := make([][]string, 0)
			for _, src := range srcs {
				//csvFile, err := os.Open("F:\\data\\" + src + "\\" + db + "\\" + strconv.Itoa(i) + ".csv")
				csvFile, err := os.Open(*dataPath + "/" + src + "/" + db + "/" + strconv.Itoa(i) + ".csv")
				if err != nil {
					log.Fatalln("Couldn't open the csv file", err)
				}
				r := csv.NewReader(csvFile)
				for {
					record, err := r.Read()
					if err == io.EOF {
						break
					}
					if err != nil {
						log.Fatal(err)
					}
					for _, index := range uniqueColumnIndex {
						if value, ok := m1[record[index]]; ok {
							if value == nil {
								m1[record[uniqueColumnIndex[0]]] = &record
							} else {
								value1 := *value
								if value1[len(*value) - 1] < record[len(*value) - 1] {
									m1[record[index]] = &record
								}
							}
						} else {
							queue = append(queue, &record)
						}
					}
					if len(primaryColumnIndex) == 1 {
						index := primaryColumnIndex[0]
						if value, ok := m2[record[index]]; ok {
							if value == nil {
								m2[record[primaryColumnIndex[0]]] = &record
							} else {
								value1 := *value
								if value1[len(*value) - 1] < record[len(*value) - 1] {
									m2[record[index]] = &record
								}
							}
						} else {
							queue = append(queue, &record)
						}
					} else {
						index := primaryColumnIndex[0]
						if _, ok := m2[record[index]]; ok {
							jail = append(jail, record)
						} else {
							queue = append(queue, &record)
						}
					}
					//if len(queue) >= 10000 {
					//	wg.Add(1)
					//	go func(q []*[]string) {
					//		defer wg.Done()
					//		sqlExec(db, i, q)
					//	}(queue)
					//	queue = queue[:0]
					//}
				}
				csvFile.Close()
			}
			mapForJail := make(map[string]*[]string, 0)
			if len(jail) != 0 {
				for _, rec1 := range jail {
					if value, ok := mapForJail[rec1[primaryColumnIndex[0]]]; ok {
						value1 := *value
						if equals(rec1, value1, primaryColumnIndex, len(primaryColumnIndex), floatLine) {
							if value1[len(*value) - 1] < rec1[len(*value) - 1] {
								mapForJail[rec1[primaryColumnIndex[0]]] = &rec1
							}
						}
					} else {
						mapForJail[rec1[primaryColumnIndex[0]]] = &rec1
					}
				}
			}
			for _, v := range m1 {
				queue = append(queue, v)
			}
			for _, v := range mapForJail {
				queue = append(queue, v)
			}
			fmt.Println("start insert")
			sqlExec(db, i, queue)
			//wg.Add(1)
			//go func(q []*[]string) {
			//	defer wg.Done()
			//	sqlExec(db, i, q)
			//}(queue)
		}
	}
	//wg.Wait()
}

func equals(a, b []string, index []int, target int, floatLine int) bool {
	num := 0
	for _, i := range index {
		if i == floatLine {
			if transferFloatToDouble(a[i]) == transferFloatToDouble(b[i]) {
				num++
				if num == target {
					return true
				}
			}
		} else {
			if a[i] == b[i] {
				num++
				if num == target {
					return true
				}
			}
		}
	}
	return false
}

func sqlExec(database string, i int, queue []*[]string) {
	//dstUser1 := *dstUser
	//dstPassword1 := "Henkxie1314#"
	//dstIP1 := *dstIP
	//dstPort1 := *dstPort
	//db, err := sql.Open("help", dstUser1+":"+dstPassword1+"@tcp("+dstIP1+":"+strconv.Itoa(dstPort1)+")/?charset=utf8")
	//db, err := sql.Open("help", "root:134676@tcp(localhost:3306)/")
	//db, err := sql.Open("help", "hjd:Hejundong1998.@tcp(182.254.128.133:138)/" + database + "?charset=utf8")
	db, err := sql.Open("help", "hjd:AAAaaa1_@tcp(10.0.0.6:3306)/?charset=utf8")
	if err != nil {
		fmt.Println("connect failed", err)
	}
	//创建表
	_, err = db.Exec(makeUseDatabaseSql(database))
	if err != nil {
		fmt.Println("use database failed", err)
	}
	_, err = db.Exec(makeCreateTableSql("src_a", database, strconv.Itoa(i)))
	if err != nil {
		fmt.Println("create table failed", err)
	}
	sizeOfBathInsert := 10000
	num := 0
	wg := &sync.WaitGroup{}
	for len(queue) >= (num + 1) * sizeOfBathInsert {
		go func(q []*[]string, start int) {
			defer wg.Done()
			db.Exec(makeBatchInsertSql(strconv.Itoa(i), start + sizeOfBathInsert, q, start))
		}(queue, num * sizeOfBathInsert)
		num++
	}
	if len(queue) > 0 {
		db.Exec(makeBatchInsertSql(strconv.Itoa(i), len(queue), queue, num * sizeOfBathInsert))
		queue = queue[len(queue):]
	}
	//fmt.Println("insert a table success")
	wg.Wait()
	db.Close()
}

func makeBatchInsertSql(table_name string, r int, queue []*[]string, start int) string {
	var buffer bytes.Buffer
	buffer.WriteString("INSERT INTO `" + table_name + "` VALUES ")
	for i := start; i < start + r; i++ {
		buffer.WriteString("(")
		row := queue[i]
		row1 := *row
		for j, metaData := range row1 {
			if j != 0 {
				buffer.WriteString(",")
			}
			buffer.WriteString("\"")
			buffer.WriteString(metaData)
			buffer.WriteString("\"")
		}
		buffer.WriteString(")")
		if i != r-1 {
			buffer.WriteString(",")
		}
	}
	buffer.WriteString(";")
	//fmt.Println(sentence)
	return buffer.String()
}

func makeCreateTableSql(folder, database, table string) string {
	//file, err := os.Open("F:\\data\\" + folder + "\\" + database + "\\" + table + ".sql")
	file, err := os.Open(*dataPath + "/" + folder + "/" + database + "/" + table + ".sql")
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
	// fmt.Println(sentence)
	return sentence
}

func makeDropDatabaseSql(database string) string {
	sentence := ""
	sentence = sentence + "DROP DATABASE " + database + ";"
	// fmt.Println(sentence)
	return sentence
}

func makeCreateDatabaseSql(database string) string {
	sentence := ""
	sentence = sentence + "CREATE DATABASE " + database + ";"
	// fmt.Println(sentence)
	return sentence
}

func makeUseDatabaseSql(database string) string {
	sentence := ""
	sentence = sentence + "USE " + database + ";"
	// fmt.Println(sentence)
	return sentence
}

func HandleSql(folder, database, table string) (string, []string, []int, []int, int) {
	//file, err := os.Open("F:\\data\\" + folder + "\\" + database + "\\" + table + ".sql")
	file, err := os.Open(*dataPath + "/" + folder + "/" + database + "/" + table + ".sql")
	if err != nil {
		log.Printf("Cannot open sql file, err: [%v]", err)
	}
	defer file.Close()
	floatLine := -1
	var tableName string
	columnName := make([]string, 0)
	primaryColumnName := make([]string, 0)
	primaryColumnIndex := make([]int, 0)
	uniqueColumnIndex := make([]int, 0)

	scanner := bufio.NewScanner(file)
	numColumn := 0
	for scanner.Scan() {
		line := scanner.Text()
		if numColumn == 0 {
			tableName = getTableName(line)
			numColumn++
		} else if isColumn(line) {
			columnNameCur := getColumnName(line)
			columnName = append(columnName, columnNameCur)
			if isUnique(line) {
				uniqueColumnIndex = append(uniqueColumnIndex, numColumn)
			}
			if isPrimary(line) {
				primaryColumnIndex = append(primaryColumnIndex, numColumn)
			}
			if isFloat(line) {
				floatLine = numColumn
			}
			numColumn++
		} else if !isLastline(line) {
			getPrimaryName(&primaryColumnName, line)
		}
		fmt.Printf("%s\n", line)
	}
	if len(primaryColumnName) != 0 {
		for i := 0; i < len(primaryColumnName); i++ {
			for j := 0; j < len(columnName); j++ {
				if strings.Compare(primaryColumnName[i], columnName[j]) == 0 {
					primaryColumnIndex = append(primaryColumnIndex, j)
				}
			}
		}
	} else {
		for j := 0; j < len(columnName); j++ {
			if strings.Compare("updated_at", columnName[j]) != 0 {
				primaryColumnIndex = append(primaryColumnIndex, j)
			}
		}
	}

	//fmt.Println(table_name)
	//fmt.Println(column_name)
	//fmt.Println(unique_column_index)
	//fmt.Println(primary_column_index)
	return tableName, columnName, uniqueColumnIndex, primaryColumnIndex, floatLine
}

func transferFloatToDouble(d string) string {
	chuyi := -1
	for i := 0; i < len(d); i++ {
		if d[i] == '.' {
			chuyi = i
			break
		}
	}
	if chuyi == -1 {
		return d
	}
	if chuyi == 6 {
		return d[:6]
	}
	if len(d) <= 7 {
		return d
	}
	d = d[:chuyi] + d[chuyi + 1: 8]
	num, _ := strconv.ParseInt(d, 0,64)
	num += 5
	num = num/10
	s := strconv.FormatInt(int64(num), 10)
	s = s[:chuyi] + "." + s[chuyi:6]
	//fmt.Println(s)
	return s
}

func getTableName(former string) string {
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
	return former[start+1 : end]
}

func getColumnName(former string) string {
	num, pre := 0, 0
	for i := 0; i < len(former); i++ {
		if former[i] == '`' {
			num++
			if num%2 == 1 {
				pre = i
			}
			if num%2 == 0 {
				return former[pre+1 : i]
			}
		}
	}
	return ""
}

func getPrimaryName(ans *[]string, former string) {
	num, pre := 0, 0
	for i := 0; i < len(former); i++ {
		if former[i] == '`' {
			num++
			if num%2 == 1 {
				pre = i
			}
			if num%2 == 0 {
				*ans = append(*ans, former[pre+1:i])
			}
		}
	}
}

func isColumn(former string) bool {
	for i := 0; i < len(former); i++ {
		if former[i] == ' ' {
			continue
		} else if former[i] == '`' {
			return true
		} else {
			return false
		}
	}
	return false
}

func isLastline(former string) bool {
	if former[0] == '(' {
		return true
	}
	return false
}

func isUnique(former string) bool {
	if strings.Index(former, "UNIQUE") == -1 {
		return false
	}
	return true
}

func isFloat(former string) bool {
	if strings.Index(former, "float") == -1 {
		return false
	}
	return true
}

func isPrimary(former string) bool {
	if strings.Index(former, "PRIMARY") == -1 {
		return false
	}
	return true
}

//---------------------------------------------------------------------------------------------
//---------------------------------------------------------------------------------------------
//---------------------------------------------------------------------------------------------
//---------------------------------------------------------------------------------------------
//---------------------------------------------------------------------------------------------

//auth.go
func RegisterServerPubKey(name string, pubKey *rsa.PublicKey) {
	serverPubKeyLock.Lock()
	if serverPubKeyRegistry == nil {
		serverPubKeyRegistry = make(map[string]*rsa.PublicKey)
	}

	serverPubKeyRegistry[name] = pubKey
	serverPubKeyLock.Unlock()
}

// DeregisterServerPubKey removes the public key registered with the given name.
func DeregisterServerPubKey(name string) {
	serverPubKeyLock.Lock()
	if serverPubKeyRegistry != nil {
		delete(serverPubKeyRegistry, name)
	}
	serverPubKeyLock.Unlock()
}

func getServerPubKey(name string) (pubKey *rsa.PublicKey) {
	serverPubKeyLock.RLock()
	if v, ok := serverPubKeyRegistry[name]; ok {
		pubKey = v
	}
	serverPubKeyLock.RUnlock()
	return
}

// Hash password using pre 4.1 (old password) method
// https://github.com/atcurtis/mariadb/blob/master/mysys/my_rnd.c
type myRnd struct {
	seed1, seed2 uint32
}

const myRndMaxVal = 0x3FFFFFFF

// Pseudo random number generator
func newMyRnd(seed1, seed2 uint32) *myRnd {
	return &myRnd{
		seed1: seed1 % myRndMaxVal,
		seed2: seed2 % myRndMaxVal,
	}
}

func (r *myRnd) NextByte() byte {
	r.seed1 = (r.seed1*3 + r.seed2) % myRndMaxVal
	r.seed2 = (r.seed1 + r.seed2 + 33) % myRndMaxVal

	return byte(uint64(r.seed1) * 31 / myRndMaxVal)
}

// Generate binary hash from byte string using insecure pre 4.1 method
func pwHash(password []byte) (result [2]uint32) {
	var add uint32 = 7
	var tmp uint32

	result[0] = 1345345333
	result[1] = 0x12345671

	for _, c := range password {
		// skip spaces and tabs in password
		if c == ' ' || c == '\t' {
			continue
		}

		tmp = uint32(c)
		result[0] ^= (((result[0] & 63) + add) * tmp) + (result[0] << 8)
		result[1] += (result[1] << 8) ^ result[0]
		add += tmp
	}

	// Remove sign bit (1<<31)-1)
	result[0] &= 0x7FFFFFFF
	result[1] &= 0x7FFFFFFF

	return
}

// Hash password using insecure pre 4.1 method
func scrambleOldPassword(scramble []byte, password string) []byte {
	scramble = scramble[:8]

	hashPw := pwHash([]byte(password))
	hashSc := pwHash(scramble)

	r := newMyRnd(hashPw[0]^hashSc[0], hashPw[1]^hashSc[1])

	var out [8]byte
	for i := range out {
		out[i] = r.NextByte() + 64
	}

	mask := r.NextByte()
	for i := range out {
		out[i] ^= mask
	}

	return out[:]
}

// Hash password using 4.1+ method (SHA1)
func scramblePassword(scramble []byte, password string) []byte {
	if len(password) == 0 {
		return nil
	}

	// stage1Hash = SHA1(password)
	crypt := sha1.New()
	crypt.Write([]byte(password))
	stage1 := crypt.Sum(nil)

	// scrambleHash = SHA1(scramble + SHA1(stage1Hash))
	// inner Hash
	crypt.Reset()
	crypt.Write(stage1)
	hash := crypt.Sum(nil)

	// outer Hash
	crypt.Reset()
	crypt.Write(scramble)
	crypt.Write(hash)
	scramble = crypt.Sum(nil)

	// token = scrambleHash XOR stage1Hash
	for i := range scramble {
		scramble[i] ^= stage1[i]
	}
	return scramble
}

// Hash password using MySQL 8+ method (SHA256)
func scrambleSHA256Password(scramble []byte, password string) []byte {
	if len(password) == 0 {
		return nil
	}

	// XOR(SHA256(password), SHA256(SHA256(SHA256(password)), scramble))

	crypt := sha256.New()
	crypt.Write([]byte(password))
	message1 := crypt.Sum(nil)

	crypt.Reset()
	crypt.Write(message1)
	message1Hash := crypt.Sum(nil)

	crypt.Reset()
	crypt.Write(message1Hash)
	crypt.Write(scramble)
	message2 := crypt.Sum(nil)

	for i := range message1 {
		message1[i] ^= message2[i]
	}

	return message1
}

func encryptPassword(password string, seed []byte, pub *rsa.PublicKey) ([]byte, error) {
	plain := make([]byte, len(password)+1)
	copy(plain, password)
	for i := range plain {
		j := i % len(seed)
		plain[i] ^= seed[j]
	}
	sha1 := sha1.New()
	return rsa.EncryptOAEP(sha1, rand.Reader, pub, plain, nil)
}

func (mc *mysqlConn) sendEncryptedPassword(seed []byte, pub *rsa.PublicKey) error {
	enc, err := encryptPassword(mc.cfg.Passwd, seed, pub)
	if err != nil {
		return err
	}
	return mc.writeAuthSwitchPacket(enc)
}

func (mc *mysqlConn) auth(authData []byte, plugin string) ([]byte, error) {
	switch plugin {
	case "caching_sha2_password":
		authResp := scrambleSHA256Password(authData, mc.cfg.Passwd)
		return authResp, nil

	case "mysql_old_password":
		if !mc.cfg.AllowOldPasswords {
			return nil, ErrOldPassword
		}
		if len(mc.cfg.Passwd) == 0 {
			return nil, nil
		}
		// Note: there are edge cases where this should work but doesn't;
		// this is currently "wontfix":
		// https://github.com/go-sql-driver/mysql/issues/184
		authResp := append(scrambleOldPassword(authData[:8], mc.cfg.Passwd), 0)
		return authResp, nil

	case "mysql_clear_password":
		if !mc.cfg.AllowCleartextPasswords {
			return nil, ErrCleartextPassword
		}
		// http://dev.mysql.com/doc/refman/5.7/en/cleartext-authentication-plugin.html
		// http://dev.mysql.com/doc/refman/5.7/en/pam-authentication-plugin.html
		return append([]byte(mc.cfg.Passwd), 0), nil

	case "mysql_native_password":
		if !mc.cfg.AllowNativePasswords {
			return nil, ErrNativePassword
		}
		// https://dev.mysql.com/doc/internals/en/secure-password-authentication.html
		// Native password authentication only need and will need 20-byte challenge.
		authResp := scramblePassword(authData[:20], mc.cfg.Passwd)
		return authResp, nil

	case "sha256_password":
		if len(mc.cfg.Passwd) == 0 {
			return []byte{0}, nil
		}
		// unlike caching_sha2_password, sha256_password does not accept
		// cleartext password on unix transport.
		if mc.cfg.tls != nil {
			// write cleartext auth packet
			return append([]byte(mc.cfg.Passwd), 0), nil
		}

		pubKey := mc.cfg.pubKey
		if pubKey == nil {
			// request public key from server
			return []byte{1}, nil
		}

		// encrypted password
		enc, err := encryptPassword(mc.cfg.Passwd, authData, pubKey)
		return enc, err

	default:
		errLog.Print("unknown auth plugin:", plugin)
		return nil, ErrUnknownPlugin
	}
}

func (mc *mysqlConn) handleAuthResult(oldAuthData []byte, plugin string) error {
	// Read Result Packet
	authData, newPlugin, err := mc.readAuthResult()
	if err != nil {
		return err
	}

	// handle auth plugin switch, if requested
	if newPlugin != "" {
		// If CLIENT_PLUGIN_AUTH capability is not supported, no new cipher is
		// sent and we have to keep using the cipher sent in the init packet.
		if authData == nil {
			authData = oldAuthData
		} else {
			// copy data from read buffer to owned slice
			copy(oldAuthData, authData)
		}

		plugin = newPlugin

		authResp, err := mc.auth(authData, plugin)
		if err != nil {
			return err
		}
		if err = mc.writeAuthSwitchPacket(authResp); err != nil {
			return err
		}

		// Read Result Packet
		authData, newPlugin, err = mc.readAuthResult()
		if err != nil {
			return err
		}

		// Do not allow to change the auth plugin more than once
		if newPlugin != "" {
			return ErrMalformPkt
		}
	}

	switch plugin {

	// https://insidemysql.com/preparing-your-community-connector-for-mysql-8-part-2-sha256/
	case "caching_sha2_password":
		switch len(authData) {
		case 0:
			return nil // auth successful
		case 1:
			switch authData[0] {
			case cachingSha2PasswordFastAuthSuccess:
				if err = mc.readResultOK(); err == nil {
					return nil // auth successful
				}

			case cachingSha2PasswordPerformFullAuthentication:
				if mc.cfg.tls != nil || mc.cfg.Net == "unix" {
					// write cleartext auth packet
					err = mc.writeAuthSwitchPacket(append([]byte(mc.cfg.Passwd), 0))
					if err != nil {
						return err
					}
				} else {
					pubKey := mc.cfg.pubKey
					if pubKey == nil {
						// request public key from server
						data, err := mc.buf.takeSmallBuffer(4 + 1)
						if err != nil {
							return err
						}
						data[4] = cachingSha2PasswordRequestPublicKey
						mc.writePacket(data)

						// parse public key
						if data, err = mc.readPacket(); err != nil {
							return err
						}

						block, rest := pem.Decode(data[1:])
						if block == nil {
							return fmt.Errorf("No Pem data found, data: %s", rest)
						}
						pkix, err := x509.ParsePKIXPublicKey(block.Bytes)
						if err != nil {
							return err
						}
						pubKey = pkix.(*rsa.PublicKey)
					}

					// send encrypted password
					err = mc.sendEncryptedPassword(oldAuthData, pubKey)
					if err != nil {
						return err
					}
				}
				return mc.readResultOK()

			default:
				return ErrMalformPkt
			}
		default:
			return ErrMalformPkt
		}

	case "sha256_password":
		switch len(authData) {
		case 0:
			return nil // auth successful
		default:
			block, _ := pem.Decode(authData)
			pub, err := x509.ParsePKIXPublicKey(block.Bytes)
			if err != nil {
				return err
			}

			// send encrypted password
			err = mc.sendEncryptedPassword(oldAuthData, pub.(*rsa.PublicKey))
			if err != nil {
				return err
			}
			return mc.readResultOK()
		}

	default:
		return nil // auth successful
	}

	return err
}

//buffer.go
func newBuffer(nc net.Conn) buffer {
	fg := make([]byte, defaultBufSize)
	return buffer{
		buf:  fg,
		nc:   nc,
		dbuf: [2][]byte{fg, nil},
	}
}

// flip replaces the active buffer with the background buffer
// this is a delayed flip that simply increases the buffer counter;
// the actual flip will be performed the next time we call `buffer.fill`
func (b *buffer) flip() {
	b.flipcnt += 1
}

// fill reads into the buffer until at least _need_ bytes are in it
func (b *buffer) fill(need int) error {
	n := b.length
	// fill data into its double-buffering target: if we've called
	// flip on this buffer, we'll be copying to the background buffer,
	// and then filling it with network data; otherwise we'll just move
	// the contents of the current buffer to the front before filling it
	dest := b.dbuf[b.flipcnt&1]

	// grow buffer if necessary to fit the whole packet.
	if need > len(dest) {
		// Round up to the next multiple of the default size
		dest = make([]byte, ((need/defaultBufSize)+1)*defaultBufSize)

		// if the allocated buffer is not too large, move it to backing storage
		// to prevent extra allocations on applications that perform large reads
		if len(dest) <= maxCachedBufSize {
			b.dbuf[b.flipcnt&1] = dest
		}
	}

	// if we're filling the fg buffer, move the existing data to the start of it.
	// if we're filling the bg buffer, copy over the data
	if n > 0 {
		copy(dest[:n], b.buf[b.idx:])
	}

	b.buf = dest
	b.idx = 0

	for {
		if b.timeout > 0 {
			if err := b.nc.SetReadDeadline(time.Now().Add(b.timeout)); err != nil {
				return err
			}
		}

		nn, err := b.nc.Read(b.buf[n:])
		n += nn

		switch err {
		case nil:
			if n < need {
				continue
			}
			b.length = n
			return nil

		case io.EOF:
			if n >= need {
				b.length = n
				return nil
			}
			return io.ErrUnexpectedEOF

		default:
			return err
		}
	}
}

// returns next N bytes from buffer.
// The returned slice is only guaranteed to be valid until the next read
func (b *buffer) readNext(need int) ([]byte, error) {
	if b.length < need {
		// refill
		if err := b.fill(need); err != nil {
			return nil, err
		}
	}

	offset := b.idx
	b.idx += need
	b.length -= need
	return b.buf[offset:b.idx], nil
}

// takeBuffer returns a buffer with the requested size.
// If possible, a slice from the existing buffer is returned.
// Otherwise a bigger buffer is made.
// Only one buffer (total) can be used at a time.
func (b *buffer) takeBuffer(length int) ([]byte, error) {
	if b.length > 0 {
		return nil, ErrBusyBuffer
	}

	// test (cheap) general case first
	if length <= cap(b.buf) {
		return b.buf[:length], nil
	}

	if length < maxPacketSize {
		b.buf = make([]byte, length)
		return b.buf, nil
	}

	// buffer is larger than we want to store.
	return make([]byte, length), nil
}

// takeSmallBuffer is shortcut which can be used if length is
// known to be smaller than defaultBufSize.
// Only one buffer (total) can be used at a time.
func (b *buffer) takeSmallBuffer(length int) ([]byte, error) {
	if b.length > 0 {
		return nil, ErrBusyBuffer
	}
	return b.buf[:length], nil
}

// takeCompleteBuffer returns the complete existing buffer.
// This can be used if the necessary buffer size is unknown.
// cap and len of the returned buffer will be equal.
// Only one buffer (total) can be used at a time.
func (b *buffer) takeCompleteBuffer() ([]byte, error) {
	if b.length > 0 {
		return nil, ErrBusyBuffer
	}
	return b.buf, nil
}

// store stores buf, an updated buffer, if its suitable to do so.
func (b *buffer) store(buf []byte) error {
	if b.length > 0 {
		return ErrBusyBuffer
	} else if cap(buf) <= maxPacketSize && cap(buf) > cap(b.buf) {
		b.buf = buf[:cap(buf)]
	}
	return nil
}

//collations.go
const defaultCollation = "utf8mb4_general_ci"
const binaryCollation = "binary"

// A list of available collations mapped to the internal ID.
// To update this map use the following MySQL query:
//     SELECT COLLATION_NAME, ID FROM information_schema.COLLATIONS WHERE ID<256 ORDER BY ID
//
// Handshake packet have only 1 byte for collation_id.  So we can't use collations with ID > 255.
//
// ucs2, utf16, and utf32 can't be used for connection charset.
// https://dev.mysql.com/doc/refman/5.7/en/charset-connection.html#charset-connection-impermissible-client-charset
// They are commented out to reduce this map.

// A denylist of collations which is unsafe to interpolate parameters.
// These multibyte encodings may contains 0x5c (`\`) in their trailing bytes.
var unsafeCollations = map[string]bool{
	"big5_chinese_ci":        true,
	"sjis_japanese_ci":       true,
	"gbk_chinese_ci":         true,
	"big5_bin":               true,
	"gb2312_bin":             true,
	"gbk_bin":                true,
	"sjis_bin":               true,
	"cp932_japanese_ci":      true,
	"cp932_bin":              true,
	"gb18030_chinese_ci":     true,
	"gb18030_bin":            true,
	"gb18030_unicode_520_ci": true,
}

//conncheck.go
var errUnexpectedRead = errors.New("unexpected read from socket")

//

type mysqlConn struct {
	buf              buffer
	netConn          net.Conn
	rawConn          net.Conn // underlying connection when netConn is TLS connection.
	affectedRows     uint64
	insertId         uint64
	cfg              *Config
	maxAllowedPacket int
	maxWriteSize     int
	writeTimeout     time.Duration
	flags            clientFlag
	status           statusFlag
	sequence         uint8
	parseTime        bool
	reset            bool // set when the Go SQL package calls ResetSession

	// for context support (Go 1.8+)
	watching bool
	watcher  chan<- context.Context
	closech  chan struct{}
	finished chan<- struct{}
	canceled atomicError // set non-nil if conn is canceled
	closed   atomicBool  // set when conn is closed, before closech is closed
}

// Handles parameters set in DSN after the connection is established
func (mc *mysqlConn) handleParams() (err error) {
	var cmdSet strings.Builder
	for param, val := range mc.cfg.Params {
		switch param {
		// Charset: character_set_connection, character_set_client, character_set_results
		case "charset":
			charsets := strings.Split(val, ",")
			for i := range charsets {
				// ignore errors here - a charset may not exist
				err = mc.exec("SET NAMES " + charsets[i])
				if err == nil {
					break
				}
			}
			if err != nil {
				return
			}

		// Other system vars accumulated in a single SET command
		default:
			if cmdSet.Len() == 0 {
				// Heuristic: 29 chars for each other key=value to reduce reallocations
				cmdSet.Grow(4 + len(param) + 1 + len(val) + 30*(len(mc.cfg.Params)-1))
				cmdSet.WriteString("SET ")
			} else {
				cmdSet.WriteByte(',')
			}
			cmdSet.WriteString(param)
			cmdSet.WriteByte('=')
			cmdSet.WriteString(val)
		}
	}

	if cmdSet.Len() > 0 {
		err = mc.exec(cmdSet.String())
		if err != nil {
			return
		}
	}

	return
}

func (mc *mysqlConn) markBadConn(err error) error {
	if mc == nil {
		return err
	}
	if err != errBadConnNoWrite {
		return err
	}
	return driver.ErrBadConn
}

func (mc *mysqlConn) Begin() (driver.Tx, error) {
	return mc.begin(false)
}

func (mc *mysqlConn) begin(readOnly bool) (driver.Tx, error) {
	if mc.closed.IsSet() {
		errLog.Print(ErrInvalidConn)
		return nil, driver.ErrBadConn
	}
	var q string
	if readOnly {
		q = "START TRANSACTION READ ONLY"
	} else {
		q = "START TRANSACTION"
	}
	err := mc.exec(q)
	if err == nil {
		return &mysqlTx{mc}, err
	}
	return nil, mc.markBadConn(err)
}

func (mc *mysqlConn) Close() (err error) {
	// Makes Close idempotent
	if !mc.closed.IsSet() {
		err = mc.writeCommandPacket(comQuit)
	}

	mc.cleanup()

	return
}

// Closes the network connection and unsets internal variables. Do not call this
// function after successfully authentication, call Close instead. This function
// is called before auth or on auth failure because MySQL will have already
// closed the network connection.
func (mc *mysqlConn) cleanup() {
	if !mc.closed.TrySet(true) {
		return
	}

	// Makes cleanup idempotent
	close(mc.closech)
	if mc.netConn == nil {
		return
	}
	if err := mc.netConn.Close(); err != nil {
		errLog.Print(err)
	}
}

func (mc *mysqlConn) error() error {
	if mc.closed.IsSet() {
		if err := mc.canceled.Value(); err != nil {
			return err
		}
		return ErrInvalidConn
	}
	return nil
}

func (mc *mysqlConn) Prepare(query string) (driver.Stmt, error) {
	if mc.closed.IsSet() {
		errLog.Print(ErrInvalidConn)
		return nil, driver.ErrBadConn
	}
	// Send command
	err := mc.writeCommandPacketStr(comStmtPrepare, query)
	if err != nil {
		// STMT_PREPARE is safe to retry.  So we can return ErrBadConn here.
		errLog.Print(err)
		return nil, driver.ErrBadConn
	}

	stmt := &mysqlStmt{
		mc: mc,
	}

	// Read Result
	columnCount, err := stmt.readPrepareResultPacket()
	if err == nil {
		if stmt.paramCount > 0 {
			if err = mc.readUntilEOF(); err != nil {
				return nil, err
			}
		}

		if columnCount > 0 {
			err = mc.readUntilEOF()
		}
	}

	return stmt, err
}

func (mc *mysqlConn) interpolateParams(query string, args []driver.Value) (string, error) {
	// Number of ? should be same to len(args)
	if strings.Count(query, "?") != len(args) {
		return "", driver.ErrSkip
	}

	buf, err := mc.buf.takeCompleteBuffer()
	if err != nil {
		// can not take the buffer. Something must be wrong with the connection
		errLog.Print(err)
		return "", ErrInvalidConn
	}
	buf = buf[:0]
	argPos := 0

	for i := 0; i < len(query); i++ {
		q := strings.IndexByte(query[i:], '?')
		if q == -1 {
			buf = append(buf, query[i:]...)
			break
		}
		buf = append(buf, query[i:i+q]...)
		i += q

		arg := args[argPos]
		argPos++

		if arg == nil {
			buf = append(buf, "NULL"...)
			continue
		}

		switch v := arg.(type) {
		case int64:
			buf = strconv.AppendInt(buf, v, 10)
		case uint64:
			// Handle uint64 explicitly because our custom ConvertValue emits unsigned values
			buf = strconv.AppendUint(buf, v, 10)
		case float64:
			buf = strconv.AppendFloat(buf, v, 'g', -1, 64)
		case bool:
			if v {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case time.Time:
			if v.IsZero() {
				buf = append(buf, "'0000-00-00'"...)
			} else {
				buf = append(buf, '\'')
				buf, err = appendDateTime(buf, v.In(mc.cfg.Loc))
				if err != nil {
					return "", err
				}
				buf = append(buf, '\'')
			}
		case json.RawMessage:
			buf = append(buf, '\'')
			if mc.status&statusNoBackslashEscapes == 0 {
				buf = escapeBytesBackslash(buf, v)
			} else {
				buf = escapeBytesQuotes(buf, v)
			}
			buf = append(buf, '\'')
		case []byte:
			if v == nil {
				buf = append(buf, "NULL"...)
			} else {
				buf = append(buf, "_binary'"...)
				if mc.status&statusNoBackslashEscapes == 0 {
					buf = escapeBytesBackslash(buf, v)
				} else {
					buf = escapeBytesQuotes(buf, v)
				}
				buf = append(buf, '\'')
			}
		case string:
			buf = append(buf, '\'')
			if mc.status&statusNoBackslashEscapes == 0 {
				buf = escapeStringBackslash(buf, v)
			} else {
				buf = escapeStringQuotes(buf, v)
			}
			buf = append(buf, '\'')
		default:
			return "", driver.ErrSkip
		}

		if len(buf)+4 > mc.maxAllowedPacket {
			return "", driver.ErrSkip
		}
	}
	if argPos != len(args) {
		return "", driver.ErrSkip
	}
	return string(buf), nil
}

func (mc *mysqlConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	if mc.closed.IsSet() {
		errLog.Print(ErrInvalidConn)
		return nil, driver.ErrBadConn
	}
	if len(args) != 0 {
		if !mc.cfg.InterpolateParams {
			return nil, driver.ErrSkip
		}
		// try to interpolate the parameters to save extra roundtrips for preparing and closing a statement
		prepared, err := mc.interpolateParams(query, args)
		if err != nil {
			return nil, err
		}
		query = prepared
	}
	mc.affectedRows = 0
	mc.insertId = 0

	err := mc.exec(query)
	if err == nil {
		return &mysqlResult{
			affectedRows: int64(mc.affectedRows),
			insertId:     int64(mc.insertId),
		}, err
	}
	return nil, mc.markBadConn(err)
}

// Internal function to execute commands
func (mc *mysqlConn) exec(query string) error {
	// Send command
	if err := mc.writeCommandPacketStr(comQuery, query); err != nil {
		return mc.markBadConn(err)
	}

	// Read Result
	resLen, err := mc.readResultSetHeaderPacket()
	if err != nil {
		return err
	}

	if resLen > 0 {
		// columns
		if err := mc.readUntilEOF(); err != nil {
			return err
		}

		// rows
		if err := mc.readUntilEOF(); err != nil {
			return err
		}
	}

	return mc.discardResults()
}

func (mc *mysqlConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	return mc.query(query, args)
}

func (mc *mysqlConn) query(query string, args []driver.Value) (*textRows, error) {
	if mc.closed.IsSet() {
		errLog.Print(ErrInvalidConn)
		return nil, driver.ErrBadConn
	}
	if len(args) != 0 {
		if !mc.cfg.InterpolateParams {
			return nil, driver.ErrSkip
		}
		// try client-side prepare to reduce roundtrip
		prepared, err := mc.interpolateParams(query, args)
		if err != nil {
			return nil, err
		}
		query = prepared
	}
	// Send command
	err := mc.writeCommandPacketStr(comQuery, query)
	if err == nil {
		// Read Result
		var resLen int
		resLen, err = mc.readResultSetHeaderPacket()
		if err == nil {
			rows := new(textRows)
			rows.mc = mc

			if resLen == 0 {
				rows.rs.done = true

				switch err := rows.NextResultSet(); err {
				case nil, io.EOF:
					return rows, nil
				default:
					return nil, err
				}
			}

			// Columns
			rows.rs.columns, err = mc.readColumns(resLen)
			return rows, err
		}
	}
	return nil, mc.markBadConn(err)
}

// Gets the value of the given MySQL System Variable
// The returned byte slice is only valid until the next read
func (mc *mysqlConn) getSystemVar(name string) ([]byte, error) {
	// Send command
	if err := mc.writeCommandPacketStr(comQuery, "SELECT @@"+name); err != nil {
		return nil, err
	}

	// Read Result
	resLen, err := mc.readResultSetHeaderPacket()
	if err == nil {
		rows := new(textRows)
		rows.mc = mc
		rows.rs.columns = []mysqlField{{fieldType: fieldTypeVarChar}}

		if resLen > 0 {
			// Columns
			if err := mc.readUntilEOF(); err != nil {
				return nil, err
			}
		}

		dest := make([]driver.Value, resLen)
		if err = rows.readRow(dest); err == nil {
			return dest[0].([]byte), mc.readUntilEOF()
		}
	}
	return nil, err
}

// finish is called when the query has canceled.
func (mc *mysqlConn) cancel(err error) {
	mc.canceled.Set(err)
	mc.cleanup()
}

// finish is called when the query has succeeded.
func (mc *mysqlConn) finish() {
	if !mc.watching || mc.finished == nil {
		return
	}
	select {
	case mc.finished <- struct{}{}:
		mc.watching = false
	case <-mc.closech:
	}
}

// Ping implements driver.Pinger interface
func (mc *mysqlConn) Ping(ctx context.Context) (err error) {
	if mc.closed.IsSet() {
		errLog.Print(ErrInvalidConn)
		return driver.ErrBadConn
	}

	if err = mc.watchCancel(ctx); err != nil {
		return
	}
	defer mc.finish()

	if err = mc.writeCommandPacket(comPing); err != nil {
		return mc.markBadConn(err)
	}

	return mc.readResultOK()
}

// BeginTx implements driver.ConnBeginTx interface
func (mc *mysqlConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if mc.closed.IsSet() {
		return nil, driver.ErrBadConn
	}

	if err := mc.watchCancel(ctx); err != nil {
		return nil, err
	}
	defer mc.finish()

	if sql.IsolationLevel(opts.Isolation) != sql.LevelDefault {
		level, err := mapIsolationLevel(opts.Isolation)
		if err != nil {
			return nil, err
		}
		err = mc.exec("SET TRANSACTION ISOLATION LEVEL " + level)
		if err != nil {
			return nil, err
		}
	}

	return mc.begin(opts.ReadOnly)
}

func (mc *mysqlConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}

	if err := mc.watchCancel(ctx); err != nil {
		return nil, err
	}

	rows, err := mc.query(query, dargs)
	if err != nil {
		mc.finish()
		return nil, err
	}
	rows.finish = mc.finish
	return rows, err
}

func (mc *mysqlConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}

	if err := mc.watchCancel(ctx); err != nil {
		return nil, err
	}
	defer mc.finish()

	return mc.Exec(query, dargs)
}

func (mc *mysqlConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if err := mc.watchCancel(ctx); err != nil {
		return nil, err
	}

	stmt, err := mc.Prepare(query)
	mc.finish()
	if err != nil {
		return nil, err
	}

	select {
	default:
	case <-ctx.Done():
		stmt.Close()
		return nil, ctx.Err()
	}
	return stmt, nil
}

func (stmt *mysqlStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}

	if err := stmt.mc.watchCancel(ctx); err != nil {
		return nil, err
	}

	rows, err := stmt.query(dargs)
	if err != nil {
		stmt.mc.finish()
		return nil, err
	}
	rows.finish = stmt.mc.finish
	return rows, err
}

func (stmt *mysqlStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}

	if err := stmt.mc.watchCancel(ctx); err != nil {
		return nil, err
	}
	defer stmt.mc.finish()

	return stmt.Exec(dargs)
}

func (mc *mysqlConn) watchCancel(ctx context.Context) error {
	if mc.watching {
		// Reach here if canceled,
		// so the connection is already invalid
		mc.cleanup()
		return nil
	}
	// When ctx is already cancelled, don't watch it.
	if err := ctx.Err(); err != nil {
		return err
	}
	// When ctx is not cancellable, don't watch it.
	if ctx.Done() == nil {
		return nil
	}
	// When watcher is not alive, can't watch it.
	if mc.watcher == nil {
		return nil
	}

	mc.watching = true
	mc.watcher <- ctx
	return nil
}

func (mc *mysqlConn) startWatcher() {
	watcher := make(chan context.Context, 1)
	mc.watcher = watcher
	finished := make(chan struct{})
	mc.finished = finished
	go func() {
		for {
			var ctx context.Context
			select {
			case ctx = <-watcher:
			case <-mc.closech:
				return
			}

			select {
			case <-ctx.Done():
				mc.cancel(ctx.Err())
			case <-finished:
			case <-mc.closech:
				return
			}
		}
	}()
}

func (mc *mysqlConn) CheckNamedValue(nv *driver.NamedValue) (err error) {
	nv.Value, err = converter{}.ConvertValue(nv.Value)
	return
}

// ResetSession implements driver.SessionResetter.
// (From Go 1.10)
func (mc *mysqlConn) ResetSession(ctx context.Context) error {
	if mc.closed.IsSet() {
		return driver.ErrBadConn
	}
	mc.reset = true
	return nil
}

// IsValid implements driver.Validator interface
// (From Go 1.15)
func (mc *mysqlConn) IsValid() bool {
	return !mc.closed.IsSet()
}

//

type connector struct {
	cfg *Config // immutable private copy.
}

// Connect implements driver.Connector interface.
// Connect returns a connection to the database.
func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	var err error

	// New mysqlConn
	mc := &mysqlConn{
		maxAllowedPacket: maxPacketSize,
		maxWriteSize:     maxPacketSize - 1,
		closech:          make(chan struct{}),
		cfg:              c.cfg,
	}
	mc.parseTime = mc.cfg.ParseTime

	// Connect to Server
	dialsLock.RLock()
	dial, ok := dials[mc.cfg.Net]
	dialsLock.RUnlock()
	if ok {
		dctx := ctx
		if mc.cfg.Timeout > 0 {
			var cancel context.CancelFunc
			dctx, cancel = context.WithTimeout(ctx, c.cfg.Timeout)
			defer cancel()
		}
		mc.netConn, err = dial(dctx, mc.cfg.Addr)
	} else {
		nd := net.Dialer{Timeout: mc.cfg.Timeout}
		mc.netConn, err = nd.DialContext(ctx, mc.cfg.Net, mc.cfg.Addr)
	}

	if err != nil {
		return nil, err
	}

	// Enable TCP Keepalives on TCP connections
	if tc, ok := mc.netConn.(*net.TCPConn); ok {
		if err := tc.SetKeepAlive(true); err != nil {
			// Don't send COM_QUIT before handshake.
			mc.netConn.Close()
			mc.netConn = nil
			return nil, err
		}
	}

	// Call startWatcher for context support (From Go 1.8)
	mc.startWatcher()
	if err := mc.watchCancel(ctx); err != nil {
		mc.cleanup()
		return nil, err
	}
	defer mc.finish()

	mc.buf = newBuffer(mc.netConn)

	// Set I/O timeouts
	mc.buf.timeout = mc.cfg.ReadTimeout
	mc.writeTimeout = mc.cfg.WriteTimeout

	// Reading Handshake Initialization Packet
	authData, plugin, err := mc.readHandshakePacket()
	if err != nil {
		mc.cleanup()
		return nil, err
	}

	if plugin == "" {
		plugin = defaultAuthPlugin
	}

	// Send Client Authentication Packet
	authResp, err := mc.auth(authData, plugin)
	if err != nil {
		// try the default auth plugin, if using the requested plugin failed
		errLog.Print("could not use requested auth plugin '"+plugin+"': ", err.Error())
		plugin = defaultAuthPlugin
		authResp, err = mc.auth(authData, plugin)
		if err != nil {
			mc.cleanup()
			return nil, err
		}
	}
	if err = mc.writeHandshakeResponsePacket(authResp, plugin); err != nil {
		mc.cleanup()
		return nil, err
	}

	// Handle response to auth packet, switch methods if possible
	if err = mc.handleAuthResult(authData, plugin); err != nil {
		// Authentication failed and MySQL has already closed the connection
		// (https://dev.mysql.com/doc/internals/en/authentication-fails.html).
		// Do not send COM_QUIT, just cleanup and return the error.
		mc.cleanup()
		return nil, err
	}

	if mc.cfg.MaxAllowedPacket > 0 {
		mc.maxAllowedPacket = mc.cfg.MaxAllowedPacket
	} else {
		// Get max allowed packet size
		maxap, err := mc.getSystemVar("max_allowed_packet")
		if err != nil {
			mc.Close()
			return nil, err
		}
		mc.maxAllowedPacket = stringToInt(maxap) - 1
	}
	if mc.maxAllowedPacket < maxPacketSize {
		mc.maxWriteSize = mc.maxAllowedPacket
	}

	// Handle DSN Params
	err = mc.handleParams()
	if err != nil {
		mc.Close()
		return nil, err
	}

	return mc, nil
}

// Driver implements driver.Connector interface.
// Driver returns &MySQLDriver{}.
func (c *connector) Driver() driver.Driver {
	return &MySQLDriver{}
}

const (
	defaultAuthPlugin       = "mysql_native_password"
	defaultMaxAllowedPacket = 4 << 20 // 4 MiB
	minProtocolVersion      = 10
	maxPacketSize           = 1<<24 - 1
	timeFormat              = "2006-01-02 15:04:05.999999"
)

// MySQL constants documentation:
// http://dev.mysql.com/doc/internals/en/client-server-protocol.html

const (
	iOK           byte = 0x00
	iAuthMoreData byte = 0x01
	iLocalInFile  byte = 0xfb
	iEOF          byte = 0xfe
	iERR          byte = 0xff
)

// https://dev.mysql.com/doc/internals/en/capability-flags.html#packet-Protocol::CapabilityFlags
type clientFlag uint32

const (
	clientLongPassword clientFlag = 1 << iota
	clientFoundRows
	clientLongFlag
	clientConnectWithDB
	clientNoSchema
	clientCompress
	clientODBC
	clientLocalFiles
	clientIgnoreSpace
	clientProtocol41
	clientInteractive
	clientSSL
	clientIgnoreSIGPIPE
	clientTransactions
	clientReserved
	clientSecureConn
	clientMultiStatements
	clientMultiResults
	clientPSMultiResults
	clientPluginAuth
	clientConnectAttrs
	clientPluginAuthLenEncClientData
	clientCanHandleExpiredPasswords
	clientSessionTrack
	clientDeprecateEOF
)

const (
	comQuit byte = iota + 1
	comInitDB
	comQuery
	comFieldList
	comCreateDB
	comDropDB
	comRefresh
	comShutdown
	comStatistics
	comProcessInfo
	comConnect
	comProcessKill
	comDebug
	comPing
	comTime
	comDelayedInsert
	comChangeUser
	comBinlogDump
	comTableDump
	comConnectOut
	comRegisterSlave
	comStmtPrepare
	comStmtExecute
	comStmtSendLongData
	comStmtClose
	comStmtReset
	comSetOption
	comStmtFetch
)

// https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnType
type fieldType byte

const (
	fieldTypeDecimal fieldType = iota
	fieldTypeTiny
	fieldTypeShort
	fieldTypeLong
	fieldTypeFloat
	fieldTypeDouble
	fieldTypeNULL
	fieldTypeTimestamp
	fieldTypeLongLong
	fieldTypeInt24
	fieldTypeDate
	fieldTypeTime
	fieldTypeDateTime
	fieldTypeYear
	fieldTypeNewDate
	fieldTypeVarChar
	fieldTypeBit
)
const (
	fieldTypeJSON fieldType = iota + 0xf5
	fieldTypeNewDecimal
	fieldTypeEnum
	fieldTypeSet
	fieldTypeTinyBLOB
	fieldTypeMediumBLOB
	fieldTypeLongBLOB
	fieldTypeBLOB
	fieldTypeVarString
	fieldTypeString
	fieldTypeGeometry
)

type fieldFlag uint16

const (
	flagNotNULL fieldFlag = 1 << iota
	flagPriKey
	flagUniqueKey
	flagMultipleKey
	flagBLOB
	flagUnsigned
	flagZeroFill
	flagBinary
	flagEnum
	flagAutoIncrement
	flagTimestamp
	flagSet
	flagUnknown1
	flagUnknown2
	flagUnknown3
	flagUnknown4
)

// http://dev.mysql.com/doc/internals/en/status-flags.html
type statusFlag uint16

const (
	statusInTrans statusFlag = 1 << iota
	statusInAutocommit
	statusReserved // Not in documentation
	statusMoreResultsExists
	statusNoGoodIndexUsed
	statusNoIndexUsed
	statusCursorExists
	statusLastRowSent
	statusDbDropped
	statusNoBackslashEscapes
	statusMetadataChanged
	statusQueryWasSlow
	statusPsOutParams
	statusInTransReadonly
	statusSessionStateChanged
)

const (
	cachingSha2PasswordRequestPublicKey          = 2
	cachingSha2PasswordFastAuthSuccess           = 3
	cachingSha2PasswordPerformFullAuthentication = 4
)

type MySQLDriver struct{}

// DialFunc is a function which can be used to establish the network connection.
// Custom dial functions must be registered with RegisterDial
//
// Deprecated: users should register a DialContextFunc instead
type DialFunc func(addr string) (net.Conn, error)

// DialContextFunc is a function which can be used to establish the network connection.
// Custom dial functions must be registered with RegisterDialContext
type DialContextFunc func(ctx context.Context, addr string) (net.Conn, error)

var (
	dialsLock sync.RWMutex
	dials     map[string]DialContextFunc
)

// RegisterDialContext registers a custom dial function. It can then be used by the
// network address mynet(addr), where mynet is the registered new network.
// The current context for the connection and its address is passed to the dial function.
func RegisterDialContext(net string, dial DialContextFunc) {
	dialsLock.Lock()
	defer dialsLock.Unlock()
	if dials == nil {
		dials = make(map[string]DialContextFunc)
	}
	dials[net] = dial
}

// RegisterDial registers a custom dial function. It can then be used by the
// network address mynet(addr), where mynet is the registered new network.
// addr is passed as a parameter to the dial function.
//
// Deprecated: users should call RegisterDialContext instead
func RegisterDial(network string, dial DialFunc) {
	RegisterDialContext(network, func(_ context.Context, addr string) (net.Conn, error) {
		return dial(addr)
	})
}

// Open new Connection.
// See https://github.com/go-sql-driver/mysql#dsn-data-source-name for how
// the DSN string is formatted
func (d MySQLDriver) Open(dsn string) (driver.Conn, error) {
	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	c := &connector{
		cfg: cfg,
	}
	return c.Connect(context.Background())
}

func init() {
	sql.Register("help", &MySQLDriver{})
}

// NewConnector returns new driver.Connector.
func NewConnector(cfg *Config) (driver.Connector, error) {
	cfg = cfg.Clone()
	// normalize the contents of cfg so calls to NewConnector have the same
	// behavior as MySQLDriver.OpenConnector
	if err := cfg.normalize(); err != nil {
		return nil, err
	}
	return &connector{cfg: cfg}, nil
}

// OpenConnector implements driver.DriverContext.
func (d MySQLDriver) OpenConnector(dsn string) (driver.Connector, error) {
	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	return &connector{
		cfg: cfg,
	}, nil
}

var (
	errInvalidDSNUnescaped       = errors.New("invalid DSN: did you forget to escape a param value?")
	errInvalidDSNAddr            = errors.New("invalid DSN: network address not terminated (missing closing brace)")
	errInvalidDSNNoSlash         = errors.New("invalid DSN: missing the slash separating the database name")
	errInvalidDSNUnsafeCollation = errors.New("invalid DSN: interpolateParams can not be used with unsafe collations")
)

// Config is a configuration parsed from a DSN string.
// If a new Config is created instead of being parsed from a DSN string,
// the NewConfig function should be used, which sets default values.
type Config struct {
	User             string            // Username
	Passwd           string            // Password (requires User)
	Net              string            // Network type
	Addr             string            // Network address (requires Net)
	DBName           string            // Database name
	Params           map[string]string // Connection parameters
	Collation        string            // Connection collation
	Loc              *time.Location    // Location for time.Time values
	MaxAllowedPacket int               // Max packet size allowed
	ServerPubKey     string            // Server public key name
	pubKey           *rsa.PublicKey    // Server public key
	TLSConfig        string            // TLS configuration name
	tls              *tls.Config       // TLS configuration
	Timeout          time.Duration     // Dial timeout
	ReadTimeout      time.Duration     // I/O read timeout
	WriteTimeout     time.Duration     // I/O write timeout

	AllowAllFiles           bool // Allow all files to be used with LOAD DATA LOCAL INFILE
	AllowCleartextPasswords bool // Allows the cleartext client side plugin
	AllowNativePasswords    bool // Allows the native password authentication method
	AllowOldPasswords       bool // Allows the old insecure password method
	CheckConnLiveness       bool // Check connections for liveness before using them
	ClientFoundRows         bool // Return number of matching rows instead of rows changed
	ColumnsWithAlias        bool // Prepend table alias to column names
	InterpolateParams       bool // Interpolate placeholders into query string
	MultiStatements         bool // Allow multiple statements in one query
	ParseTime               bool // Parse time values to time.Time
	RejectReadOnly          bool // Reject read-only connections
}

// NewConfig creates a new Config and sets default values.
func NewConfig() *Config {
	return &Config{
		Collation:            defaultCollation,
		Loc:                  time.UTC,
		MaxAllowedPacket:     defaultMaxAllowedPacket,
		AllowNativePasswords: true,
		CheckConnLiveness:    true,
	}
}

func (cfg *Config) Clone() *Config {
	cp := *cfg
	if cp.tls != nil {
		cp.tls = cfg.tls.Clone()
	}
	if len(cp.Params) > 0 {
		cp.Params = make(map[string]string, len(cfg.Params))
		for k, v := range cfg.Params {
			cp.Params[k] = v
		}
	}
	if cfg.pubKey != nil {
		cp.pubKey = &rsa.PublicKey{
			N: new(big.Int).Set(cfg.pubKey.N),
			E: cfg.pubKey.E,
		}
	}
	return &cp
}

func (cfg *Config) normalize() error {
	if cfg.InterpolateParams && unsafeCollations[cfg.Collation] {
		return errInvalidDSNUnsafeCollation
	}

	// Set default network if empty
	if cfg.Net == "" {
		cfg.Net = "tcp"
	}

	// Set default address if empty
	if cfg.Addr == "" {
		switch cfg.Net {
		case "tcp":
			cfg.Addr = "127.0.0.1:3306"
		case "unix":
			cfg.Addr = "/tmp/help.sock"
		default:
			return errors.New("default addr for network '" + cfg.Net + "' unknown")
		}
	} else if cfg.Net == "tcp" {
		cfg.Addr = ensureHavePort(cfg.Addr)
	}

	switch cfg.TLSConfig {
	case "false", "":
		// don't set anything
	case "true":
		cfg.tls = &tls.Config{}
	case "skip-verify", "preferred":
		cfg.tls = &tls.Config{InsecureSkipVerify: true}
	default:
		cfg.tls = getTLSConfigClone(cfg.TLSConfig)
		if cfg.tls == nil {
			return errors.New("invalid value / unknown config name: " + cfg.TLSConfig)
		}
	}

	if cfg.tls != nil && cfg.tls.ServerName == "" && !cfg.tls.InsecureSkipVerify {
		host, _, err := net.SplitHostPort(cfg.Addr)
		if err == nil {
			cfg.tls.ServerName = host
		}
	}

	if cfg.ServerPubKey != "" {
		cfg.pubKey = getServerPubKey(cfg.ServerPubKey)
		if cfg.pubKey == nil {
			return errors.New("invalid value / unknown server pub key name: " + cfg.ServerPubKey)
		}
	}

	return nil
}

func writeDSNParam(buf *bytes.Buffer, hasParam *bool, name, value string) {
	buf.Grow(1 + len(name) + 1 + len(value))
	if !*hasParam {
		*hasParam = true
		buf.WriteByte('?')
	} else {
		buf.WriteByte('&')
	}
	buf.WriteString(name)
	buf.WriteByte('=')
	buf.WriteString(value)
}

// FormatDSN formats the given Config into a DSN string which can be passed to
// the driver.
func (cfg *Config) FormatDSN() string {
	var buf bytes.Buffer

	// [username[:password]@]
	if len(cfg.User) > 0 {
		buf.WriteString(cfg.User)
		if len(cfg.Passwd) > 0 {
			buf.WriteByte(':')
			buf.WriteString(cfg.Passwd)
		}
		buf.WriteByte('@')
	}

	// [protocol[(address)]]
	if len(cfg.Net) > 0 {
		buf.WriteString(cfg.Net)
		if len(cfg.Addr) > 0 {
			buf.WriteByte('(')
			buf.WriteString(cfg.Addr)
			buf.WriteByte(')')
		}
	}

	// /dbname
	buf.WriteByte('/')
	buf.WriteString(cfg.DBName)

	// [?param1=value1&...&paramN=valueN]
	hasParam := false

	if cfg.AllowAllFiles {
		hasParam = true
		buf.WriteString("?allowAllFiles=true")
	}

	if cfg.AllowCleartextPasswords {
		writeDSNParam(&buf, &hasParam, "allowCleartextPasswords", "true")
	}

	if !cfg.AllowNativePasswords {
		writeDSNParam(&buf, &hasParam, "allowNativePasswords", "false")
	}

	if cfg.AllowOldPasswords {
		writeDSNParam(&buf, &hasParam, "allowOldPasswords", "true")
	}

	if !cfg.CheckConnLiveness {
		writeDSNParam(&buf, &hasParam, "checkConnLiveness", "false")
	}

	if cfg.ClientFoundRows {
		writeDSNParam(&buf, &hasParam, "clientFoundRows", "true")
	}

	if col := cfg.Collation; col != defaultCollation && len(col) > 0 {
		writeDSNParam(&buf, &hasParam, "collation", col)
	}

	if cfg.ColumnsWithAlias {
		writeDSNParam(&buf, &hasParam, "columnsWithAlias", "true")
	}

	if cfg.InterpolateParams {
		writeDSNParam(&buf, &hasParam, "interpolateParams", "true")
	}

	if cfg.Loc != time.UTC && cfg.Loc != nil {
		writeDSNParam(&buf, &hasParam, "loc", url.QueryEscape(cfg.Loc.String()))
	}

	if cfg.MultiStatements {
		writeDSNParam(&buf, &hasParam, "multiStatements", "true")
	}

	if cfg.ParseTime {
		writeDSNParam(&buf, &hasParam, "parseTime", "true")
	}

	if cfg.ReadTimeout > 0 {
		writeDSNParam(&buf, &hasParam, "readTimeout", cfg.ReadTimeout.String())
	}

	if cfg.RejectReadOnly {
		writeDSNParam(&buf, &hasParam, "rejectReadOnly", "true")
	}

	if len(cfg.ServerPubKey) > 0 {
		writeDSNParam(&buf, &hasParam, "serverPubKey", url.QueryEscape(cfg.ServerPubKey))
	}

	if cfg.Timeout > 0 {
		writeDSNParam(&buf, &hasParam, "timeout", cfg.Timeout.String())
	}

	if len(cfg.TLSConfig) > 0 {
		writeDSNParam(&buf, &hasParam, "tls", url.QueryEscape(cfg.TLSConfig))
	}

	if cfg.WriteTimeout > 0 {
		writeDSNParam(&buf, &hasParam, "writeTimeout", cfg.WriteTimeout.String())
	}

	if cfg.MaxAllowedPacket != defaultMaxAllowedPacket {
		writeDSNParam(&buf, &hasParam, "maxAllowedPacket", strconv.Itoa(cfg.MaxAllowedPacket))
	}

	// other params
	if cfg.Params != nil {
		var params []string
		for param := range cfg.Params {
			params = append(params, param)
		}
		sort.Strings(params)
		for _, param := range params {
			writeDSNParam(&buf, &hasParam, param, url.QueryEscape(cfg.Params[param]))
		}
	}

	return buf.String()
}

// ParseDSN parses the DSN string to a Config
func ParseDSN(dsn string) (cfg *Config, err error) {
	// New config with some default values
	cfg = NewConfig()

	// [user[:password]@][net[(addr)]]/dbname[?param1=value1&paramN=valueN]
	// Find the last '/' (since the password or the net addr might contain a '/')
	foundSlash := false
	for i := len(dsn) - 1; i >= 0; i-- {
		if dsn[i] == '/' {
			foundSlash = true
			var j, k int

			// left part is empty if i <= 0
			if i > 0 {
				// [username[:password]@][protocol[(address)]]
				// Find the last '@' in dsn[:i]
				for j = i; j >= 0; j-- {
					if dsn[j] == '@' {
						// username[:password]
						// Find the first ':' in dsn[:j]
						for k = 0; k < j; k++ {
							if dsn[k] == ':' {
								cfg.Passwd = dsn[k+1 : j]
								break
							}
						}
						cfg.User = dsn[:k]

						break
					}
				}

				// [protocol[(address)]]
				// Find the first '(' in dsn[j+1:i]
				for k = j + 1; k < i; k++ {
					if dsn[k] == '(' {
						// dsn[i-1] must be == ')' if an address is specified
						if dsn[i-1] != ')' {
							if strings.ContainsRune(dsn[k+1:i], ')') {
								return nil, errInvalidDSNUnescaped
							}
							return nil, errInvalidDSNAddr
						}
						cfg.Addr = dsn[k+1 : i-1]
						break
					}
				}
				cfg.Net = dsn[j+1 : k]
			}

			// dbname[?param1=value1&...&paramN=valueN]
			// Find the first '?' in dsn[i+1:]
			for j = i + 1; j < len(dsn); j++ {
				if dsn[j] == '?' {
					if err = parseDSNParams(cfg, dsn[j+1:]); err != nil {
						return
					}
					break
				}
			}
			cfg.DBName = dsn[i+1 : j]

			break
		}
	}

	if !foundSlash && len(dsn) > 0 {
		return nil, errInvalidDSNNoSlash
	}

	if err = cfg.normalize(); err != nil {
		return nil, err
	}
	return
}

// parseDSNParams parses the DSN "query string"
// Values must be url.QueryEscape'ed
func parseDSNParams(cfg *Config, params string) (err error) {
	for _, v := range strings.Split(params, "&") {
		param := strings.SplitN(v, "=", 2)
		if len(param) != 2 {
			continue
		}

		// cfg params
		switch value := param[1]; param[0] {
		// Disable INFILE allowlist / enable all files
		case "allowAllFiles":
			var isBool bool
			cfg.AllowAllFiles, isBool = readBool(value)
			if !isBool {
				return errors.New("invalid bool value: " + value)
			}

		// Use cleartext authentication mode (MySQL 5.5.10+)
		case "allowCleartextPasswords":
			var isBool bool
			cfg.AllowCleartextPasswords, isBool = readBool(value)
			if !isBool {
				return errors.New("invalid bool value: " + value)
			}

		// Use native password authentication
		case "allowNativePasswords":
			var isBool bool
			cfg.AllowNativePasswords, isBool = readBool(value)
			if !isBool {
				return errors.New("invalid bool value: " + value)
			}

		// Use old authentication mode (pre MySQL 4.1)
		case "allowOldPasswords":
			var isBool bool
			cfg.AllowOldPasswords, isBool = readBool(value)
			if !isBool {
				return errors.New("invalid bool value: " + value)
			}

		// Check connections for Liveness before using them
		case "checkConnLiveness":
			var isBool bool
			cfg.CheckConnLiveness, isBool = readBool(value)
			if !isBool {
				return errors.New("invalid bool value: " + value)
			}

		// Switch "rowsAffected" mode
		case "clientFoundRows":
			var isBool bool
			cfg.ClientFoundRows, isBool = readBool(value)
			if !isBool {
				return errors.New("invalid bool value: " + value)
			}

		// Collation
		case "collation":
			cfg.Collation = value

		case "columnsWithAlias":
			var isBool bool
			cfg.ColumnsWithAlias, isBool = readBool(value)
			if !isBool {
				return errors.New("invalid bool value: " + value)
			}

		// Compression
		case "compress":
			return errors.New("compression not implemented yet")

		// Enable client side placeholder substitution
		case "interpolateParams":
			var isBool bool
			cfg.InterpolateParams, isBool = readBool(value)
			if !isBool {
				return errors.New("invalid bool value: " + value)
			}

		// Time Location
		case "loc":
			if value, err = url.QueryUnescape(value); err != nil {
				return
			}
			cfg.Loc, err = time.LoadLocation(value)
			if err != nil {
				return
			}

		// multiple statements in one query
		case "multiStatements":
			var isBool bool
			cfg.MultiStatements, isBool = readBool(value)
			if !isBool {
				return errors.New("invalid bool value: " + value)
			}

		// time.Time parsing
		case "parseTime":
			var isBool bool
			cfg.ParseTime, isBool = readBool(value)
			if !isBool {
				return errors.New("invalid bool value: " + value)
			}

		// I/O read Timeout
		case "readTimeout":
			cfg.ReadTimeout, err = time.ParseDuration(value)
			if err != nil {
				return
			}

		// Reject read-only connections
		case "rejectReadOnly":
			var isBool bool
			cfg.RejectReadOnly, isBool = readBool(value)
			if !isBool {
				return errors.New("invalid bool value: " + value)
			}

		// Server public key
		case "serverPubKey":
			name, err := url.QueryUnescape(value)
			if err != nil {
				return fmt.Errorf("invalid value for server pub key name: %v", err)
			}
			cfg.ServerPubKey = name

		// Strict mode
		case "strict":
			panic("strict mode has been removed. See https://github.com/go-sql-driver/help/wiki/strict-mode")

		// Dial Timeout
		case "timeout":
			cfg.Timeout, err = time.ParseDuration(value)
			if err != nil {
				return
			}

		// TLS-Encryption
		case "tls":
			boolValue, isBool := readBool(value)
			if isBool {
				if boolValue {
					cfg.TLSConfig = "true"
				} else {
					cfg.TLSConfig = "false"
				}
			} else if vl := strings.ToLower(value); vl == "skip-verify" || vl == "preferred" {
				cfg.TLSConfig = vl
			} else {
				name, err := url.QueryUnescape(value)
				if err != nil {
					return fmt.Errorf("invalid value for TLS config name: %v", err)
				}
				cfg.TLSConfig = name
			}

		// I/O write Timeout
		case "writeTimeout":
			cfg.WriteTimeout, err = time.ParseDuration(value)
			if err != nil {
				return
			}
		case "maxAllowedPacket":
			cfg.MaxAllowedPacket, err = strconv.Atoi(value)
			if err != nil {
				return
			}
		default:
			// lazy init
			if cfg.Params == nil {
				cfg.Params = make(map[string]string)
			}

			if cfg.Params[param[0]], err = url.QueryUnescape(value); err != nil {
				return
			}
		}
	}

	return
}

func ensureHavePort(addr string) string {
	if _, _, err := net.SplitHostPort(addr); err != nil {
		return net.JoinHostPort(addr, "3306")
	}
	return addr
}

// Various errors the driver might return. Can change between driver versions.
var (
	ErrInvalidConn       = errors.New("invalid connection")
	ErrMalformPkt        = errors.New("malformed packet")
	ErrNoTLS             = errors.New("TLS requested but server does not support TLS")
	ErrCleartextPassword = errors.New("this user requires clear text authentication. If you still want to use it, please add 'allowCleartextPasswords=1' to your DSN")
	ErrNativePassword    = errors.New("this user requires help native password authentication.")
	ErrOldPassword       = errors.New("this user requires old password authentication. If you still want to use it, please add 'allowOldPasswords=1' to your DSN. See also https://github.com/go-sql-driver/help/wiki/old_passwords")
	ErrUnknownPlugin     = errors.New("this authentication plugin is not supported")
	ErrOldProtocol       = errors.New("MySQL server does not support required protocol 41+")
	ErrPktSync           = errors.New("commands out of sync. You can't run this command now")
	ErrPktSyncMul        = errors.New("commands out of sync. Did you run multiple statements at once?")
	ErrPktTooLarge       = errors.New("packet for query is too large. Try adjusting the 'max_allowed_packet' variable on the server")
	ErrBusyBuffer        = errors.New("busy buffer")

	// errBadConnNoWrite is used for connection errors where nothing was sent to the database yet.
	// If this happens first in a function starting a database interaction, it should be replaced by driver.ErrBadConn
	// to trigger a resend.
	// See https://github.com/go-sql-driver/mysql/pull/302
	errBadConnNoWrite = errors.New("bad connection")
)

var errLog = Logger(log.New(os.Stderr, "[help] ", log.Ldate|log.Ltime|log.Lshortfile))

// Logger is used to log critical error messages.
type Logger interface {
	Print(v ...interface{})
}

// SetLogger is used to set the logger for critical errors.
// The initial logger is os.Stderr.
func SetLogger(logger Logger) error {
	if logger == nil {
		return errors.New("logger is nil")
	}
	errLog = logger
	return nil
}

// MySQLError is an error type which represents a single MySQL error
type MySQLError struct {
	Number  uint16
	Message string
}

func (me *MySQLError) Error() string {
	return fmt.Sprintf("Error %d: %s", me.Number, me.Message)
}

func (me *MySQLError) Is(err error) bool {
	if merr, ok := err.(*MySQLError); ok {
		return merr.Number == me.Number
	}
	return false
}

func (mf *mysqlField) typeDatabaseName() string {
	switch mf.fieldType {
	case fieldTypeBit:
		return "BIT"
	case fieldTypeBLOB:
		if mf.charSet != collations[binaryCollation] {
			return "TEXT"
		}
		return "BLOB"
	case fieldTypeDate:
		return "DATE"
	case fieldTypeDateTime:
		return "DATETIME"
	case fieldTypeDecimal:
		return "DECIMAL"
	case fieldTypeDouble:
		return "DOUBLE"
	case fieldTypeEnum:
		return "ENUM"
	case fieldTypeFloat:
		return "FLOAT"
	case fieldTypeGeometry:
		return "GEOMETRY"
	case fieldTypeInt24:
		return "MEDIUMINT"
	case fieldTypeJSON:
		return "JSON"
	case fieldTypeLong:
		if mf.flags&flagUnsigned != 0 {
			return "UNSIGNED INT"
		}
		return "INT"
	case fieldTypeLongBLOB:
		if mf.charSet != collations[binaryCollation] {
			return "LONGTEXT"
		}
		return "LONGBLOB"
	case fieldTypeLongLong:
		if mf.flags&flagUnsigned != 0 {
			return "UNSIGNED BIGINT"
		}
		return "BIGINT"
	case fieldTypeMediumBLOB:
		if mf.charSet != collations[binaryCollation] {
			return "MEDIUMTEXT"
		}
		return "MEDIUMBLOB"
	case fieldTypeNewDate:
		return "DATE"
	case fieldTypeNewDecimal:
		return "DECIMAL"
	case fieldTypeNULL:
		return "NULL"
	case fieldTypeSet:
		return "SET"
	case fieldTypeShort:
		if mf.flags&flagUnsigned != 0 {
			return "UNSIGNED SMALLINT"
		}
		return "SMALLINT"
	case fieldTypeString:
		if mf.charSet == collations[binaryCollation] {
			return "BINARY"
		}
		return "CHAR"
	case fieldTypeTime:
		return "TIME"
	case fieldTypeTimestamp:
		return "TIMESTAMP"
	case fieldTypeTiny:
		if mf.flags&flagUnsigned != 0 {
			return "UNSIGNED TINYINT"
		}
		return "TINYINT"
	case fieldTypeTinyBLOB:
		if mf.charSet != collations[binaryCollation] {
			return "TINYTEXT"
		}
		return "TINYBLOB"
	case fieldTypeVarChar:
		if mf.charSet == collations[binaryCollation] {
			return "VARBINARY"
		}
		return "VARCHAR"
	case fieldTypeVarString:
		if mf.charSet == collations[binaryCollation] {
			return "VARBINARY"
		}
		return "VARCHAR"
	case fieldTypeYear:
		return "YEAR"
	default:
		return ""
	}
}

type mysqlField struct {
	tableName string
	name      string
	length    uint32
	flags     fieldFlag
	fieldType fieldType
	decimals  byte
	charSet   uint8
}

func (mf *mysqlField) scanType() reflect.Type {
	switch mf.fieldType {
	case fieldTypeTiny:
		if mf.flags&flagNotNULL != 0 {
			if mf.flags&flagUnsigned != 0 {
				return scanTypeUint8
			}
			return scanTypeInt8
		}
		return scanTypeNullInt

	case fieldTypeShort, fieldTypeYear:
		if mf.flags&flagNotNULL != 0 {
			if mf.flags&flagUnsigned != 0 {
				return scanTypeUint16
			}
			return scanTypeInt16
		}
		return scanTypeNullInt

	case fieldTypeInt24, fieldTypeLong:
		if mf.flags&flagNotNULL != 0 {
			if mf.flags&flagUnsigned != 0 {
				return scanTypeUint32
			}
			return scanTypeInt32
		}
		return scanTypeNullInt

	case fieldTypeLongLong:
		if mf.flags&flagNotNULL != 0 {
			if mf.flags&flagUnsigned != 0 {
				return scanTypeUint64
			}
			return scanTypeInt64
		}
		return scanTypeNullInt

	case fieldTypeFloat:
		if mf.flags&flagNotNULL != 0 {
			return scanTypeFloat32
		}
		return scanTypeNullFloat

	case fieldTypeDouble:
		if mf.flags&flagNotNULL != 0 {
			return scanTypeFloat64
		}
		return scanTypeNullFloat

	case fieldTypeDecimal, fieldTypeNewDecimal, fieldTypeVarChar,
		fieldTypeBit, fieldTypeEnum, fieldTypeSet, fieldTypeTinyBLOB,
		fieldTypeMediumBLOB, fieldTypeLongBLOB, fieldTypeBLOB,
		fieldTypeVarString, fieldTypeString, fieldTypeGeometry, fieldTypeJSON,
		fieldTypeTime:
		return scanTypeRawBytes

	//case fieldTypeDate, fieldTypeNewDate,
	//	fieldTypeTimestamp, fieldTypeDateTime:
	//	// NullTime is always returned for more consistent behavior as it can
	//	// handle both cases of parseTime regardless if the field is nullable.
	//	return scanTypeNullTime

	default:
		return scanTypeUnknown
	}
}

func Fuzz(data []byte) int {
	db, err := sql.Open("help", string(data))
	if err != nil {
		return 0
	}
	db.Close()
	return 1
}

var (
	fileRegister       map[string]bool
	fileRegisterLock   sync.RWMutex
	readerRegister     map[string]func() io.Reader
	readerRegisterLock sync.RWMutex
)

// RegisterLocalFile adds the given file to the file allowlist,
// so that it can be used by "LOAD DATA LOCAL INFILE <filepath>".
// Alternatively you can allow the use of all local files with
// the DSN parameter 'allowAllFiles=true'
//
//  filePath := "/home/gopher/data.csv"
//  help.RegisterLocalFile(filePath)
//  err := db.Exec("LOAD DATA LOCAL INFILE '" + filePath + "' INTO TABLE foo")
//  if err != nil {
//  ...
//
func RegisterLocalFile(filePath string) {
	fileRegisterLock.Lock()
	// lazy map init
	if fileRegister == nil {
		fileRegister = make(map[string]bool)
	}

	fileRegister[strings.Trim(filePath, `"`)] = true
	fileRegisterLock.Unlock()
}

// DeregisterLocalFile removes the given filepath from the allowlist.
func DeregisterLocalFile(filePath string) {
	fileRegisterLock.Lock()
	delete(fileRegister, strings.Trim(filePath, `"`))
	fileRegisterLock.Unlock()
}

// RegisterReaderHandler registers a handler function which is used
// to receive a io.Reader.
// The Reader can be used by "LOAD DATA LOCAL INFILE Reader::<name>".
// If the handler returns a io.ReadCloser Close() is called when the
// request is finished.
//
//  help.RegisterReaderHandler("data", func() io.Reader {
//  	var csvReader io.Reader // Some Reader that returns CSV data
//  	... // Open Reader here
//  	return csvReader
//  })
//  err := db.Exec("LOAD DATA LOCAL INFILE 'Reader::data' INTO TABLE foo")
//  if err != nil {
//  ...
//
func RegisterReaderHandler(name string, handler func() io.Reader) {
	readerRegisterLock.Lock()
	// lazy map init
	if readerRegister == nil {
		readerRegister = make(map[string]func() io.Reader)
	}

	readerRegister[name] = handler
	readerRegisterLock.Unlock()
}

// DeregisterReaderHandler removes the ReaderHandler function with
// the given name from the registry.
func DeregisterReaderHandler(name string) {
	readerRegisterLock.Lock()
	delete(readerRegister, name)
	readerRegisterLock.Unlock()
}

func deferredClose(err *error, closer io.Closer) {
	closeErr := closer.Close()
	if *err == nil {
		*err = closeErr
	}
}

func (mc *mysqlConn) handleInFileRequest(name string) (err error) {
	var rdr io.Reader
	var data []byte
	packetSize := 16 * 1024 // 16KB is small enough for disk readahead and large enough for TCP
	if mc.maxWriteSize < packetSize {
		packetSize = mc.maxWriteSize
	}

	if idx := strings.Index(name, "Reader::"); idx == 0 || (idx > 0 && name[idx-1] == '/') { // io.Reader
		// The server might return an an absolute path. See issue #355.
		name = name[idx+8:]

		readerRegisterLock.RLock()
		handler, inMap := readerRegister[name]
		readerRegisterLock.RUnlock()

		if inMap {
			rdr = handler()
			if rdr != nil {
				if cl, ok := rdr.(io.Closer); ok {
					defer deferredClose(&err, cl)
				}
			} else {
				err = fmt.Errorf("Reader '%s' is <nil>", name)
			}
		} else {
			err = fmt.Errorf("Reader '%s' is not registered", name)
		}
	} else { // File
		name = strings.Trim(name, `"`)
		fileRegisterLock.RLock()
		fr := fileRegister[name]
		fileRegisterLock.RUnlock()
		if mc.cfg.AllowAllFiles || fr {
			var file *os.File
			var fi os.FileInfo

			if file, err = os.Open(name); err == nil {
				defer deferredClose(&err, file)

				// get file size
				if fi, err = file.Stat(); err == nil {
					rdr = file
					if fileSize := int(fi.Size()); fileSize < packetSize {
						packetSize = fileSize
					}
				}
			}
		} else {
			err = fmt.Errorf("local file '%s' is not registered", name)
		}
	}

	// send content packets
	// if packetSize == 0, the Reader contains no data
	if err == nil && packetSize > 0 {
		data := make([]byte, 4+packetSize)
		var n int
		for err == nil {
			n, err = rdr.Read(data[4:])
			if n > 0 {
				if ioErr := mc.writePacket(data[:4+n]); ioErr != nil {
					return ioErr
				}
			}
		}
		if err == io.EOF {
			err = nil
		}
	}

	// send empty packet (termination)
	if data == nil {
		data = make([]byte, 4)
	}
	if ioErr := mc.writePacket(data[:4]); ioErr != nil {
		return ioErr
	}

	// read OK packet
	if err == nil {
		return mc.readResultOK()
	}

	mc.readPacket()
	return err
}

// Scan implements the Scanner interface.
// The value type must be time.Time or string / []byte (formatted time-string),
// otherwise Scan fails.
//func (nt *NullTime) Scan(value interface{}) (err error) {
//	if value == nil {
//		nt.Time, nt.Valid = time.Time{}, false
//		return
//	}
//
//	switch v := value.(type) {
//	case time.Time:
//		nt.Time, nt.Valid = v, true
//		return
//	case []byte:
//		nt.Time, err = parseDateTime(v, time.UTC)
//		nt.Valid = (err == nil)
//		return
//	case string:
//		nt.Time, err = parseDateTime([]byte(v), time.UTC)
//		nt.Valid = (err == nil)
//		return
//	}
//
//	nt.Valid = false
//	return fmt.Errorf("Can't convert %T to time.Time", value)
//}
//
//// Value implements the driver Valuer interface.
//func (nt NullTime) Value() (driver.Value, error) {
//	if !nt.Valid {
//		return nil, nil
//	}
//	return nt.Time, nil
//}

func (mc *mysqlConn) readPacket() ([]byte, error) {
	var prevData []byte
	for {
		// read packet header
		data, err := mc.buf.readNext(4)
		if err != nil {
			if cerr := mc.canceled.Value(); cerr != nil {
				return nil, cerr
			}
			errLog.Print(err)
			mc.Close()
			return nil, ErrInvalidConn
		}

		// packet length [24 bit]
		pktLen := int(uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16)

		// check packet sync [8 bit]
		if data[3] != mc.sequence {
			if data[3] > mc.sequence {
				return nil, ErrPktSyncMul
			}
			return nil, ErrPktSync
		}
		mc.sequence++

		// packets with length 0 terminate a previous packet which is a
		// multiple of (2^24)-1 bytes long
		if pktLen == 0 {
			// there was no previous packet
			if prevData == nil {
				errLog.Print(ErrMalformPkt)
				mc.Close()
				return nil, ErrInvalidConn
			}

			return prevData, nil
		}

		// read packet body [pktLen bytes]
		data, err = mc.buf.readNext(pktLen)
		if err != nil {
			if cerr := mc.canceled.Value(); cerr != nil {
				return nil, cerr
			}
			errLog.Print(err)
			mc.Close()
			return nil, ErrInvalidConn
		}

		// return data if this was the last packet
		if pktLen < maxPacketSize {
			// zero allocations for non-split packets
			if prevData == nil {
				return data, nil
			}

			return append(prevData, data...), nil
		}

		prevData = append(prevData, data...)
	}
}

// Write packet buffer 'data'
func (mc *mysqlConn) writePacket(data []byte) error {
	pktLen := len(data) - 4

	if pktLen > mc.maxAllowedPacket {
		return ErrPktTooLarge
	}

	// Perform a stale connection check. We only perform this check for
	// the first query on a connection that has been checked out of the
	// connection pool: a fresh connection from the pool is more likely
	// to be stale, and it has not performed any previous writes that
	// could cause data corruption, so it's safe to return ErrBadConn
	// if the check fails.
	if mc.reset {
		mc.reset = false
		conn := mc.netConn
		if mc.rawConn != nil {
			conn = mc.rawConn
		}
		var err error
		// If this connection has a ReadTimeout which we've been setting on
		// reads, reset it to its default value before we attempt a non-blocking
		// read, otherwise the scheduler will just time us out before we can read
		if mc.cfg.ReadTimeout != 0 {
			err = conn.SetReadDeadline(time.Time{})
		}
		if err == nil && mc.cfg.CheckConnLiveness {
			err = connCheck(conn)
		}
		if err != nil {
			errLog.Print("closing bad idle connection: ", err)
			mc.Close()
			return driver.ErrBadConn
		}
	}

	for {
		var size int
		if pktLen >= maxPacketSize {
			data[0] = 0xff
			data[1] = 0xff
			data[2] = 0xff
			size = maxPacketSize
		} else {
			data[0] = byte(pktLen)
			data[1] = byte(pktLen >> 8)
			data[2] = byte(pktLen >> 16)
			size = pktLen
		}
		data[3] = mc.sequence

		// Write packet
		if mc.writeTimeout > 0 {
			if err := mc.netConn.SetWriteDeadline(time.Now().Add(mc.writeTimeout)); err != nil {
				return err
			}
		}

		n, err := mc.netConn.Write(data[:4+size])
		if err == nil && n == 4+size {
			mc.sequence++
			if size != maxPacketSize {
				return nil
			}
			pktLen -= size
			data = data[size:]
			continue
		}

		// Handle error
		if err == nil { // n != len(data)
			mc.cleanup()
			errLog.Print(ErrMalformPkt)
		} else {
			if cerr := mc.canceled.Value(); cerr != nil {
				return cerr
			}
			if n == 0 && pktLen == len(data)-4 {
				// only for the first loop iteration when nothing was written yet
				return errBadConnNoWrite
			}
			mc.cleanup()
			errLog.Print(err)
		}
		return ErrInvalidConn
	}
}

/******************************************************************************
*                           Initialization Process                            *
******************************************************************************/

// Handshake Initialization Packet
// http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::Handshake
func (mc *mysqlConn) readHandshakePacket() (data []byte, plugin string, err error) {
	data, err = mc.readPacket()
	if err != nil {
		// for init we can rewrite this to ErrBadConn for sql.Driver to retry, since
		// in connection initialization we don't risk retrying non-idempotent actions.
		if err == ErrInvalidConn {
			return nil, "", driver.ErrBadConn
		}
		return
	}

	if data[0] == iERR {
		return nil, "", mc.handleErrorPacket(data)
	}

	// protocol version [1 byte]
	if data[0] < minProtocolVersion {
		return nil, "", fmt.Errorf(
			"unsupported protocol version %d. Version %d or higher is required",
			data[0],
			minProtocolVersion,
		)
	}

	// server version [null terminated string]
	// connection id [4 bytes]
	pos := 1 + bytes.IndexByte(data[1:], 0x00) + 1 + 4

	// first part of the password cipher [8 bytes]
	authData := data[pos : pos+8]

	// (filler) always 0x00 [1 byte]
	pos += 8 + 1

	// capability flags (lower 2 bytes) [2 bytes]
	mc.flags = clientFlag(binary.LittleEndian.Uint16(data[pos : pos+2]))
	if mc.flags&clientProtocol41 == 0 {
		return nil, "", ErrOldProtocol
	}
	if mc.flags&clientSSL == 0 && mc.cfg.tls != nil {
		if mc.cfg.TLSConfig == "preferred" {
			mc.cfg.tls = nil
		} else {
			return nil, "", ErrNoTLS
		}
	}
	pos += 2

	if len(data) > pos {
		// character set [1 byte]
		// status flags [2 bytes]
		// capability flags (upper 2 bytes) [2 bytes]
		// length of auth-plugin-data [1 byte]
		// reserved (all [00]) [10 bytes]
		pos += 1 + 2 + 2 + 1 + 10

		// second part of the password cipher [mininum 13 bytes],
		// where len=MAX(13, length of auth-plugin-data - 8)
		//
		// The web documentation is ambiguous about the length. However,
		// according to help-5.7/sql/auth/sql_authentication.cc line 538,
		// the 13th byte is "\0 byte, terminating the second part of
		// a scramble". So the second part of the password cipher is
		// a NULL terminated string that's at least 13 bytes with the
		// last byte being NULL.
		//
		// The official Python library uses the fixed length 12
		// which seems to work but technically could have a hidden bug.
		authData = append(authData, data[pos:pos+12]...)
		pos += 13

		// EOF if version (>= 5.5.7 and < 5.5.10) or (>= 5.6.0 and < 5.6.2)
		// \NUL otherwise
		if end := bytes.IndexByte(data[pos:], 0x00); end != -1 {
			plugin = string(data[pos : pos+end])
		} else {
			plugin = string(data[pos:])
		}

		// make a memory safe copy of the cipher slice
		var b [20]byte
		copy(b[:], authData)
		return b[:], plugin, nil
	}

	// make a memory safe copy of the cipher slice
	var b [8]byte
	copy(b[:], authData)
	return b[:], plugin, nil
}

// Client Authentication Packet
// http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeResponse
func (mc *mysqlConn) writeHandshakeResponsePacket(authResp []byte, plugin string) error {
	// Adjust client flags based on server support
	clientFlags := clientProtocol41 |
		clientSecureConn |
		clientLongPassword |
		clientTransactions |
		clientLocalFiles |
		clientPluginAuth |
		clientMultiResults |
		mc.flags&clientLongFlag

	if mc.cfg.ClientFoundRows {
		clientFlags |= clientFoundRows
	}

	// To enable TLS / SSL
	if mc.cfg.tls != nil {
		clientFlags |= clientSSL
	}

	if mc.cfg.MultiStatements {
		clientFlags |= clientMultiStatements
	}

	// encode length of the auth plugin data
	var authRespLEIBuf [9]byte
	authRespLen := len(authResp)
	authRespLEI := appendLengthEncodedInteger(authRespLEIBuf[:0], uint64(authRespLen))
	if len(authRespLEI) > 1 {
		// if the length can not be written in 1 byte, it must be written as a
		// length encoded integer
		clientFlags |= clientPluginAuthLenEncClientData
	}

	pktLen := 4 + 4 + 1 + 23 + len(mc.cfg.User) + 1 + len(authRespLEI) + len(authResp) + 21 + 1

	// To specify a db name
	if n := len(mc.cfg.DBName); n > 0 {
		clientFlags |= clientConnectWithDB
		pktLen += n + 1
	}

	// Calculate packet length and get buffer with that size
	data, err := mc.buf.takeSmallBuffer(pktLen + 4)
	if err != nil {
		// cannot take the buffer. Something must be wrong with the connection
		errLog.Print(err)
		return errBadConnNoWrite
	}

	// ClientFlags [32 bit]
	data[4] = byte(clientFlags)
	data[5] = byte(clientFlags >> 8)
	data[6] = byte(clientFlags >> 16)
	data[7] = byte(clientFlags >> 24)

	// MaxPacketSize [32 bit] (none)
	data[8] = 0x00
	data[9] = 0x00
	data[10] = 0x00
	data[11] = 0x00

	// Charset [1 byte]
	var found bool
	data[12], found = collations[mc.cfg.Collation]
	if !found {
		// Note possibility for false negatives:
		// could be triggered  although the collation is valid if the
		// collations map does not contain entries the server supports.
		return errors.New("unknown collation")
	}

	// Filler [23 bytes] (all 0x00)
	pos := 13
	for ; pos < 13+23; pos++ {
		data[pos] = 0
	}

	// SSL Connection Request Packet
	// http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::SSLRequest
	if mc.cfg.tls != nil {
		// Send TLS / SSL request packet
		if err := mc.writePacket(data[:(4+4+1+23)+4]); err != nil {
			return err
		}

		// Switch to TLS
		tlsConn := tls.Client(mc.netConn, mc.cfg.tls)
		if err := tlsConn.Handshake(); err != nil {
			return err
		}
		mc.rawConn = mc.netConn
		mc.netConn = tlsConn
		mc.buf.nc = tlsConn
	}

	// User [null terminated string]
	if len(mc.cfg.User) > 0 {
		pos += copy(data[pos:], mc.cfg.User)
	}
	data[pos] = 0x00
	pos++

	// Auth Data [length encoded integer]
	pos += copy(data[pos:], authRespLEI)
	pos += copy(data[pos:], authResp)

	// Databasename [null terminated string]
	if len(mc.cfg.DBName) > 0 {
		pos += copy(data[pos:], mc.cfg.DBName)
		data[pos] = 0x00
		pos++
	}

	pos += copy(data[pos:], plugin)
	data[pos] = 0x00
	pos++

	// Send Auth packet
	return mc.writePacket(data[:pos])
}

// http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::AuthSwitchResponse
func (mc *mysqlConn) writeAuthSwitchPacket(authData []byte) error {
	pktLen := 4 + len(authData)
	data, err := mc.buf.takeSmallBuffer(pktLen)
	if err != nil {
		// cannot take the buffer. Something must be wrong with the connection
		errLog.Print(err)
		return errBadConnNoWrite
	}

	// Add the auth data [EOF]
	copy(data[4:], authData)
	return mc.writePacket(data)
}

/******************************************************************************
*                             Command Packets                                 *
******************************************************************************/

func (mc *mysqlConn) writeCommandPacket(command byte) error {
	// Reset Packet Sequence
	mc.sequence = 0

	data, err := mc.buf.takeSmallBuffer(4 + 1)
	if err != nil {
		// cannot take the buffer. Something must be wrong with the connection
		errLog.Print(err)
		return errBadConnNoWrite
	}

	// Add command byte
	data[4] = command

	// Send CMD packet
	return mc.writePacket(data)
}

func (mc *mysqlConn) writeCommandPacketStr(command byte, arg string) error {
	// Reset Packet Sequence
	mc.sequence = 0

	pktLen := 1 + len(arg)
	data, err := mc.buf.takeBuffer(pktLen + 4)
	if err != nil {
		// cannot take the buffer. Something must be wrong with the connection
		errLog.Print(err)
		return errBadConnNoWrite
	}

	// Add command byte
	data[4] = command

	// Add arg
	copy(data[5:], arg)

	// Send CMD packet
	return mc.writePacket(data)
}

func (mc *mysqlConn) writeCommandPacketUint32(command byte, arg uint32) error {
	// Reset Packet Sequence
	mc.sequence = 0

	data, err := mc.buf.takeSmallBuffer(4 + 1 + 4)
	if err != nil {
		// cannot take the buffer. Something must be wrong with the connection
		errLog.Print(err)
		return errBadConnNoWrite
	}

	// Add command byte
	data[4] = command

	// Add arg [32 bit]
	data[5] = byte(arg)
	data[6] = byte(arg >> 8)
	data[7] = byte(arg >> 16)
	data[8] = byte(arg >> 24)

	// Send CMD packet
	return mc.writePacket(data)
}

/******************************************************************************
*                              Result Packets                                 *
******************************************************************************/

func (mc *mysqlConn) readAuthResult() ([]byte, string, error) {
	data, err := mc.readPacket()
	if err != nil {
		return nil, "", err
	}

	// packet indicator
	switch data[0] {

	case iOK:
		return nil, "", mc.handleOkPacket(data)

	case iAuthMoreData:
		return data[1:], "", err

	case iEOF:
		if len(data) == 1 {
			// https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::OldAuthSwitchRequest
			return nil, "mysql_old_password", nil
		}
		pluginEndIndex := bytes.IndexByte(data, 0x00)
		if pluginEndIndex < 0 {
			return nil, "", ErrMalformPkt
		}
		plugin := string(data[1:pluginEndIndex])
		authData := data[pluginEndIndex+1:]
		return authData, plugin, nil

	default: // Error otherwise
		return nil, "", mc.handleErrorPacket(data)
	}
}

// Returns error if Packet is not an 'Result OK'-Packet
func (mc *mysqlConn) readResultOK() error {
	data, err := mc.readPacket()
	if err != nil {
		return err
	}

	if data[0] == iOK {
		return mc.handleOkPacket(data)
	}
	return mc.handleErrorPacket(data)
}

// Result Set Header Packet
// http://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::Resultset
func (mc *mysqlConn) readResultSetHeaderPacket() (int, error) {
	data, err := mc.readPacket()
	if err == nil {
		switch data[0] {

		case iOK:
			return 0, mc.handleOkPacket(data)

		case iERR:
			return 0, mc.handleErrorPacket(data)

		case iLocalInFile:
			return 0, mc.handleInFileRequest(string(data[1:]))
		}

		// column count
		num, _, n := readLengthEncodedInteger(data)
		if n-len(data) == 0 {
			return int(num), nil
		}

		return 0, ErrMalformPkt
	}
	return 0, err
}

// Error Packet
// http://dev.mysql.com/doc/internals/en/generic-response-packets.html#packet-ERR_Packet
func (mc *mysqlConn) handleErrorPacket(data []byte) error {
	if data[0] != iERR {
		return ErrMalformPkt
	}

	// 0xff [1 byte]

	// Error Number [16 bit uint]
	errno := binary.LittleEndian.Uint16(data[1:3])

	// 1792: ER_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION
	// 1290: ER_OPTION_PREVENTS_STATEMENT (returned by Aurora during failover)
	if (errno == 1792 || errno == 1290) && mc.cfg.RejectReadOnly {
		// Oops; we are connected to a read-only connection, and won't be able
		// to issue any write statements. Since RejectReadOnly is configured,
		// we throw away this connection hoping this one would have write
		// permission. This is specifically for a possible race condition
		// during failover (e.g. on AWS Aurora). See README.md for more.
		//
		// We explicitly close the connection before returning
		// driver.ErrBadConn to ensure that `database/sql` purges this
		// connection and initiates a new one for next statement next time.
		mc.Close()
		return driver.ErrBadConn
	}

	pos := 3

	// SQL State [optional: # + 5bytes string]
	if data[3] == 0x23 {
		//sqlstate := string(data[4 : 4+5])
		pos = 9
	}

	// Error Message [string]
	return &MySQLError{
		Number:  errno,
		Message: string(data[pos:]),
	}
}

func readStatus(b []byte) statusFlag {
	return statusFlag(b[0]) | statusFlag(b[1])<<8
}

// Ok Packet
// http://dev.mysql.com/doc/internals/en/generic-response-packets.html#packet-OK_Packet
func (mc *mysqlConn) handleOkPacket(data []byte) error {
	var n, m int

	// 0x00 [1 byte]

	// Affected rows [Length Coded Binary]
	mc.affectedRows, _, n = readLengthEncodedInteger(data[1:])

	// Insert id [Length Coded Binary]
	mc.insertId, _, m = readLengthEncodedInteger(data[1+n:])

	// server_status [2 bytes]
	mc.status = readStatus(data[1+n+m : 1+n+m+2])
	if mc.status&statusMoreResultsExists != 0 {
		return nil
	}

	// warning count [2 bytes]

	return nil
}

// Read Packets as Field Packets until EOF-Packet or an Error appears
// http://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnDefinition41
func (mc *mysqlConn) readColumns(count int) ([]mysqlField, error) {
	columns := make([]mysqlField, count)

	for i := 0; ; i++ {
		data, err := mc.readPacket()
		if err != nil {
			return nil, err
		}

		// EOF Packet
		if data[0] == iEOF && (len(data) == 5 || len(data) == 1) {
			if i == count {
				return columns, nil
			}
			return nil, fmt.Errorf("column count mismatch n:%d len:%d", count, len(columns))
		}

		// Catalog
		pos, err := skipLengthEncodedString(data)
		if err != nil {
			return nil, err
		}

		// Database [len coded string]
		n, err := skipLengthEncodedString(data[pos:])
		if err != nil {
			return nil, err
		}
		pos += n

		// Table [len coded string]
		if mc.cfg.ColumnsWithAlias {
			tableName, _, n, err := readLengthEncodedString(data[pos:])
			if err != nil {
				return nil, err
			}
			pos += n
			columns[i].tableName = string(tableName)
		} else {
			n, err = skipLengthEncodedString(data[pos:])
			if err != nil {
				return nil, err
			}
			pos += n
		}

		// Original table [len coded string]
		n, err = skipLengthEncodedString(data[pos:])
		if err != nil {
			return nil, err
		}
		pos += n

		// Name [len coded string]
		name, _, n, err := readLengthEncodedString(data[pos:])
		if err != nil {
			return nil, err
		}
		columns[i].name = string(name)
		pos += n

		// Original name [len coded string]
		n, err = skipLengthEncodedString(data[pos:])
		if err != nil {
			return nil, err
		}
		pos += n

		// Filler [uint8]
		pos++

		// Charset [charset, collation uint8]
		columns[i].charSet = data[pos]
		pos += 2

		// Length [uint32]
		columns[i].length = binary.LittleEndian.Uint32(data[pos : pos+4])
		pos += 4

		// Field type [uint8]
		columns[i].fieldType = fieldType(data[pos])
		pos++

		// Flags [uint16]
		columns[i].flags = fieldFlag(binary.LittleEndian.Uint16(data[pos : pos+2]))
		pos += 2

		// Decimals [uint8]
		columns[i].decimals = data[pos]
		//pos++

		// Default value [len coded binary]
		//if pos < len(data) {
		//	defaultVal, _, err = bytesToLengthCodedBinary(data[pos:])
		//}
	}
}

// Read Packets as Field Packets until EOF-Packet or an Error appears
// http://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::ResultsetRow
func (rows *textRows) readRow(dest []driver.Value) error {
	mc := rows.mc

	if rows.rs.done {
		return io.EOF
	}

	data, err := mc.readPacket()
	if err != nil {
		return err
	}

	// EOF Packet
	if data[0] == iEOF && len(data) == 5 {
		// server_status [2 bytes]
		rows.mc.status = readStatus(data[3:])
		rows.rs.done = true
		if !rows.HasNextResultSet() {
			rows.mc = nil
		}
		return io.EOF
	}
	if data[0] == iERR {
		rows.mc = nil
		return mc.handleErrorPacket(data)
	}

	// RowSet Packet
	var (
		n      int
		isNull bool
		pos    int = 0
	)

	for i := range dest {
		// Read bytes and convert to string
		dest[i], isNull, n, err = readLengthEncodedString(data[pos:])
		pos += n

		if err != nil {
			return err
		}

		if isNull {
			dest[i] = nil
			continue
		}

		if !mc.parseTime {
			continue
		}

		// Parse time field
		switch rows.rs.columns[i].fieldType {
		case fieldTypeTimestamp,
			fieldTypeDateTime,
			fieldTypeDate,
			fieldTypeNewDate:
			if dest[i], err = parseDateTime(dest[i].([]byte), mc.cfg.Loc); err != nil {
				return err
			}
		}
	}

	return nil
}

// Reads Packets until EOF-Packet or an Error appears. Returns count of Packets read
func (mc *mysqlConn) readUntilEOF() error {
	for {
		data, err := mc.readPacket()
		if err != nil {
			return err
		}

		switch data[0] {
		case iERR:
			return mc.handleErrorPacket(data)
		case iEOF:
			if len(data) == 5 {
				mc.status = readStatus(data[3:])
			}
			return nil
		}
	}
}

/******************************************************************************
*                           Prepared Statements                               *
******************************************************************************/

// Prepare Result Packets
// http://dev.mysql.com/doc/internals/en/com-stmt-prepare-response.html
func (stmt *mysqlStmt) readPrepareResultPacket() (uint16, error) {
	data, err := stmt.mc.readPacket()
	if err == nil {
		// packet indicator [1 byte]
		if data[0] != iOK {
			return 0, stmt.mc.handleErrorPacket(data)
		}

		// statement id [4 bytes]
		stmt.id = binary.LittleEndian.Uint32(data[1:5])

		// Column count [16 bit uint]
		columnCount := binary.LittleEndian.Uint16(data[5:7])

		// Param count [16 bit uint]
		stmt.paramCount = int(binary.LittleEndian.Uint16(data[7:9]))

		// Reserved [8 bit]

		// Warning count [16 bit uint]

		return columnCount, nil
	}
	return 0, err
}

// http://dev.mysql.com/doc/internals/en/com-stmt-send-long-data.html
func (stmt *mysqlStmt) writeCommandLongData(paramID int, arg []byte) error {
	maxLen := stmt.mc.maxAllowedPacket - 1
	pktLen := maxLen

	// After the header (bytes 0-3) follows before the data:
	// 1 byte command
	// 4 bytes stmtID
	// 2 bytes paramID
	const dataOffset = 1 + 4 + 2

	// Cannot use the write buffer since
	// a) the buffer is too small
	// b) it is in use
	data := make([]byte, 4+1+4+2+len(arg))

	copy(data[4+dataOffset:], arg)

	for argLen := len(arg); argLen > 0; argLen -= pktLen - dataOffset {
		if dataOffset+argLen < maxLen {
			pktLen = dataOffset + argLen
		}

		stmt.mc.sequence = 0
		// Add command byte [1 byte]
		data[4] = comStmtSendLongData

		// Add stmtID [32 bit]
		data[5] = byte(stmt.id)
		data[6] = byte(stmt.id >> 8)
		data[7] = byte(stmt.id >> 16)
		data[8] = byte(stmt.id >> 24)

		// Add paramID [16 bit]
		data[9] = byte(paramID)
		data[10] = byte(paramID >> 8)

		// Send CMD packet
		err := stmt.mc.writePacket(data[:4+pktLen])
		if err == nil {
			data = data[pktLen-dataOffset:]
			continue
		}
		return err

	}

	// Reset Packet Sequence
	stmt.mc.sequence = 0
	return nil
}

// Execute Prepared Statement
// http://dev.mysql.com/doc/internals/en/com-stmt-execute.html
func (stmt *mysqlStmt) writeExecutePacket(args []driver.Value) error {
	if len(args) != stmt.paramCount {
		return fmt.Errorf(
			"argument count mismatch (got: %d; has: %d)",
			len(args),
			stmt.paramCount,
		)
	}

	const minPktLen = 4 + 1 + 4 + 1 + 4
	mc := stmt.mc

	// Determine threshold dynamically to avoid packet size shortage.
	longDataSize := mc.maxAllowedPacket / (stmt.paramCount + 1)
	if longDataSize < 64 {
		longDataSize = 64
	}

	// Reset packet-sequence
	mc.sequence = 0

	var data []byte
	var err error

	if len(args) == 0 {
		data, err = mc.buf.takeBuffer(minPktLen)
	} else {
		data, err = mc.buf.takeCompleteBuffer()
		// In this case the len(data) == cap(data) which is used to optimise the flow below.
	}
	if err != nil {
		// cannot take the buffer. Something must be wrong with the connection
		errLog.Print(err)
		return errBadConnNoWrite
	}

	// command [1 byte]
	data[4] = comStmtExecute

	// statement_id [4 bytes]
	data[5] = byte(stmt.id)
	data[6] = byte(stmt.id >> 8)
	data[7] = byte(stmt.id >> 16)
	data[8] = byte(stmt.id >> 24)

	// flags (0: CURSOR_TYPE_NO_CURSOR) [1 byte]
	data[9] = 0x00

	// iteration_count (uint32(1)) [4 bytes]
	data[10] = 0x01
	data[11] = 0x00
	data[12] = 0x00
	data[13] = 0x00

	if len(args) > 0 {
		pos := minPktLen

		var nullMask []byte
		if maskLen, typesLen := (len(args)+7)/8, 1+2*len(args); pos+maskLen+typesLen >= cap(data) {
			// buffer has to be extended but we don't know by how much so
			// we depend on append after all data with known sizes fit.
			// We stop at that because we deal with a lot of columns here
			// which makes the required allocation size hard to guess.
			tmp := make([]byte, pos+maskLen+typesLen)
			copy(tmp[:pos], data[:pos])
			data = tmp
			nullMask = data[pos : pos+maskLen]
			// No need to clean nullMask as make ensures that.
			pos += maskLen
		} else {
			nullMask = data[pos : pos+maskLen]
			for i := range nullMask {
				nullMask[i] = 0
			}
			pos += maskLen
		}

		// newParameterBoundFlag 1 [1 byte]
		data[pos] = 0x01
		pos++

		// type of each parameter [len(args)*2 bytes]
		paramTypes := data[pos:]
		pos += len(args) * 2

		// value of each parameter [n bytes]
		paramValues := data[pos:pos]
		valuesCap := cap(paramValues)

		for i, arg := range args {
			// build NULL-bitmap
			if arg == nil {
				nullMask[i/8] |= 1 << (uint(i) & 7)
				paramTypes[i+i] = byte(fieldTypeNULL)
				paramTypes[i+i+1] = 0x00
				continue
			}

			if v, ok := arg.(json.RawMessage); ok {
				arg = []byte(v)
			}
			// cache types and values
			switch v := arg.(type) {
			case int64:
				paramTypes[i+i] = byte(fieldTypeLongLong)
				paramTypes[i+i+1] = 0x00

				if cap(paramValues)-len(paramValues)-8 >= 0 {
					paramValues = paramValues[:len(paramValues)+8]
					binary.LittleEndian.PutUint64(
						paramValues[len(paramValues)-8:],
						uint64(v),
					)
				} else {
					paramValues = append(paramValues,
						uint64ToBytes(uint64(v))...,
					)
				}

			case uint64:
				paramTypes[i+i] = byte(fieldTypeLongLong)
				paramTypes[i+i+1] = 0x80 // type is unsigned

				if cap(paramValues)-len(paramValues)-8 >= 0 {
					paramValues = paramValues[:len(paramValues)+8]
					binary.LittleEndian.PutUint64(
						paramValues[len(paramValues)-8:],
						uint64(v),
					)
				} else {
					paramValues = append(paramValues,
						uint64ToBytes(uint64(v))...,
					)
				}

			case float64:
				paramTypes[i+i] = byte(fieldTypeDouble)
				paramTypes[i+i+1] = 0x00

				if cap(paramValues)-len(paramValues)-8 >= 0 {
					paramValues = paramValues[:len(paramValues)+8]
					binary.LittleEndian.PutUint64(
						paramValues[len(paramValues)-8:],
						math.Float64bits(v),
					)
				} else {
					paramValues = append(paramValues,
						uint64ToBytes(math.Float64bits(v))...,
					)
				}

			case bool:
				paramTypes[i+i] = byte(fieldTypeTiny)
				paramTypes[i+i+1] = 0x00

				if v {
					paramValues = append(paramValues, 0x01)
				} else {
					paramValues = append(paramValues, 0x00)
				}

			case []byte:
				// Common case (non-nil value) first
				if v != nil {
					paramTypes[i+i] = byte(fieldTypeString)
					paramTypes[i+i+1] = 0x00

					if len(v) < longDataSize {
						paramValues = appendLengthEncodedInteger(paramValues,
							uint64(len(v)),
						)
						paramValues = append(paramValues, v...)
					} else {
						if err := stmt.writeCommandLongData(i, v); err != nil {
							return err
						}
					}
					continue
				}

				// Handle []byte(nil) as a NULL value
				nullMask[i/8] |= 1 << (uint(i) & 7)
				paramTypes[i+i] = byte(fieldTypeNULL)
				paramTypes[i+i+1] = 0x00

			case string:
				paramTypes[i+i] = byte(fieldTypeString)
				paramTypes[i+i+1] = 0x00

				if len(v) < longDataSize {
					paramValues = appendLengthEncodedInteger(paramValues,
						uint64(len(v)),
					)
					paramValues = append(paramValues, v...)
				} else {
					if err := stmt.writeCommandLongData(i, []byte(v)); err != nil {
						return err
					}
				}

			case time.Time:
				paramTypes[i+i] = byte(fieldTypeString)
				paramTypes[i+i+1] = 0x00

				var a [64]byte
				var b = a[:0]

				if v.IsZero() {
					b = append(b, "0000-00-00"...)
				} else {
					b, err = appendDateTime(b, v.In(mc.cfg.Loc))
					if err != nil {
						return err
					}
				}

				paramValues = appendLengthEncodedInteger(paramValues,
					uint64(len(b)),
				)
				paramValues = append(paramValues, b...)

			default:
				return fmt.Errorf("cannot convert type: %T", arg)
			}
		}

		// Check if param values exceeded the available buffer
		// In that case we must build the data packet with the new values buffer
		if valuesCap != cap(paramValues) {
			data = append(data[:pos], paramValues...)
			if err = mc.buf.store(data); err != nil {
				errLog.Print(err)
				return errBadConnNoWrite
			}
		}

		pos += len(paramValues)
		data = data[:pos]
	}

	return mc.writePacket(data)
}

func (mc *mysqlConn) discardResults() error {
	for mc.status&statusMoreResultsExists != 0 {
		resLen, err := mc.readResultSetHeaderPacket()
		if err != nil {
			return err
		}
		if resLen > 0 {
			// columns
			if err := mc.readUntilEOF(); err != nil {
				return err
			}
			// rows
			if err := mc.readUntilEOF(); err != nil {
				return err
			}
		}
	}
	return nil
}

// http://dev.mysql.com/doc/internals/en/binary-protocol-resultset-row.html
func (rows *binaryRows) readRow(dest []driver.Value) error {
	data, err := rows.mc.readPacket()
	if err != nil {
		return err
	}

	// packet indicator [1 byte]
	if data[0] != iOK {
		// EOF Packet
		if data[0] == iEOF && len(data) == 5 {
			rows.mc.status = readStatus(data[3:])
			rows.rs.done = true
			if !rows.HasNextResultSet() {
				rows.mc = nil
			}
			return io.EOF
		}
		mc := rows.mc
		rows.mc = nil

		// Error otherwise
		return mc.handleErrorPacket(data)
	}

	// NULL-bitmap,  [(column-count + 7 + 2) / 8 bytes]
	pos := 1 + (len(dest)+7+2)>>3
	nullMask := data[1:pos]

	for i := range dest {
		// Field is NULL
		// (byte >> bit-pos) % 2 == 1
		if ((nullMask[(i+2)>>3] >> uint((i+2)&7)) & 1) == 1 {
			dest[i] = nil
			continue
		}

		// Convert to byte-coded string
		switch rows.rs.columns[i].fieldType {
		case fieldTypeNULL:
			dest[i] = nil
			continue

		// Numeric Types
		case fieldTypeTiny:
			if rows.rs.columns[i].flags&flagUnsigned != 0 {
				dest[i] = int64(data[pos])
			} else {
				dest[i] = int64(int8(data[pos]))
			}
			pos++
			continue

		case fieldTypeShort, fieldTypeYear:
			if rows.rs.columns[i].flags&flagUnsigned != 0 {
				dest[i] = int64(binary.LittleEndian.Uint16(data[pos : pos+2]))
			} else {
				dest[i] = int64(int16(binary.LittleEndian.Uint16(data[pos : pos+2])))
			}
			pos += 2
			continue

		case fieldTypeInt24, fieldTypeLong:
			if rows.rs.columns[i].flags&flagUnsigned != 0 {
				dest[i] = int64(binary.LittleEndian.Uint32(data[pos : pos+4]))
			} else {
				dest[i] = int64(int32(binary.LittleEndian.Uint32(data[pos : pos+4])))
			}
			pos += 4
			continue

		case fieldTypeLongLong:
			if rows.rs.columns[i].flags&flagUnsigned != 0 {
				val := binary.LittleEndian.Uint64(data[pos : pos+8])
				if val > math.MaxInt64 {
					dest[i] = uint64ToString(val)
				} else {
					dest[i] = int64(val)
				}
			} else {
				dest[i] = int64(binary.LittleEndian.Uint64(data[pos : pos+8]))
			}
			pos += 8
			continue

		case fieldTypeFloat:
			dest[i] = math.Float32frombits(binary.LittleEndian.Uint32(data[pos : pos+4]))
			pos += 4
			continue

		case fieldTypeDouble:
			dest[i] = math.Float64frombits(binary.LittleEndian.Uint64(data[pos : pos+8]))
			pos += 8
			continue

		// Length coded Binary Strings
		case fieldTypeDecimal, fieldTypeNewDecimal, fieldTypeVarChar,
			fieldTypeBit, fieldTypeEnum, fieldTypeSet, fieldTypeTinyBLOB,
			fieldTypeMediumBLOB, fieldTypeLongBLOB, fieldTypeBLOB,
			fieldTypeVarString, fieldTypeString, fieldTypeGeometry, fieldTypeJSON:
			var isNull bool
			var n int
			dest[i], isNull, n, err = readLengthEncodedString(data[pos:])
			pos += n
			if err == nil {
				if !isNull {
					continue
				} else {
					dest[i] = nil
					continue
				}
			}
			return err

		case
			fieldTypeDate, fieldTypeNewDate, // Date YYYY-MM-DD
			fieldTypeTime,                         // Time [-][H]HH:MM:SS[.fractal]
			fieldTypeTimestamp, fieldTypeDateTime: // Timestamp YYYY-MM-DD HH:MM:SS[.fractal]

			num, isNull, n := readLengthEncodedInteger(data[pos:])
			pos += n

			switch {
			case isNull:
				dest[i] = nil
				continue
			case rows.rs.columns[i].fieldType == fieldTypeTime:
				// database/sql does not support an equivalent to TIME, return a string
				var dstlen uint8
				switch decimals := rows.rs.columns[i].decimals; decimals {
				case 0x00, 0x1f:
					dstlen = 8
				case 1, 2, 3, 4, 5, 6:
					dstlen = 8 + 1 + decimals
				default:
					return fmt.Errorf(
						"protocol error, illegal decimals value %d",
						rows.rs.columns[i].decimals,
					)
				}
				dest[i], err = formatBinaryTime(data[pos:pos+int(num)], dstlen)
			case rows.mc.parseTime:
				dest[i], err = parseBinaryDateTime(num, data[pos:], rows.mc.cfg.Loc)
			default:
				var dstlen uint8
				if rows.rs.columns[i].fieldType == fieldTypeDate {
					dstlen = 10
				} else {
					switch decimals := rows.rs.columns[i].decimals; decimals {
					case 0x00, 0x1f:
						dstlen = 19
					case 1, 2, 3, 4, 5, 6:
						dstlen = 19 + 1 + decimals
					default:
						return fmt.Errorf(
							"protocol error, illegal decimals value %d",
							rows.rs.columns[i].decimals,
						)
					}
				}
				dest[i], err = formatBinaryDateTime(data[pos:pos+int(num)], dstlen)
			}

			if err == nil {
				pos += int(num)
				continue
			} else {
				return err
			}

		// Please report if this happens!
		default:
			return fmt.Errorf("unknown field type %d", rows.rs.columns[i].fieldType)
		}
	}

	return nil
}

type mysqlResult struct {
	affectedRows int64
	insertId     int64
}

func (res *mysqlResult) LastInsertId() (int64, error) {
	return res.insertId, nil
}

func (res *mysqlResult) RowsAffected() (int64, error) {
	return res.affectedRows, nil
}

type resultSet struct {
	columns     []mysqlField
	columnNames []string
	done        bool
}

type mysqlRows struct {
	mc     *mysqlConn
	rs     resultSet
	finish func()
}

type binaryRows struct {
	mysqlRows
}

type textRows struct {
	mysqlRows
}

func (rows *mysqlRows) Columns() []string {
	if rows.rs.columnNames != nil {
		return rows.rs.columnNames
	}

	columns := make([]string, len(rows.rs.columns))
	if rows.mc != nil && rows.mc.cfg.ColumnsWithAlias {
		for i := range columns {
			if tableName := rows.rs.columns[i].tableName; len(tableName) > 0 {
				columns[i] = tableName + "." + rows.rs.columns[i].name
			} else {
				columns[i] = rows.rs.columns[i].name
			}
		}
	} else {
		for i := range columns {
			columns[i] = rows.rs.columns[i].name
		}
	}

	rows.rs.columnNames = columns
	return columns
}

func (rows *mysqlRows) ColumnTypeDatabaseTypeName(i int) string {
	return rows.rs.columns[i].typeDatabaseName()
}

// func (rows *mysqlRows) ColumnTypeLength(i int) (length int64, ok bool) {
// 	return int64(rows.rs.columns[i].length), true
// }

func (rows *mysqlRows) ColumnTypeNullable(i int) (nullable, ok bool) {
	return rows.rs.columns[i].flags&flagNotNULL == 0, true
}

func (rows *mysqlRows) ColumnTypePrecisionScale(i int) (int64, int64, bool) {
	column := rows.rs.columns[i]
	decimals := int64(column.decimals)

	switch column.fieldType {
	case fieldTypeDecimal, fieldTypeNewDecimal:
		if decimals > 0 {
			return int64(column.length) - 2, decimals, true
		}
		return int64(column.length) - 1, decimals, true
	case fieldTypeTimestamp, fieldTypeDateTime, fieldTypeTime:
		return decimals, decimals, true
	case fieldTypeFloat, fieldTypeDouble:
		if decimals == 0x1f {
			return math.MaxInt64, math.MaxInt64, true
		}
		return math.MaxInt64, decimals, true
	}

	return 0, 0, false
}

func (rows *mysqlRows) ColumnTypeScanType(i int) reflect.Type {
	return rows.rs.columns[i].scanType()
}

func (rows *mysqlRows) Close() (err error) {
	if f := rows.finish; f != nil {
		f()
		rows.finish = nil
	}

	mc := rows.mc
	if mc == nil {
		return nil
	}
	if err := mc.error(); err != nil {
		return err
	}

	// flip the buffer for this connection if we need to drain it.
	// note that for a successful query (i.e. one where rows.next()
	// has been called until it returns false), `rows.mc` will be nil
	// by the time the user calls `(*Rows).Close`, so we won't reach this
	// see: https://github.com/golang/go/commit/651ddbdb5056ded455f47f9c494c67b389622a47
	mc.buf.flip()

	// Remove unread packets from stream
	if !rows.rs.done {
		err = mc.readUntilEOF()
	}
	if err == nil {
		if err = mc.discardResults(); err != nil {
			return err
		}
	}

	rows.mc = nil
	return err
}

func (rows *mysqlRows) HasNextResultSet() (b bool) {
	if rows.mc == nil {
		return false
	}
	return rows.mc.status&statusMoreResultsExists != 0
}

func (rows *mysqlRows) nextResultSet() (int, error) {
	if rows.mc == nil {
		return 0, io.EOF
	}
	if err := rows.mc.error(); err != nil {
		return 0, err
	}

	// Remove unread packets from stream
	if !rows.rs.done {
		if err := rows.mc.readUntilEOF(); err != nil {
			return 0, err
		}
		rows.rs.done = true
	}

	if !rows.HasNextResultSet() {
		rows.mc = nil
		return 0, io.EOF
	}
	rows.rs = resultSet{}
	return rows.mc.readResultSetHeaderPacket()
}

func (rows *mysqlRows) nextNotEmptyResultSet() (int, error) {
	for {
		resLen, err := rows.nextResultSet()
		if err != nil {
			return 0, err
		}

		if resLen > 0 {
			return resLen, nil
		}

		rows.rs.done = true
	}
}

func (rows *binaryRows) NextResultSet() error {
	resLen, err := rows.nextNotEmptyResultSet()
	if err != nil {
		return err
	}

	rows.rs.columns, err = rows.mc.readColumns(resLen)
	return err
}

func (rows *binaryRows) Next(dest []driver.Value) error {
	if mc := rows.mc; mc != nil {
		if err := mc.error(); err != nil {
			return err
		}

		// Fetch next row from stream
		return rows.readRow(dest)
	}
	return io.EOF
}

func (rows *textRows) NextResultSet() (err error) {
	resLen, err := rows.nextNotEmptyResultSet()
	if err != nil {
		return err
	}

	rows.rs.columns, err = rows.mc.readColumns(resLen)
	return err
}

func (rows *textRows) Next(dest []driver.Value) error {
	if mc := rows.mc; mc != nil {
		if err := mc.error(); err != nil {
			return err
		}

		// Fetch next row from stream
		return rows.readRow(dest)
	}
	return io.EOF
}

type mysqlStmt struct {
	mc         *mysqlConn
	id         uint32
	paramCount int
}

func (stmt *mysqlStmt) Close() error {
	if stmt.mc == nil || stmt.mc.closed.IsSet() {
		// driver.Stmt.Close can be called more than once, thus this function
		// has to be idempotent.
		// See also Issue #450 and golang/go#16019.
		//errLog.Print(ErrInvalidConn)
		return driver.ErrBadConn
	}

	err := stmt.mc.writeCommandPacketUint32(comStmtClose, stmt.id)
	stmt.mc = nil
	return err
}

func (stmt *mysqlStmt) NumInput() int {
	return stmt.paramCount
}

func (stmt *mysqlStmt) ColumnConverter(idx int) driver.ValueConverter {
	return converter{}
}

func (stmt *mysqlStmt) CheckNamedValue(nv *driver.NamedValue) (err error) {
	nv.Value, err = converter{}.ConvertValue(nv.Value)
	return
}

func (stmt *mysqlStmt) Exec(args []driver.Value) (driver.Result, error) {
	if stmt.mc.closed.IsSet() {
		errLog.Print(ErrInvalidConn)
		return nil, driver.ErrBadConn
	}
	// Send command
	err := stmt.writeExecutePacket(args)
	if err != nil {
		return nil, stmt.mc.markBadConn(err)
	}

	mc := stmt.mc

	mc.affectedRows = 0
	mc.insertId = 0

	// Read Result
	resLen, err := mc.readResultSetHeaderPacket()
	if err != nil {
		return nil, err
	}

	if resLen > 0 {
		// Columns
		if err = mc.readUntilEOF(); err != nil {
			return nil, err
		}

		// Rows
		if err := mc.readUntilEOF(); err != nil {
			return nil, err
		}
	}

	if err := mc.discardResults(); err != nil {
		return nil, err
	}

	return &mysqlResult{
		affectedRows: int64(mc.affectedRows),
		insertId:     int64(mc.insertId),
	}, nil
}

func (stmt *mysqlStmt) Query(args []driver.Value) (driver.Rows, error) {
	return stmt.query(args)
}

func (stmt *mysqlStmt) query(args []driver.Value) (*binaryRows, error) {
	if stmt.mc.closed.IsSet() {
		errLog.Print(ErrInvalidConn)
		return nil, driver.ErrBadConn
	}
	// Send command
	err := stmt.writeExecutePacket(args)
	if err != nil {
		return nil, stmt.mc.markBadConn(err)
	}

	mc := stmt.mc

	// Read Result
	resLen, err := mc.readResultSetHeaderPacket()
	if err != nil {
		return nil, err
	}

	rows := new(binaryRows)

	if resLen > 0 {
		rows.mc = mc
		rows.rs.columns, err = mc.readColumns(resLen)
	} else {
		rows.rs.done = true

		switch err := rows.NextResultSet(); err {
		case nil, io.EOF:
			return rows, nil
		default:
			return nil, err
		}
	}

	return rows, err
}

var jsonType = reflect.TypeOf(json.RawMessage{})

type converter struct{}

// ConvertValue mirrors the reference/default converter in database/sql/driver
// with _one_ exception.  We support uint64 with their high bit and the default
// implementation does not.  This function should be kept in sync with
// database/sql/driver defaultConverter.ConvertValue() except for that
// deliberate difference.
func (c converter) ConvertValue(v interface{}) (driver.Value, error) {
	if driver.IsValue(v) {
		return v, nil
	}

	if vr, ok := v.(driver.Valuer); ok {
		sv, err := callValuerValue(vr)
		if err != nil {
			return nil, err
		}
		if driver.IsValue(sv) {
			return sv, nil
		}
		// A value returend from the Valuer interface can be "a type handled by
		// a database driver's NamedValueChecker interface" so we should accept
		// uint64 here as well.
		if u, ok := sv.(uint64); ok {
			return u, nil
		}
		return nil, fmt.Errorf("non-Value type %T returned from Value", sv)
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Ptr:
		// indirect pointers
		if rv.IsNil() {
			return nil, nil
		} else {
			return c.ConvertValue(rv.Elem().Interface())
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return rv.Uint(), nil
	case reflect.Float32, reflect.Float64:
		return rv.Float(), nil
	case reflect.Bool:
		return rv.Bool(), nil
	case reflect.Slice:
		switch t := rv.Type(); {
		case t == jsonType:
			return v, nil
		case t.Elem().Kind() == reflect.Uint8:
			return rv.Bytes(), nil
		default:
			return nil, fmt.Errorf("unsupported type %T, a slice of %s", v, t.Elem().Kind())
		}
	case reflect.String:
		return rv.String(), nil
	}
	return nil, fmt.Errorf("unsupported type %T, a %s", v, rv.Kind())
}

var valuerReflectType = reflect.TypeOf((*driver.Valuer)(nil)).Elem()

// callValuerValue returns vr.Value(), with one exception:
// If vr.Value is an auto-generated method on a pointer type and the
// pointer is nil, it would panic at runtime in the panicwrap
// method. Treat it like nil instead.
//
// This is so people can implement driver.Value on value types and
// still use nil pointers to those types to mean nil/NULL, just like
// string/*string.
//
// This is an exact copy of the same-named unexported function from the
// database/sql package.
func callValuerValue(vr driver.Valuer) (v driver.Value, err error) {
	if rv := reflect.ValueOf(vr); rv.Kind() == reflect.Ptr &&
		rv.IsNil() &&
		rv.Type().Elem().Implements(valuerReflectType) {
		return nil, nil
	}
	return vr.Value()
}

type mysqlTx struct {
	mc *mysqlConn
}

func (tx *mysqlTx) Commit() (err error) {
	if tx.mc == nil || tx.mc.closed.IsSet() {
		return ErrInvalidConn
	}
	err = tx.mc.exec("COMMIT")
	tx.mc = nil
	return
}

func (tx *mysqlTx) Rollback() (err error) {
	if tx.mc == nil || tx.mc.closed.IsSet() {
		return ErrInvalidConn
	}
	err = tx.mc.exec("ROLLBACK")
	tx.mc = nil
	return
}

// Registry for custom tls.Configs
var (
	tlsConfigLock     sync.RWMutex
	tlsConfigRegistry map[string]*tls.Config
)

// RegisterTLSConfig registers a custom tls.Config to be used with sql.Open.
// Use the key as a value in the DSN where tls=value.
//
// Note: The provided tls.Config is exclusively owned by the driver after
// registering it.
//
//  rootCertPool := x509.NewCertPool()
//  pem, err := ioutil.ReadFile("/path/ca-cert.pem")
//  if err != nil {
//      log.Fatal(err)
//  }
//  if ok := rootCertPool.AppendCertsFromPEM(pem); !ok {
//      log.Fatal("Failed to append PEM.")
//  }
//  clientCert := make([]tls.Certificate, 0, 1)
//  certs, err := tls.LoadX509KeyPair("/path/client-cert.pem", "/path/client-key.pem")
//  if err != nil {
//      log.Fatal(err)
//  }
//  clientCert = append(clientCert, certs)
//  help.RegisterTLSConfig("custom", &tls.Config{
//      RootCAs: rootCertPool,
//      Certificates: clientCert,
//  })
//  db, err := sql.Open("help", "user@tcp(localhost:3306)/test?tls=custom")
//
func RegisterTLSConfig(key string, config *tls.Config) error {
	if _, isBool := readBool(key); isBool || strings.ToLower(key) == "skip-verify" || strings.ToLower(key) == "preferred" {
		return fmt.Errorf("key '%s' is reserved", key)
	}

	tlsConfigLock.Lock()
	if tlsConfigRegistry == nil {
		tlsConfigRegistry = make(map[string]*tls.Config)
	}

	tlsConfigRegistry[key] = config
	tlsConfigLock.Unlock()
	return nil
}

// DeregisterTLSConfig removes the tls.Config associated with key.
func DeregisterTLSConfig(key string) {
	tlsConfigLock.Lock()
	if tlsConfigRegistry != nil {
		delete(tlsConfigRegistry, key)
	}
	tlsConfigLock.Unlock()
}

func getTLSConfigClone(key string) (config *tls.Config) {
	tlsConfigLock.RLock()
	if v, ok := tlsConfigRegistry[key]; ok {
		config = v.Clone()
	}
	tlsConfigLock.RUnlock()
	return
}

// Returns the bool value of the input.
// The 2nd return value indicates if the input was a valid bool value
func readBool(input string) (value bool, valid bool) {
	switch input {
	case "1", "true", "TRUE", "True":
		return true, true
	case "0", "false", "FALSE", "False":
		return false, true
	}

	// Not a valid bool value
	return
}

/******************************************************************************
*                           Time related utils                                *
******************************************************************************/

func parseDateTime(b []byte, loc *time.Location) (time.Time, error) {
	const base = "0000-00-00 00:00:00.000000"
	switch len(b) {
	case 10, 19, 21, 22, 23, 24, 25, 26: // up to "YYYY-MM-DD HH:MM:SS.MMMMMM"
		if string(b) == base[:len(b)] {
			return time.Time{}, nil
		}

		year, err := parseByteYear(b)
		if err != nil {
			return time.Time{}, err
		}
		if year <= 0 {
			year = 1
		}

		if b[4] != '-' {
			return time.Time{}, fmt.Errorf("bad value for field: `%c`", b[4])
		}

		m, err := parseByte2Digits(b[5], b[6])
		if err != nil {
			return time.Time{}, err
		}
		if m <= 0 {
			m = 1
		}
		month := time.Month(m)

		if b[7] != '-' {
			return time.Time{}, fmt.Errorf("bad value for field: `%c`", b[7])
		}

		day, err := parseByte2Digits(b[8], b[9])
		if err != nil {
			return time.Time{}, err
		}
		if day <= 0 {
			day = 1
		}
		if len(b) == 10 {
			return time.Date(year, month, day, 0, 0, 0, 0, loc), nil
		}

		if b[10] != ' ' {
			return time.Time{}, fmt.Errorf("bad value for field: `%c`", b[10])
		}

		hour, err := parseByte2Digits(b[11], b[12])
		if err != nil {
			return time.Time{}, err
		}
		if b[13] != ':' {
			return time.Time{}, fmt.Errorf("bad value for field: `%c`", b[13])
		}

		min, err := parseByte2Digits(b[14], b[15])
		if err != nil {
			return time.Time{}, err
		}
		if b[16] != ':' {
			return time.Time{}, fmt.Errorf("bad value for field: `%c`", b[16])
		}

		sec, err := parseByte2Digits(b[17], b[18])
		if err != nil {
			return time.Time{}, err
		}
		if len(b) == 19 {
			return time.Date(year, month, day, hour, min, sec, 0, loc), nil
		}

		if b[19] != '.' {
			return time.Time{}, fmt.Errorf("bad value for field: `%c`", b[19])
		}
		nsec, err := parseByteNanoSec(b[20:])
		if err != nil {
			return time.Time{}, err
		}
		return time.Date(year, month, day, hour, min, sec, nsec, loc), nil
	default:
		return time.Time{}, fmt.Errorf("invalid time bytes: %s", b)
	}
}

func parseByteYear(b []byte) (int, error) {
	year, n := 0, 1000
	for i := 0; i < 4; i++ {
		v, err := bToi(b[i])
		if err != nil {
			return 0, err
		}
		year += v * n
		n /= 10
	}
	return year, nil
}

func parseByte2Digits(b1, b2 byte) (int, error) {
	d1, err := bToi(b1)
	if err != nil {
		return 0, err
	}
	d2, err := bToi(b2)
	if err != nil {
		return 0, err
	}
	return d1*10 + d2, nil
}

func parseByteNanoSec(b []byte) (int, error) {
	ns, digit := 0, 100000 // max is 6-digits
	for i := 0; i < len(b); i++ {
		v, err := bToi(b[i])
		if err != nil {
			return 0, err
		}
		ns += v * digit
		digit /= 10
	}
	// nanoseconds has 10-digits. (needs to scale digits)
	// 10 - 6 = 4, so we have to multiple 1000.
	return ns * 1000, nil
}

func bToi(b byte) (int, error) {
	if b < '0' || b > '9' {
		return 0, errors.New("not [0-9]")
	}
	return int(b - '0'), nil
}

func parseBinaryDateTime(num uint64, data []byte, loc *time.Location) (driver.Value, error) {
	switch num {
	case 0:
		return time.Time{}, nil
	case 4:
		return time.Date(
			int(binary.LittleEndian.Uint16(data[:2])), // year
			time.Month(data[2]),                       // month
			int(data[3]),                              // day
			0, 0, 0, 0,
			loc,
		), nil
	case 7:
		return time.Date(
			int(binary.LittleEndian.Uint16(data[:2])), // year
			time.Month(data[2]),                       // month
			int(data[3]),                              // day
			int(data[4]),                              // hour
			int(data[5]),                              // minutes
			int(data[6]),                              // seconds
			0,
			loc,
		), nil
	case 11:
		return time.Date(
			int(binary.LittleEndian.Uint16(data[:2])), // year
			time.Month(data[2]),                       // month
			int(data[3]),                              // day
			int(data[4]),                              // hour
			int(data[5]),                              // minutes
			int(data[6]),                              // seconds
			int(binary.LittleEndian.Uint32(data[7:11]))*1000, // nanoseconds
			loc,
		), nil
	}
	return nil, fmt.Errorf("invalid DATETIME packet length %d", num)
}

func appendDateTime(buf []byte, t time.Time) ([]byte, error) {
	year, month, day := t.Date()
	hour, min, sec := t.Clock()
	nsec := t.Nanosecond()

	if year < 1 || year > 9999 {
		return buf, errors.New("year is not in the range [1, 9999]: " + strconv.Itoa(year)) // use errors.New instead of fmt.Errorf to avoid year escape to heap
	}
	year100 := year / 100
	year1 := year % 100

	var localBuf [len("2006-01-02T15:04:05.999999999")]byte // does not escape
	localBuf[0], localBuf[1], localBuf[2], localBuf[3] = digits10[year100], digits01[year100], digits10[year1], digits01[year1]
	localBuf[4] = '-'
	localBuf[5], localBuf[6] = digits10[month], digits01[month]
	localBuf[7] = '-'
	localBuf[8], localBuf[9] = digits10[day], digits01[day]

	if hour == 0 && min == 0 && sec == 0 && nsec == 0 {
		return append(buf, localBuf[:10]...), nil
	}

	localBuf[10] = ' '
	localBuf[11], localBuf[12] = digits10[hour], digits01[hour]
	localBuf[13] = ':'
	localBuf[14], localBuf[15] = digits10[min], digits01[min]
	localBuf[16] = ':'
	localBuf[17], localBuf[18] = digits10[sec], digits01[sec]

	if nsec == 0 {
		return append(buf, localBuf[:19]...), nil
	}
	nsec100000000 := nsec / 100000000
	nsec1000000 := (nsec / 1000000) % 100
	nsec10000 := (nsec / 10000) % 100
	nsec100 := (nsec / 100) % 100
	nsec1 := nsec % 100
	localBuf[19] = '.'

	// milli second
	localBuf[20], localBuf[21], localBuf[22] =
		digits01[nsec100000000], digits10[nsec1000000], digits01[nsec1000000]
	// micro second
	localBuf[23], localBuf[24], localBuf[25] =
		digits10[nsec10000], digits01[nsec10000], digits10[nsec100]
	// nano second
	localBuf[26], localBuf[27], localBuf[28] =
		digits01[nsec100], digits10[nsec1], digits01[nsec1]

	// trim trailing zeros
	n := len(localBuf)
	for n > 0 && localBuf[n-1] == '0' {
		n--
	}

	return append(buf, localBuf[:n]...), nil
}

// zeroDateTime is used in formatBinaryDateTime to avoid an allocation
// if the DATE or DATETIME has the zero value.
// It must never be changed.
// The current behavior depends on database/sql copying the result.
var zeroDateTime = []byte("0000-00-00 00:00:00.000000")

const digits01 = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
const digits10 = "0000000000111111111122222222223333333333444444444455555555556666666666777777777788888888889999999999"

func appendMicrosecs(dst, src []byte, decimals int) []byte {
	if decimals <= 0 {
		return dst
	}
	if len(src) == 0 {
		return append(dst, ".000000"[:decimals+1]...)
	}

	microsecs := binary.LittleEndian.Uint32(src[:4])
	p1 := byte(microsecs / 10000)
	microsecs -= 10000 * uint32(p1)
	p2 := byte(microsecs / 100)
	microsecs -= 100 * uint32(p2)
	p3 := byte(microsecs)

	switch decimals {
	default:
		return append(dst, '.',
			digits10[p1], digits01[p1],
			digits10[p2], digits01[p2],
			digits10[p3], digits01[p3],
		)
	case 1:
		return append(dst, '.',
			digits10[p1],
		)
	case 2:
		return append(dst, '.',
			digits10[p1], digits01[p1],
		)
	case 3:
		return append(dst, '.',
			digits10[p1], digits01[p1],
			digits10[p2],
		)
	case 4:
		return append(dst, '.',
			digits10[p1], digits01[p1],
			digits10[p2], digits01[p2],
		)
	case 5:
		return append(dst, '.',
			digits10[p1], digits01[p1],
			digits10[p2], digits01[p2],
			digits10[p3],
		)
	}
}

func formatBinaryDateTime(src []byte, length uint8) (driver.Value, error) {
	// length expects the deterministic length of the zero value,
	// negative time and 100+ hours are automatically added if needed
	if len(src) == 0 {
		return zeroDateTime[:length], nil
	}
	var dst []byte      // return value
	var p1, p2, p3 byte // current digit pair

	switch length {
	case 10, 19, 21, 22, 23, 24, 25, 26:
	default:
		t := "DATE"
		if length > 10 {
			t += "TIME"
		}
		return nil, fmt.Errorf("illegal %s length %d", t, length)
	}
	switch len(src) {
	case 4, 7, 11:
	default:
		t := "DATE"
		if length > 10 {
			t += "TIME"
		}
		return nil, fmt.Errorf("illegal %s packet length %d", t, len(src))
	}
	dst = make([]byte, 0, length)
	// start with the date
	year := binary.LittleEndian.Uint16(src[:2])
	pt := year / 100
	p1 = byte(year - 100*uint16(pt))
	p2, p3 = src[2], src[3]
	dst = append(dst,
		digits10[pt], digits01[pt],
		digits10[p1], digits01[p1], '-',
		digits10[p2], digits01[p2], '-',
		digits10[p3], digits01[p3],
	)
	if length == 10 {
		return dst, nil
	}
	if len(src) == 4 {
		return append(dst, zeroDateTime[10:length]...), nil
	}
	dst = append(dst, ' ')
	p1 = src[4] // hour
	src = src[5:]

	// p1 is 2-digit hour, src is after hour
	p2, p3 = src[0], src[1]
	dst = append(dst,
		digits10[p1], digits01[p1], ':',
		digits10[p2], digits01[p2], ':',
		digits10[p3], digits01[p3],
	)
	return appendMicrosecs(dst, src[2:], int(length)-20), nil
}

func formatBinaryTime(src []byte, length uint8) (driver.Value, error) {
	// length expects the deterministic length of the zero value,
	// negative time and 100+ hours are automatically added if needed
	if len(src) == 0 {
		return zeroDateTime[11 : 11+length], nil
	}
	var dst []byte // return value

	switch length {
	case
		8,                      // time (can be up to 10 when negative and 100+ hours)
		10, 11, 12, 13, 14, 15: // time with fractional seconds
	default:
		return nil, fmt.Errorf("illegal TIME length %d", length)
	}
	switch len(src) {
	case 8, 12:
	default:
		return nil, fmt.Errorf("invalid TIME packet length %d", len(src))
	}
	// +2 to enable negative time and 100+ hours
	dst = make([]byte, 0, length+2)
	if src[0] == 1 {
		dst = append(dst, '-')
	}
	days := binary.LittleEndian.Uint32(src[1:5])
	hours := int64(days)*24 + int64(src[5])

	if hours >= 100 {
		dst = strconv.AppendInt(dst, hours, 10)
	} else {
		dst = append(dst, digits10[hours], digits01[hours])
	}

	min, sec := src[6], src[7]
	dst = append(dst, ':',
		digits10[min], digits01[min], ':',
		digits10[sec], digits01[sec],
	)
	return appendMicrosecs(dst, src[8:], int(length)-9), nil
}

/******************************************************************************
*                       Convert from and to bytes                             *
******************************************************************************/

func uint64ToBytes(n uint64) []byte {
	return []byte{
		byte(n),
		byte(n >> 8),
		byte(n >> 16),
		byte(n >> 24),
		byte(n >> 32),
		byte(n >> 40),
		byte(n >> 48),
		byte(n >> 56),
	}
}

func uint64ToString(n uint64) []byte {
	var a [20]byte
	i := 20

	// U+0030 = 0
	// ...
	// U+0039 = 9

	var q uint64
	for n >= 10 {
		i--
		q = n / 10
		a[i] = uint8(n-q*10) + 0x30
		n = q
	}

	i--
	a[i] = uint8(n) + 0x30

	return a[i:]
}

// treats string value as unsigned integer representation
func stringToInt(b []byte) int {
	val := 0
	for i := range b {
		val *= 10
		val += int(b[i] - 0x30)
	}
	return val
}

// returns the string read as a bytes slice, wheter the value is NULL,
// the number of bytes read and an error, in case the string is longer than
// the input slice
func readLengthEncodedString(b []byte) ([]byte, bool, int, error) {
	// Get length
	num, isNull, n := readLengthEncodedInteger(b)
	if num < 1 {
		return b[n:n], isNull, n, nil
	}

	n += int(num)

	// Check data length
	if len(b) >= n {
		return b[n-int(num) : n : n], false, n, nil
	}
	return nil, false, n, io.EOF
}

// returns the number of bytes skipped and an error, in case the string is
// longer than the input slice
func skipLengthEncodedString(b []byte) (int, error) {
	// Get length
	num, _, n := readLengthEncodedInteger(b)
	if num < 1 {
		return n, nil
	}

	n += int(num)

	// Check data length
	if len(b) >= n {
		return n, nil
	}
	return n, io.EOF
}

// returns the number read, whether the value is NULL and the number of bytes read
func readLengthEncodedInteger(b []byte) (uint64, bool, int) {
	// See issue #349
	if len(b) == 0 {
		return 0, true, 1
	}

	switch b[0] {
	// 251: NULL
	case 0xfb:
		return 0, true, 1

	// 252: value of following 2
	case 0xfc:
		return uint64(b[1]) | uint64(b[2])<<8, false, 3

	// 253: value of following 3
	case 0xfd:
		return uint64(b[1]) | uint64(b[2])<<8 | uint64(b[3])<<16, false, 4

	// 254: value of following 8
	case 0xfe:
		return uint64(b[1]) | uint64(b[2])<<8 | uint64(b[3])<<16 |
				uint64(b[4])<<24 | uint64(b[5])<<32 | uint64(b[6])<<40 |
				uint64(b[7])<<48 | uint64(b[8])<<56,
			false, 9
	}

	// 0-250: value of first byte
	return uint64(b[0]), false, 1
}

// encodes a uint64 value and appends it to the given bytes slice
func appendLengthEncodedInteger(b []byte, n uint64) []byte {
	switch {
	case n <= 250:
		return append(b, byte(n))

	case n <= 0xffff:
		return append(b, 0xfc, byte(n), byte(n>>8))

	case n <= 0xffffff:
		return append(b, 0xfd, byte(n), byte(n>>8), byte(n>>16))
	}
	return append(b, 0xfe, byte(n), byte(n>>8), byte(n>>16), byte(n>>24),
		byte(n>>32), byte(n>>40), byte(n>>48), byte(n>>56))
}

// reserveBuffer checks cap(buf) and expand buffer to len(buf) + appendSize.
// If cap(buf) is not enough, reallocate new buffer.
func reserveBuffer(buf []byte, appendSize int) []byte {
	newSize := len(buf) + appendSize
	if cap(buf) < newSize {
		// Grow buffer exponentially
		newBuf := make([]byte, len(buf)*2+appendSize)
		copy(newBuf, buf)
		buf = newBuf
	}
	return buf[:newSize]
}

// escapeBytesBackslash escapes []byte with backslashes (\)
// This escapes the contents of a string (provided as []byte) by adding backslashes before special
// characters, and turning others into specific escape sequences, such as
// turning newlines into \n and null bytes into \0.
// https://github.com/mysql/mysql-server/blob/mysql-5.7.5/mysys/charset.c#L823-L932
func escapeBytesBackslash(buf, v []byte) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for _, c := range v {
		switch c {
		case '\x00':
			buf[pos] = '\\'
			buf[pos+1] = '0'
			pos += 2
		case '\n':
			buf[pos] = '\\'
			buf[pos+1] = 'n'
			pos += 2
		case '\r':
			buf[pos] = '\\'
			buf[pos+1] = 'r'
			pos += 2
		case '\x1a':
			buf[pos] = '\\'
			buf[pos+1] = 'Z'
			pos += 2
		case '\'':
			buf[pos] = '\\'
			buf[pos+1] = '\''
			pos += 2
		case '"':
			buf[pos] = '\\'
			buf[pos+1] = '"'
			pos += 2
		case '\\':
			buf[pos] = '\\'
			buf[pos+1] = '\\'
			pos += 2
		default:
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

// escapeStringBackslash is similar to escapeBytesBackslash but for string.
func escapeStringBackslash(buf []byte, v string) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for i := 0; i < len(v); i++ {
		c := v[i]
		switch c {
		case '\x00':
			buf[pos] = '\\'
			buf[pos+1] = '0'
			pos += 2
		case '\n':
			buf[pos] = '\\'
			buf[pos+1] = 'n'
			pos += 2
		case '\r':
			buf[pos] = '\\'
			buf[pos+1] = 'r'
			pos += 2
		case '\x1a':
			buf[pos] = '\\'
			buf[pos+1] = 'Z'
			pos += 2
		case '\'':
			buf[pos] = '\\'
			buf[pos+1] = '\''
			pos += 2
		case '"':
			buf[pos] = '\\'
			buf[pos+1] = '"'
			pos += 2
		case '\\':
			buf[pos] = '\\'
			buf[pos+1] = '\\'
			pos += 2
		default:
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

// escapeBytesQuotes escapes apostrophes in []byte by doubling them up.
// This escapes the contents of a string by doubling up any apostrophes that
// it contains. This is used when the NO_BACKSLASH_ESCAPES SQL_MODE is in
// effect on the server.
// https://github.com/mysql/mysql-server/blob/mysql-5.7.5/mysys/charset.c#L963-L1038
func escapeBytesQuotes(buf, v []byte) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for _, c := range v {
		if c == '\'' {
			buf[pos] = '\''
			buf[pos+1] = '\''
			pos += 2
		} else {
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

// escapeStringQuotes is similar to escapeBytesQuotes but for string.
func escapeStringQuotes(buf []byte, v string) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for i := 0; i < len(v); i++ {
		c := v[i]
		if c == '\'' {
			buf[pos] = '\''
			buf[pos+1] = '\''
			pos += 2
		} else {
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

/******************************************************************************
*                               Sync utils                                    *
******************************************************************************/

// noCopy may be embedded into structs which must not be copied
// after the first use.
//
// See https://github.com/golang/go/issues/8005#issuecomment-190753527
// for details.
type noCopy struct{}

// Lock is a no-op used by -copylocks checker from `go vet`.
func (*noCopy) Lock() {}

// Unlock is a no-op used by -copylocks checker from `go vet`.
// noCopy should implement sync.Locker from Go 1.11
// https://github.com/golang/go/commit/c2eba53e7f80df21d51285879d51ab81bcfbf6bc
// https://github.com/golang/go/issues/26165
func (*noCopy) Unlock() {}

// atomicBool is a wrapper around uint32 for usage as a boolean value with
// atomic access.
type atomicBool struct {
	_noCopy noCopy
	value   uint32
}

// IsSet returns whether the current boolean value is true
func (ab *atomicBool) IsSet() bool {
	return atomic.LoadUint32(&ab.value) > 0
}

// Set sets the value of the bool regardless of the previous value
func (ab *atomicBool) Set(value bool) {
	if value {
		atomic.StoreUint32(&ab.value, 1)
	} else {
		atomic.StoreUint32(&ab.value, 0)
	}
}

// TrySet sets the value of the bool and returns whether the value changed
func (ab *atomicBool) TrySet(value bool) bool {
	if value {
		return atomic.SwapUint32(&ab.value, 1) == 0
	}
	return atomic.SwapUint32(&ab.value, 0) > 0
}

// atomicError is a wrapper for atomically accessed error values
type atomicError struct {
	_noCopy noCopy
	value   atomic.Value
}

// Set sets the error value regardless of the previous value.
// The value must not be nil
func (ae *atomicError) Set(value error) {
	ae.value.Store(value)
}

// Value returns the current error value
func (ae *atomicError) Value() error {
	if v := ae.value.Load(); v != nil {
		// this will panic if the value doesn't implement the error interface
		return v.(error)
	}
	return nil
}

func namedValueToValue(named []driver.NamedValue) ([]driver.Value, error) {
	dargs := make([]driver.Value, len(named))
	for n, param := range named {
		if len(param.Name) > 0 {
			// TODO: support the use of Named Parameters #561
			return nil, errors.New("help: driver does not support the use of Named Parameters")
		}
		dargs[n] = param.Value
	}
	return dargs, nil
}

func mapIsolationLevel(level driver.IsolationLevel) (string, error) {
	switch sql.IsolationLevel(level) {
	case sql.LevelRepeatableRead:
		return "REPEATABLE READ", nil
	case sql.LevelReadCommitted:
		return "READ COMMITTED", nil
	case sql.LevelReadUncommitted:
		return "READ UNCOMMITTED", nil
	case sql.LevelSerializable:
		return "SERIALIZABLE", nil
	default:
		return "", fmt.Errorf("help: unsupported isolation level: %v", level)
	}
}

func connCheck(conn net.Conn) error {
	return nil
}

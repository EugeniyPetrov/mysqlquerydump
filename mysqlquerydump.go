package main

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/ini.v1"
	"io"
	"log"
	"math"
	"os"
	_ "reflect"
	"strconv"
	"strings"
)

type MysqlOptions struct {
	Host     string
	User     string
	Password string
	Database string
	Port     uint16
	Charset  string
}

func (options *MysqlOptions) Extend(extra *MysqlOptions) {
	if extra.Host != "" {
		options.Host = extra.Host
	}
	if extra.User != "" {
		options.User = extra.User
	}
	if extra.Password != "" {
		options.Password = extra.Password
	}
	if extra.Database != "" {
		options.Database = extra.Database
	}
	if extra.Port != 0 {
		options.Port = extra.Port
	}
	if extra.Charset != "" {
		options.Charset = extra.Charset
	}
}

func ParseOptionsFile(filename string) (*MysqlOptions, error) {
	options := MysqlOptions{}

	cfg, err := ini.Load(filename)
	if err != nil {
		return nil, err
	}

	section, err := cfg.GetSection("client")
	if err != nil {
		return nil, errors.New("Unable to parse " + filename)
	}

	optionsMap := section.KeysHash()

	options.Host = optionsMap["host"]
	options.User = optionsMap["user"]
	options.Password = optionsMap["password"]
	options.Database = optionsMap["database"]
	if optionsMap["port"] != "" {
		port64, err := strconv.ParseUint(optionsMap["port"], 10, 16)
		if err != nil {
			return nil, errors.New("Invalid port value in " + filename)
		}
		options.Port = uint16(port64)
	}

	return &options, nil
}

func failOnError(err error) {
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
}

func escapeString(bytes *[]byte) *[]byte {
	newBytes := make([]byte, len(*bytes)*2)
	i := 0
	for _, char := range *bytes {
		escape := true
		switch char {
		case 0:
			char = '0'
		case '\n':
			char = 'n'
		case '\r':
			char = 'r'
		case '\\':
			break
		case '\'':
			break
		case '"':
			break
		case '\032':
			char = 'Z'
		default:
			escape = false
		}

		if escape == true {
			newBytes[i] = '\\'
			i++
		}

		newBytes[i] = char
		i++
	}

	newBytes = newBytes[0:i]

	return &newBytes
}

func getDb(options *MysqlOptions) (*sql.DB, error) {
	config := mysql.Config{
		Net:    "tcp",
		Addr:   options.Host + ":" + strconv.Itoa(int(options.Port)),
		User:   options.User,
		Passwd: options.Password,
		DBName: options.Database,
		Params: map[string]string{
			"charset": options.Charset,
		},
	}

	db, err := sql.Open("mysql", config.FormatDSN())
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return db, err
	}

	return db, nil
}

func getDbOptions(host string, user string, database string, port uint16, configFile string) (*MysqlOptions, error) {
	options := &MysqlOptions{Host: "localhost", Port: 3306}

	myCnf := os.Getenv("HOME") + "/.my.cnf"
	if _, err := os.Stat(myCnf); !os.IsNotExist(err) {
		defaultOptions, err := ParseOptionsFile(myCnf)
		if err != nil {
			return nil, err
		}

		options.Extend(defaultOptions)
	}

	if configFile != "" {
		extraOptions, err := ParseOptionsFile(configFile)
		if err != nil {
			return nil, err
		}

		options.Extend(extraOptions)
	}

	// Only utf8 output may be produced at the moment, because escapeString only work well with utf8 and single-byte
	// encodings
	charset := "utf8"

	options.Extend(&MysqlOptions{
		Host:     host,
		User:     user,
		Database: database,
		Charset:  charset,
		Port:     port,
	})

	return options, nil
}

func outJson(out io.Writer, rows *sql.Rows) error {
	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	result := make([]interface{}, len(columns))
	dest := make([]interface{}, len(columns))

	for i, _ := range columns {
		dest[i] = &result[i]
	}

	mapped := make(map[string]interface{})
	for rows.Next() {
		err = rows.Scan(dest...)
		if err != nil {
			return err
		}

		for i, value := range result {
			switch value.(type) {
			case []byte:
				mapped[columns[i]] = string(value.([]byte))
			default:
				mapped[columns[i]] = value
			}
		}

		json, err := json.Marshal(mapped)
		if err != nil {
			return err
		}

		out.Write(json)
		out.Write([]byte{'\n'})
	}

	return nil
}

func outCsv(out io.Writer, rows *sql.Rows) error {
	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	result := make([]interface{}, len(columns))
	dest := make([]interface{}, len(columns))

	for i, _ := range columns {
		dest[i] = &result[i]
	}

	csvWriter := csv.NewWriter(out)
	for i := 0; rows.Next(); i++ {
		err = rows.Scan(dest...)
		if err != nil {
			return err
		}

		record := make([]string, len(columns))

		for i, value := range result {
			switch value.(type) {
			case []byte:
				record[i] = string(value.([]byte))
			case nil:
				record[i] = ""
			default:
				record[i] = fmt.Sprintf("%v", value)
			}
		}

		csvWriter.Write(record)
	}
	csvWriter.Flush()

	if err := csvWriter.Error(); err != nil {
		if err != nil {
			return err
		}
	}

	return nil
}

func outSql(
	out io.Writer,
	rows *sql.Rows,
	alias string,
	insertIgnore bool,
	onDuplicateKeyUpdate bool,
	batchSize int,
	options *MysqlOptions,
) error {
	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	result := make([]interface{}, len(columns))
	dest := make([]interface{}, len(columns))

	for i, _ := range columns {
		dest[i] = &result[i]
	}

	if alias == "" {
		return errors.New("Alias must be specified for sql format")
	}

	sqlBatchSize := int(math.Floor(1024 * float64(batchSize)))

	ignoreStatement := ""
	if insertIgnore == true {
		ignoreStatement = "IGNORE "
	}

	fields := "`" + strings.Join(columns, "`, `") + "`"
	insertHeader := "INSERT " + ignoreStatement + "INTO `" + alias + "` (" + fields + ") VALUES\n"

	onDuplicateStatement := "\nON DUPLICATE KEY UPDATE\n"
	for i, value := range columns {
		onDuplicateStatement += "`" + value + "` = VALUES(`" + value + "`)"
		if i < len(columns)-1 {
			onDuplicateStatement += ",\n"
		}
	}

	out.Write([]byte("SET NAMES " + options.Charset + ";\n\n"))

	var sqlBuffer bytes.Buffer

	printComa := false
	for i := 0; rows.Next(); i++ {
		err = rows.Scan(dest...)
		if err != nil {
			return err
		}

		if sqlBuffer.Len() == 0 {
			sqlBuffer.WriteString(insertHeader)
		}

		if printComa == true {
			sqlBuffer.WriteString(",\n")
		}

		sqlBuffer.WriteString("(")
		for i, value := range result {
			switch value.(type) {
			case []byte:
				valueBytes := value.([]byte)
				sqlBuffer.WriteString("'" + string(*escapeString(&valueBytes)) + "'")
			case nil:
				sqlBuffer.WriteString("NULL")
			default:
				sqlBuffer.WriteString(fmt.Sprintf("%v", value))
			}

			if i < len(columns)-1 {
				sqlBuffer.WriteString(", ")
			}
		}
		sqlBuffer.WriteString(")")

		if sqlBuffer.Len() >= sqlBatchSize {
			if onDuplicateKeyUpdate {
				sqlBuffer.WriteString(onDuplicateStatement)
			}
			sqlBuffer.WriteString(";\n")
			sqlBuffer.WriteTo(out)

			sqlBuffer.Truncate(0)
			printComa = false
		} else {
			printComa = true
		}
	}

	if sqlBuffer.Len() > 0 {
		if onDuplicateKeyUpdate {
			sqlBuffer.WriteString(onDuplicateStatement)
		}
		sqlBuffer.WriteString(";\n")
		sqlBuffer.WriteTo(out)
	}

	return nil
}

func main() {
	host := flag.String("h", "", "Connect to host.")
	user := flag.String("u", "", "User for login if not current user.")
	database := flag.String("D", "", "Database to use.")
	port64 := flag.Uint64("P", 0, "The TCP/IP port number to use for the connection.")
	query := flag.String("e", "", "The query to be processed. If not specified it will be given from standart input. It is recommended to use the command with outer sql-file.")
	format := flag.String("f", "csv", "Query output format. Possible values: csv, sql, json.")
	alias := flag.String("a", "", "MySQL table alias the result of a query will by written in. It is so pointless with the -f csv.")
	insertIgnore := flag.Bool("i", false, "Produce INSERT IGNORE output for sql dump.")
	onDuplicateKeyUpdate := flag.Bool("U", false, "Produce statement for update duplicate rows.")
	batchSize := flag.Int("s", 1024, "Batch size in kb")
	configFile := flag.String("c", "", "configuration ini file")

	flag.Parse()

	options, err := getDbOptions(
		*host,
		*user,
		*database,
		uint16(*port64),
		*configFile,
	)
	failOnError(err)

	db, err := getDb(options)
	failOnError(err)
	defer db.Close()

	stmt, err := db.Prepare(*query)
	failOnError(err)
	defer stmt.Close()

	rows, err := stmt.Query()
	failOnError(err)

	out := os.Stdout

	switch *format {
	case "json":
		err = outJson(out, rows)
	case "csv":
		err = outCsv(out, rows)
	case "sql":
		err = outSql(out, rows, *alias, *insertIgnore, *onDuplicateKeyUpdate, *batchSize, options)
	}

	failOnError(err)
}

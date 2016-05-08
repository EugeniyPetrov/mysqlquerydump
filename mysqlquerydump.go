package main

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/mkideal/cli"
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
	Timezone string
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
	if extra.Timezone != "" {
		options.Timezone = extra.Timezone
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

	out.Write([]byte("SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT;\n"))
	out.Write([]byte("SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS;\n"))
	out.Write([]byte("SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION;\n"))
	out.Write([]byte("SET NAMES " + options.Charset + ";\n"))
	//out.Write([]byte("SET @OLD_TIME_ZONE=@@TIME_ZONE;\n"))
	//out.Write([]byte("SET TIME_ZONE='+00:00';\n"))
	out.Write([]byte("\n"))

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

	out.Write([]byte("\n"))
	//out.Write([]byte("SET TIME_ZONE=@OLD_TIME_ZONE;\n"))
	out.Write([]byte("SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT;\n"))
	out.Write([]byte("SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS;\n"))
	out.Write([]byte("SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION;\n"))

	return nil
}

type mysqlquerydumpT struct {
	Help                 bool   `cli:"!help" usage:"display help information"`
	Host                 string `cli:"h,host" usage:"Connect to host."`
	User                 string `cli:"u,user" usage:"User for login."`
	Database             string `cli:"D,database" usage:"Database to use."`
	Port                 uint16 `cli:"P,port" usage:"The TCP/IP port number to use for the connection."`
	Query                string `cli:"q,query" usage:"The query to be processed. If not specified it will be given from standart input. It is recommended to use the command with outer sql-file."`
	Format               string `cli:"f,format" usage:"Query output format. Possible values: csv, sql, json."`
	Alias                string `cli:"a,alias" usage:"MySQL table alias the result of a query will by written in. It is so pointless with the -f csv."`
	InsertIgnore         bool   `cli:"i,insert-ignore" usage:"Produce INSERT IGNORE output for sql dump."`
	OnDuplicateKeyUpdate bool   `cli:"U,on-duplicate-key-update" usage:"Produce statement for update duplicate rows."`
	BatchSize            int    `cli:"s,batch-size" usage:"Batch size in kb"`
	ConfigFile           string `cli:"c,config-file"`
}

var app = &cli.Command{
	Name: os.Args[0],
	Desc: "mysqlquerydump - a program to dump query result in different formats",
	Argv: func() interface{} {
		return new(mysqlquerydumpT)
	},
	Fn: mysqlquerydump,
}

func mysqlquerydump(ctx *cli.Context) error {
	argv := ctx.Argv().(*mysqlquerydumpT)

	if argv.Help {
		ctx.WriteUsage()
		return nil
	}

	if argv.Query == "" {
		return errors.New("--query parameter is required.")
	}

	if argv.Format == "" {
		return errors.New("--format parameter is required.")
	}

	options, err := getDbOptions(
		argv.Host,
		argv.User,
		argv.Database,
		argv.Port,
		argv.ConfigFile,
	)

	if err != nil {
		return err
	}

	db, err := getDb(options)
	if err != nil {
		return err
	}
	defer db.Close()

	stmt, err := db.Prepare(argv.Query)
	if err != nil {
		return err
	}
	defer stmt.Close()

	rows, err := stmt.Query()
	if err != nil {
		return err
	}

	out := os.Stdout

	switch argv.Format {
	case "json":
		err = outJson(out, rows)
	case "csv":
		err = outCsv(out, rows)
	case "sql":
		err = outSql(out, rows, argv.Alias, argv.InsertIgnore, argv.OnDuplicateKeyUpdate, argv.BatchSize, options)
	default:
		return errors.New(fmt.Sprintf("Unknown format \"%s\"", argv.Format))
	}

	if err != nil {
		return err
	}

	return nil
}

// @todo read query from stdin
// @todo write man
// @todo set timezone
// @todo create table statement
func main() {
	cli.SetUsageStyle(cli.ManualStyle)

	if err := app.RunWith(os.Args[1:], os.Stderr, nil); err != nil {
		fmt.Printf("%v\n", err)
	}

	os.Exit(1)
}

package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/ini.v1"
	"log"
	"os"
	_ "reflect"
	"strconv"
)

var (
	err error
)

type MysqlOptions interface {
	Host() string
	User() string
	Password() string
	Database() string
	Port() uint16
	Charset() string
	Extend(MysqlOptions)
}

type mysqlOptions struct {
	host     string
	user     string
	password string
	database string
	port     uint16
	charset  string
}

func (options *mysqlOptions) Host() string {
	return options.host
}

func (options *mysqlOptions) User() string {
	return options.user
}

func (options *mysqlOptions) Password() string {
	return options.password
}

func (options *mysqlOptions) Database() string {
	return options.database
}

func (options *mysqlOptions) Port() uint16 {
	return options.port
}

func (options *mysqlOptions) Charset() string {
	return options.charset
}

func (options *mysqlOptions) Extend(extra MysqlOptions) {
	if extra.Host() != "" {
		options.host = extra.Host()
	}
	if extra.User() != "" {
		options.user = extra.User()
	}
	if extra.Password() != "" {
		options.password = extra.Password()
	}
	if extra.Database() != "" {
		options.database = extra.Database()
	}
	if extra.Port() != 0 {
		options.port = extra.Port()
	}
	if extra.Charset() != "" {
		options.charset = extra.Charset()
	}
}

func ParseOptions(filename string) (MysqlOptions, error) {
	options := mysqlOptions{}

	cfg, err := ini.Load(filename)
	if err != nil {
		return nil, err
	}

	section, err := cfg.GetSection("client")
	if err != nil {
		return nil, errors.New("Unable to parse " + filename)
	}

	optionsMap := section.KeysHash()

	options.host = optionsMap["host"]
	options.user = optionsMap["user"]
	options.password = optionsMap["password"]
	options.database = optionsMap["database"]
	if optionsMap["port"] != "" {
		port64, err := strconv.ParseUint(optionsMap["port"], 10, 16)
		if err != nil {
			return nil, errors.New("Invalid port value in " + filename)
		}
		options.port = uint16(port64)
	}

	return &options, nil
}

func checkErrors(err error) {
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
}

func main() {
	host := flag.String("h", "localhost", "Connect to host.")
	user := flag.String("u", "", "User for login if not current user.")
	database := flag.String("D", "", "Database to use.")
	charset := flag.String("C", "utf8", "Set the default character set.")
	port64 := flag.Uint64("P", 3306, "The TCP/IP port number to use for the connection.")
	query := flag.String("e", "", "The query to be processed. If not specified it will be given from standart input. It is recommended to use the command with outer sql-file.")
	format := flag.String("f", "csv", "Query output format. Possible values: csv, sql, json.")
	alias := flag.String("a", "", "MySQL table alias the result of a query will by written in. It is so pointless with the -f csv.")
	insertIgnore := flag.Bool("i", false, "Produce INSERT IGNORE output for sql dump.")
	onDuplicateKeyUpdate := flag.Bool("U", false, "Produce statement for update duplicate rows.")
	batchSize := flag.Int("s", 1, "Batch size in mb")
	configFile := flag.String("c", "", "configuration ini file")
	flag.Parse()

	options := mysqlOptions{host: "localhost", port: 3306}

	myCnf := os.Getenv("HOME") + "/.my.cnf"
	if _, err := os.Stat(myCnf); !os.IsNotExist(err) {
		defaultOptions, err := ParseOptions(myCnf)
		checkErrors(err)

		options.Extend(defaultOptions)
	}

	if *configFile != "" {
		extraOptions, err := ParseOptions(*configFile)
		checkErrors(err)

		options.Extend(extraOptions)
	}

	options.Extend(&mysqlOptions{
		host:     *host,
		user:     *user,
		database: *database,
		charset:  *charset,
		port:     uint16(*port64),
	})

	config := mysql.Config{
		Addr:   options.Host() + ":" + strconv.Itoa(int(options.Port())),
		User:   options.User(),
		Passwd: options.Password(),
		DBName: options.Database(),
		Params: map[string]string{
			"charset": options.Charset(),
		},
	}

	fmt.Println(config.FormatDSN())

	db, err := sql.Open("mysql", config.FormatDSN())
	checkErrors(err)

	err = db.Ping()
	checkErrors(err)

	defer db.Close()

	stmt, err := db.Prepare(*query)
	checkErrors(err)
	defer stmt.Close()

	rows, err := stmt.Query()
	checkErrors(err)

	columns, err := rows.Columns()
	checkErrors(err)

	result := make([]interface{}, len(columns))
	dest := make([]interface{}, len(columns))
	mapped := make(map[string]interface{})

	for i, v := range columns {
		dest[i] = &result[i]
		mapped[v] = nil
	}

	for rows.Next() {
		err = rows.Scan(dest...)
		checkErrors(err)

		for i, value := range result {
			switch value.(type) {
			case []byte:
				mapped[columns[i]] = string(value.([]byte))
			default:
				mapped[columns[i]] = value
			}
		}
		json, err := json.Marshal(mapped)
		checkErrors(err)

		fmt.Println(string(json))
	}

	_ = host
	_ = user
	_ = database
	_ = charset
	_ = query
	_ = format
	_ = alias
	_ = insertIgnore
	_ = onDuplicateKeyUpdate
	_ = batchSize
	_ = configFile
	_ = myCnf
}

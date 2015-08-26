package adaptor

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/pipe"

	"database/sql"
	_ "github.com/lib/pq"
)

// Postgres is an adaptor to read / write to postgres.
// it works as a source by copying files, and then optionally tailing the oplog
type Postgres struct {
	// pull these in from the node
	uri             string
	tail            bool   // run the tail oplog
	replicationSlot string // logical replication slot to use for changes
	debug           bool

	// save time by setting these once
	tableMatch *regexp.Regexp
	database   string

	latestLSN string

	//
	pipe *pipe.Pipe
	path string

	// postgres connection and options
	postgresSession *sql.DB
	oplogTimeout    time.Duration

	restartable bool // this refers to being able to refresh the iterator, not to the restart based on session op
}

// NewPostgres creates a new Postgres adaptor
func NewPostgres(p *pipe.Pipe, path string, extra Config) (StopStartListener, error) {
	var (
		conf PostgresConfig
		err  error
	)
	if err = extra.Construct(&conf); err != nil {
		return nil, err
	}

	if conf.URI == "" || conf.Namespace == "" {
		return nil, fmt.Errorf("both uri and namespace required, but missing ")
	}

	if conf.Debug {
		fmt.Printf("Postgres Config %+v\n", conf)
	}

	postgres := &Postgres{
		restartable:     true,            // assume for that we're able to restart the process
		oplogTimeout:    5 * time.Second, // timeout the oplog iterator
		pipe:            p,
		uri:             conf.URI,
		tail:            conf.Tail,
		replicationSlot: conf.ReplicationSlot,
		debug:           conf.Debug,
		path:            path,
	}

	postgres.database, postgres.tableMatch, err = extra.compileNamespace()
	if err != nil {
		return postgres, err
	}

	matchDbName := regexp.MustCompile(fmt.Sprintf("dbname=%v", postgres.database))
	if match := matchDbName.MatchString(postgres.uri); !match {
		return postgres, fmt.Errorf("Mismatch database name in YAML config and app javascript.  Postgres URI should, but does not contain dbname=%v", postgres.database)
	}

	postgres.postgresSession, err = sql.Open("postgres", postgres.uri)
	if err != nil {
		return postgres, fmt.Errorf("unable to parse uri (%s), %s\n", postgres.uri, err.Error())
	}

	return postgres, nil
}

// Start the adaptor as a source
func (postgres *Postgres) Start() (err error) {
	defer func() {
		postgres.pipe.Stop()
	}()

	if postgres.debug {
		fmt.Printf("Starting Postgres tail")
	}

	err = postgres.catData()
	if err != nil {
		postgres.pipe.Err <- err
		return err
	}
	if postgres.tail {
		// replay the oplog
		err = postgres.tailData()
		if err != nil {
			postgres.pipe.Err <- err
			return err
		}
	}

	return
}

// Listen starts the pipe's listener
func (postgres *Postgres) Listen() (err error) {
	defer func() {
		postgres.pipe.Stop()
	}()

	return postgres.pipe.Listen(postgres.writeMessage, postgres.tableMatch)
}

// Stop the adaptor
func (postgres *Postgres) Stop() error {
	postgres.pipe.Stop()

	return nil
}

// writeMessage writes one message to the destination Postgres, or sends an error down the pipe
// TODO this can be cleaned up.  I'm not sure whether this should pipe the error, or whether the
//   caller should pipe the error
func (postgres *Postgres) writeMessage(msg *message.Msg) (*message.Msg, error) {
	switch {
	case msg.Op == message.Insert:
		var (
			keys         []string
			placeholders []string
			data         []interface{}
		)

		i := 1
		for key, value := range msg.Map() {
			keys = append(keys, key)
			placeholders = append(placeholders, fmt.Sprintf("$%v", i))

			switch value.(type) {
			case map[string]interface{}:
				value, _ = json.Marshal(value)
			}
			data = append(data, value)

			i = i + 1
		}

		query := fmt.Sprintf("INSERT INTO %v (%v) VALUES (%v);", msg.Namespace, strings.Join(keys, ", "), strings.Join(placeholders, ", "))

		_, err := postgres.postgresSession.Exec(query, data...)

		if err != nil {
			fmt.Printf("Error INSERTING to Postgres with error (%v) on query (%v) with data (%v)\n", err, query, data)
			return msg, nil
		}
	case msg.Op == message.Update:
		fmt.Printf("Update Postgres %v values %v\n", msg.Namespace, msg.Data)
	case msg.Op == message.Delete:
		fmt.Printf("DELETE FROM Postgres %v values %v\n", msg.Namespace, msg.Data)
	case true:

	}

	return msg, nil
}

// catdata pulls down the original tables
func (postgres *Postgres) catData() (err error) {
	fmt.Println("Exporting data from matching tables:")
	tablesResult, err := postgres.postgresSession.Query("SELECT table_schema,table_name FROM information_schema.tables")
	if err != nil {
		return err
	}
	for tablesResult.Next() {
		var table_schema string
		var table_name string
		err = tablesResult.Scan(&table_schema, &table_name)

		err := postgres.catTable(table_schema, table_name)
		if err != nil {
			return err
		}
	}
	return
}

func (postgres *Postgres) catTable(table_schema string, table_name string) (err error) {
	// determine if table should be copied
	schemaAndTable := fmt.Sprintf("%v.%v", table_schema, table_name)
	if strings.HasPrefix(schemaAndTable, "information_schema.") || strings.HasPrefix(schemaAndTable, "pg_catalog.") {
		return
	} else if match := postgres.tableMatch.MatchString(schemaAndTable); !match {
		return
	}

	fmt.Printf("  exporting %v.%v\n", table_schema, table_name)

	// get columns for table
	columnsResult, err := postgres.postgresSession.Query(fmt.Sprintf("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '%v' AND table_schema = '%v'", table_name, table_schema))
	if err != nil {
		return err
	}
	var columns [][]string
	for columnsResult.Next() {
		var columnName string
		var columnType string

		err := columnsResult.Scan(&columnName, &columnType)
		if err != nil {
			return err
		}

		column := []string{columnName, columnType}
		columns = append(columns, column)
	}

	// build docs for table
	docsResult, err := postgres.postgresSession.Query(fmt.Sprintf("SELECT * FROM %v", schemaAndTable))
	if err != nil {
		return err
	}

	for docsResult.Next() {
		dest := make([]interface{}, len(columns))
		for i, _ := range columns {
			dest[i] = make([]byte, 30)
			dest[i] = &dest[i]
		}

		var docMap map[string]interface{}
		err = docsResult.Scan(dest...)
		if err != nil {
			fmt.Println("Failed to scan row", err)
			return err
		}

		docMap = make(map[string]interface{})

		for i, value := range dest {
			switch value := value.(type) {
			default:
				docMap[columns[i][0]] = value
			case []uint8:
				docMap[columns[i][0]] = string(value)
			}
		}

		msg := message.NewMsg(message.Insert, docMap, schemaAndTable)
		postgres.pipe.Send(msg)
	}

	return
}

// tail the logical data
func (postgres *Postgres) tailData() (err error) {
	fmt.Printf("Listening for changes on logical decoding slot '%v'\n", postgres.replicationSlot)
	for {
		dataMatcher := regexp.MustCompile("^table ([^\\.]+).([^\\.]+): (INSERT|DELETE|UPDATE): (.+)$") // 1 - schema, 2 - table, 3 - action, 4 - remaining

		changesResult, err := postgres.postgresSession.Query(fmt.Sprintf("SELECT * FROM pg_logical_slot_get_changes('%v', NULL, NULL);", postgres.replicationSlot))
		if err != nil {
			return err
		}
		for changesResult.Next() {
			var (
				location string
				xid      string
				data     string
			)

			err = changesResult.Scan(&location, &xid, &data)
			if err != nil {
				return err
			}

			// Ensure we are getting a data change row
			dataMatches := dataMatcher.FindStringSubmatch(data)
			if len(dataMatches) == 0 {
				continue
			}

			// Skippable because no pimary key on record
			// Make sure we are getting changes on valid tables
			schemaAndTable := fmt.Sprintf("%v.%v", dataMatches[1], dataMatches[2])
			if match := postgres.tableMatch.MatchString(schemaAndTable); !match {
				continue
			}

			if dataMatches[4] == "(no-tuple-data)" {
				fmt.Printf("No tuple data for action %v on %v.%v\n", dataMatches[3], dataMatches[1], dataMatches[2])
				continue
			}

			// normalize the action
			var action message.OpType
			switch {
			case dataMatches[3] == "INSERT":
				action = message.Insert
			case dataMatches[3] == "DELETE":
				action = message.Delete
			case dataMatches[3] == "UPDATE":
				action = message.Update
			case true:
				return fmt.Errorf("Error processing action from string: %v", data)
			}

			fmt.Printf("%v on %v.%v\n", dataMatches[3], dataMatches[1], dataMatches[2])

			docMap, err := postgres.ParseLogicalDecodingData(dataMatches[4])
			if err != nil {
				return err
			}

			msg := message.NewMsg(action, docMap, schemaAndTable)
			postgres.pipe.Send(msg)
		}

		time.Sleep(3 * time.Second)
	}
	return
}

// PostgresConfig provides configuration options for a postgres adaptor
// the notable difference between this and dbConfig is the presence of the Tail option
type PostgresConfig struct {
	URI             string `json:"uri" doc:"the uri to connect to, in the form 'user=my-user password=my-password dbname=dbname sslmode=require'"`
	Namespace       string `json:"namespace" doc:"mongo namespace to read/write"`
	Timeout         string `json:timeout" doc:"timeout for establishing connection, format must be parsable by time.ParseDuration and defaults to 10s"`
	Debug           bool   `json:"debug" doc:"display debug information"`
	Tail            bool   `json:"tail" doc:"if tail is true, then the postgres source will tail the oplog after copying the namespace"`
	ReplicationSlot string `json:"replication_slot" doc:"required if tail is true; sets the replication slot to use for logical decoding"`
	Wc              int    `json:"wc" doc:"The write concern to use for writes, Int, indicating the minimum number of servers to write to before returning success/failure"`
	FSync           bool   `json:"fsync" doc:"When writing, should we flush to disk before returning success"`
	Bulk            bool   `json:"bulk" doc:"use a buffer to bulk insert documents"`
}

// find the size of a document in bytes
func rowSize(ops interface{}) (int, error) {
	fmt.Println("Row size is unimplemented")
	//b, err := bson.Marshal(ops)
	//if err != nil {
	//return 0, err
	//}
	//return len(b), nil
	return 3, nil
}

func (postgres *Postgres) ParseLogicalDecodingData(data string) (docMap map[string]interface{}, err error) {
	docMap = make(map[string]interface{})

	var (
		label                  string
		labelFinished          bool
		valueType              string
		valueTypeFinished      bool
		openBracketInValueType bool
		skippedColon           bool
		value                  string // will type switch later
		valueEndCharacter      string
		defferedSingleQuote    bool
		valueFinished          bool
	)

	valueTypeFinished = false
	labelFinished = false
	skippedColon = false
	defferedSingleQuote = false
	openBracketInValueType = false
	valueFinished = false

	for _, character := range data {
		if !labelFinished {
			if string(character) == "[" {
				labelFinished = true
				continue
			}
			label = fmt.Sprintf("%v%v", label, string(character))
			continue
		}

		if !valueTypeFinished {
			if openBracketInValueType && string(character) == "]" { // if a bracket is open, close it
				openBracketInValueType = false
			} else if string(character) == "]" { // if a bracket is not open, finish valueType
				valueTypeFinished = true
				continue
			} else if string(character) == "[" {
				openBracketInValueType = true
			}
			valueType = fmt.Sprintf("%v%v", valueType, string(character))
			continue
		}

		if !skippedColon && string(character) == ":" {
			skippedColon = true
			continue
		}

		if len(valueEndCharacter) == 0 {
			if string(character) == "'" {
				valueEndCharacter = "'"
				continue
			}

			valueEndCharacter = " "
		}

		// ending with '
		if defferedSingleQuote && string(character) == " " { // we hit an unescaped single quote
			valueFinished = true
		} else if defferedSingleQuote && string(character) == "'" { // we hit an escaped single quote ''
			defferedSingleQuote = false
		} else if string(character) == "'" && !defferedSingleQuote { // we hit a first single quote
			defferedSingleQuote = true
			continue
		}

		// ending with space
		if valueEndCharacter == " " && string(character) == valueEndCharacter {
			valueFinished = true
		}

		// continue parsing
		if !valueFinished {
			value = fmt.Sprintf("%v%v", value, string(character))
			continue
		}

		// Set and reset
		docMap[label] = casifyValue(value, valueType)

		label = ""
		labelFinished = false
		valueType = ""
		valueTypeFinished = false
		skippedColon = false
		defferedSingleQuote = false
		value = ""
		valueEndCharacter = ""
		valueFinished = false
	}
	if len(label) > 0 { // ensure we process any line ending abruptly
		docMap[label] = casifyValue(value, valueType)
	}
	return
}

func casifyValue(value string, valueType string) interface{} {
	switch {
	case value == "null":
		return nil
	case valueType == "integer":
		i, _ := strconv.Atoi(value)
		return i
	case valueType == "double precision":
		f, _ := strconv.ParseFloat(value, 64)
		return f
	case valueType == "jsonb[]":
		var m map[string]interface{}
		json.Unmarshal([]byte(value), &m)
		return m
	case valueType == "timestamp without time zone":
		// parse time like 2015-08-21 16:09:02.988058
		t, err := time.Parse("2006-01-02 15:04:05.9", value)
		if err != nil {
			fmt.Printf("\nTime (%v) parse error: %v\n\n", value, err)
		}
		return t
	}

	return value
}

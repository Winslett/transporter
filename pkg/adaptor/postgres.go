package adaptor

import (
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/pipe"

	"database/sql"
	"github.com/lib/pq"
)

// Postgres is an adaptor to read / write to postgres.
// it works as a source by copying files, and then optionally tailing the oplog
type Postgres struct {
	// pull these in from the node
	uri   string
	tail  bool // run the tail oplog
	debug bool

	// save time by setting these once
	tableMatch *regexp.Regexp
	database   string

	latestLSN string

	//
	pipe *pipe.Pipe
	path string
	pg   *pq.Dialer

	// postgres connection and options
	postgresSession *sql.DB
	oplogTimeout    time.Duration

	// a buffer to hold documents
	buffLock         sync.Mutex
	opsBufferCount   int
	opsBuffer        map[string][]interface{}
	opsBufferSize    int
	bulkWriteChannel chan *SyncRow
	bulkQuitChannel  chan chan bool
	bulk             bool

	restartable bool // this refers to being able to refresh the iterator, not to the restart based on session op
}

type SyncRow struct {
	Doc        map[string]interface{}
	Collection string
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
		restartable:      true,            // assume for that we're able to restart the process
		oplogTimeout:     5 * time.Second, // timeout the oplog iterator
		pipe:             p,
		uri:              conf.URI,
		tail:             conf.Tail,
		debug:            conf.Debug,
		path:             path,
		opsBuffer:        make(map[string][]interface{}),
		bulkWriteChannel: make(chan *SyncRow),
		bulkQuitChannel:  make(chan chan bool),
		bulk:             conf.Bulk,
	}
	// opsBuffer:        make([]*SyncRow, 0, MONGO_BUFFER_LEN),

	postgres.database, postgres.tableMatch, err = extra.compileNamespace()
	if err != nil {
		return postgres, err
	}

	postgres.postgresSession, err = sql.Open("postgres", postgres.uri)
	if err != nil {
		return postgres, fmt.Errorf("unable to parse uri (%s), %s\n", postgres.uri, err.Error())
	}

	if postgres.tail {
		fmt.Println("Have not implemented Postgres tailing yet.")
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

	if postgres.bulk {
		go postgres.bulkWriter()
	}
	return postgres.pipe.Listen(postgres.writeMessage, postgres.tableMatch)
}

// Stop the adaptor
func (postgres *Postgres) Stop() error {
	postgres.pipe.Stop()

	// if we're bulk writing, ask our writer to exit here
	if postgres.bulk {
		q := make(chan bool)
		postgres.bulkQuitChannel <- q
		<-q
	}

	return nil
}

// writeMessage writes one message to the destination Postgres, or sends an error down the pipe
// TODO this can be cleaned up.  I'm not sure whether this should pipe the error, or whether the
//   caller should pipe the error
func (postgres *Postgres) writeMessage(msg *message.Msg) (*message.Msg, error) {
	//_, msgColl, err := msg.SplitNamespace()
	//if err != nil {
	//postgres.pipe.Err <- NewError(ERROR, postgres.path, fmt.Sprintf("postgres error (msg namespace improperly formatted, must be database.collection, got %s)", msg.Namespace), msg.Data)
	//return msg, nil
	//}

	fmt.Println("Run query with %v", msg)
	//collection := postgres.postgresSession.DB(postgres.database).C(msgColl)

	//if !msg.IsMap() {
	//postgres.pipe.Err <- NewError(ERROR, postgres.path, fmt.Sprintf("postgres error (document must be a bson document, got %T instead)", msg.Data), msg.Data)
	//return msg, nil
	//}

	//doc := &SyncRow{
	//Doc:        msg.Map(),
	//Collection: msgColl,
	//}

	//if postgres.bulk {
	//postgres.bulkWriteChannel <- doc
	//} else if msg.Op == message.Delete {
	//err := collection.Remove(doc.Doc)
	//if err != nil {
	//postgres.pipe.Err <- NewError(ERROR, postgres.path, fmt.Sprintf("postgres error removing (%s)", err.Error()), msg.Data)
	//}
	//} else {
	//err := collection.Insert(doc.Doc)
	//if err != nil {
	//postgres.pipe.Err <- NewError(ERROR, postgres.path, fmt.Sprintf("postgres error (%s)", err.Error()), msg.Data)
	//}
	//}

	return msg, nil
}

func (postgres *Postgres) bulkWriter() {

	for {
		select {
		case doc := <-postgres.bulkWriteChannel:
			sz, err := docSize(doc.Doc)
			if err != nil {
				postgres.pipe.Err <- NewError(ERROR, postgres.path, fmt.Sprintf("postgres error (%s)", err.Error()), doc)
				break
			}

			if ((sz + postgres.opsBufferSize) > MONGO_BUFFER_SIZE) || (postgres.opsBufferCount == MONGO_BUFFER_LEN) {
				postgres.writeBuffer() // send it off to be inserted
			}

			postgres.buffLock.Lock()
			postgres.opsBufferCount += 1
			postgres.opsBuffer[doc.Collection] = append(postgres.opsBuffer[doc.Collection], doc.Doc)
			postgres.opsBufferSize += sz
			postgres.buffLock.Unlock()
		case <-time.After(2 * time.Second):
			postgres.writeBuffer()
		case q := <-postgres.bulkQuitChannel:
			postgres.writeBuffer()
			q <- true
		}
	}
}

func (postgres *Postgres) writeBuffer() {
	fmt.Println("Write buffer is unimplemented")
	//postgres.buffLock.Lock()
	//defer postgres.buffLock.Unlock()
	//for coll, docs := range postgres.opsBuffer {

	//collection := postgres.mongoSession.DB(postgres.database).C(coll)
	//if len(docs) == 0 {
	//continue
	//}

	//err := collection.Insert(docs...)

	//if err != nil {
	//postgres.pipe.Err <- NewError(ERROR, postgres.path, fmt.Sprintf("postgres error (%s)", err.Error()), docs[0])
	//}

	//}

	//postgres.opsBufferCount = 0
	//postgres.opsBuffer = make(map[string][]interface{})
	//postgres.opsBufferSize = 0
}

// catdata pulls down the original tables
func (postgres *Postgres) catData() (err error) {
	fmt.Println("Query for tables in database.")
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

	// get columns for table
	columnsResult, err := postgres.postgresSession.Query(fmt.Sprintf("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '%v'", table_name))
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

//var (
//query  = bson.M{}
//result bson.M // hold the document
//)

//iter := postgres.mongoSession.DB(postgres.database).C(collection).Find(query).Sort("_id").Iter()

//for {
//for iter.Next(&result) {
//if stop := postgres.pipe.Stopped; stop {
//return
//}

//// set up the message
//msg := message.NewMsg(message.Insert, result, postgres.computeNamespace(collection))

//postgres.pipe.Send(msg)
//result = bson.M{}
//}

//// we've exited the mongo read loop, lets figure out why
//// check here again if we've been asked to quit
//if stop := postgres.pipe.Stopped; stop {
//return
//}

//if iter.Err() != nil && postgres.restartable {
//fmt.Printf("got err reading collection. reissuing query %v\n", iter.Err())
//time.Sleep(1 * time.Second)
//iter = postgres.mongoSession.DB(postgres.database).C(collection).Find(query).Sort("_id").Iter()
//continue
//}
//break
//}

/*
 * tail the logical data
 */
func (postgres *Postgres) tailData() (err error) {
	fmt.Println("Tailing data is unimplemented.")
	return

	//var (
	//collection = postgres.mongoSession.DB("local").C("oplog.rs")
	//result     oplogDoc // hold the document
	//query      = bson.M{
	//"ts": bson.M{"$gte": postgres.oplogTime},
	//}

	//iter = collection.Find(query).LogReplay().Sort("$natural").Tail(postgres.oplogTimeout)
	//)

	//for {
	//for iter.Next(&result) {
	//if stop := postgres.pipe.Stopped; stop {
	//return
	//}
	//if result.validOp() {
	//_, coll, _ := postgres.splitNamespace(result.Ns)

	//if strings.HasPrefix(coll, "systepostgres.") {
	//continue
	//} else if match := postgres.tableMatch.MatchString(coll); !match {
	//continue
	//}

	//var doc bson.M
	//switch result.Op {
	//case "i":
	//doc = result.O
	//case "d":
	//doc = result.O
	//case "u":
	//doc, err = postgres.getOriginalDoc(result.O2, coll)
	//if err != nil { // errors aren't fatal here, but we need to send it down the pipe
	//postgres.pipe.Err <- NewError(ERROR, postgres.path, fmt.Sprintf("Postgres error (%s)", err.Error()), nil)
	//continue
	//}
	//default:
	//postgres.pipe.Err <- NewError(ERROR, postgres.path, "Postgres error (unknown op type)", nil)
	//continue
	//}

	//msg := message.NewMsg(message.OpTypeFromString(result.Op), doc, postgres.computeNamespace(coll))
	//msg.Timestamp = int64(result.Ts) >> 32

	//postgres.oplogTime = result.Ts
	//postgres.pipe.Send(msg)
	//}
	//result = oplogDoc{}
	//}

	//// we've exited the mongo read loop, lets figure out why
	//// check here again if we've been asked to quit
	//if stop := postgres.pipe.Stopped; stop {
	//return
	//}
	//if iter.Timeout() {
	//continue
	//}
	//if iter.Err() != nil {
	//return NewError(CRITICAL, postgres.path, fmt.Sprintf("Postgres error (error reading collection %s)", iter.Err()), nil)
	//}

	//// query will change,
	//query = bson.M{
	//"ts": bson.M{"$gte": postgres.oplogTime},
	//}
	//iter = collection.Find(query).LogReplay().Tail(postgres.oplogTimeout)
	//}
}

// getOriginalDoc retrieves the original document from the database.  transport has no knowledge of update operations, all updates
// work as wholesale document replaces
func (postgres *Postgres) getOriginalRow(doc string, collection string) (result string, err error) {
	fmt.Println("Query and return original row")
	//id, exists := doc["_id"]
	//if !exists {
	//return result, fmt.Errorf("can't get _id from document")
	//}

	//err = postgres.mongoSession.DB(postgres.database).C(collection).FindId(id).One(&result)
	//if err != nil {
	//err = fmt.Errorf("%s.%s %v %v", postgres.database, collection, id, err)
	//}
	return
}

func (postgres *Postgres) computeNamespace(collection string) string {
	return strings.Join([]string{postgres.database, collection}, ".")
}

// splitNamespace split's a mongo namespace by the first '.' into a database and a collection
func (postgres *Postgres) splitNamespace(namespace string) (string, string, error) {
	fields := strings.SplitN(namespace, ".", 2)

	if len(fields) != 2 {
		return "", "", fmt.Errorf("malformed mongo namespace")
	}
	return fields[0], fields[1], nil
}

// logicalDoc are representations of the postgres logical decoding document
// detailed here: http://www.postgresql.org/docs/9.4/static/logicaldecoding-example.html
type logicalDoc struct {
	Lsn  string
	Xid  string
	Data string
	Op   string
}

// validOp checks to see if we're an insert, delete, or update, otherwise the
// document is skilled.
// TODO: skip system collections
func (l *logicalDoc) validOp() bool {
	return l.Op == "i" || l.Op == "d" || l.Op == "u"
}

// PostgresConfig provides configuration options for a postgres adaptor
// the notable difference between this and dbConfig is the presence of the Tail option
type PostgresConfig struct {
	URI       string `json:"uri" doc:"the uri to connect to, in the form 'user=my-user password=my-password dbname=dbname sslmode=require'"`
	Namespace string `json:"namespace" doc:"mongo namespace to read/write"`
	Timeout   string `json:timeout" doc:"timeout for establishing connection, format must be parsable by time.ParseDuration and defaults to 10s"`
	Debug     bool   `json:"debug" doc:"display debug information"`
	Tail      bool   `json:"tail" doc:"if tail is true, then the postgres source will tail the oplog after copying the namespace"`
	Wc        int    `json:"wc" doc:"The write concern to use for writes, Int, indicating the minimum number of servers to write to before returning success/failure"`
	FSync     bool   `json:"fsync" doc:"When writing, should we flush to disk before returning success"`
	Bulk      bool   `json:"bulk" doc:"use a buffer to bulk insert documents"`
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

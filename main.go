package main

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"time"

	_ "net/http/pprof"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"golang.org/x/exp/rand"
)

const tableName = "persons"

var (
	schema = arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		},
		nil,
	)
)

func connectToDuckDB(ctx context.Context) (adbc.Connection, adbc.Database) {
	var drv drivermgr.Driver
	db, err := drv.NewDatabase(map[string]string{
		"driver":     "duckdb",
		"entrypoint": "duckdb_adbc_init",
		"path":       "/tmp/a.db",
	})

	if err != nil {
		log.Println("failed to open database")
	}

	// After initializing the database, we must create and initialize a connection to it.
	cnxn, err := db.Open(ctx)
	if err != nil {
		log.Println("failed to open connection")
	}
	return cnxn, db
}

func printMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	//fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	// fmt.Printf("\tHeapAlloc = %v MiB", bToMb(m.HeapAlloc))
	// fmt.Printf("\tHeapSys = %v MiB", bToMb(m.HeapSys))
	fmt.Printf("\tHeapObjects = %v\n", m.HeapObjects)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

func buildSampleArrowRecord(numRows int) arrow.Record {
	// Create an arrow data that needs to be ingested to DuckDB
	pool := memory.DefaultAllocator

	// Create builders for each field
	idBuilder := array.NewInt32Builder(pool)
	defer idBuilder.Release()

	for i := 0; i < numRows; i++ {
		randomID := rand.Int31n(1000) // Generate a random integer between 0 and 999
		idBuilder.Append(randomID)
	}

	idArray := idBuilder.NewArray()
	defer idArray.Release()

	columns := []arrow.Array{idArray}
	record := array.NewRecord(schema, columns, int64(numRows))

	return record
}

func ingestArrowBatch(ctx context.Context, numRowsinBatch int, st adbc.Statement, append bool) {
	// create batch of Arrow record
	sampleArrowRecord := buildSampleArrowRecord(numRowsinBatch)
	defer sampleArrowRecord.Release()

	// Bind uses an arrow record batch to bind parameters to the query
	err := st.Bind(ctx, sampleArrowRecord)
	if err != nil {
		panic(err)
	}

	// Set an option with the table name we want to insert to
	err = st.SetOption(adbc.OptionKeyIngestTargetTable, tableName)
	if err != nil {
		panic(err)
	}

	if append {
		err = st.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeAppend)
		if err != nil {
			panic(err)
		}
	}

	// ExecuteUpdate executes a statement that does not generate a result
	// set. It returns the number of rows affected if known, otherwise -1.
	_, err = st.ExecuteUpdate(ctx)
	if err != nil {
		log.Println("failed to set query")
	}

	fmt.Printf("TotalRows = %s\t", getRowsInDuckDB(ctx, st))
	printMemUsage()
}

func getRowsInDuckDB(ctx context.Context, st adbc.Statement) string {
	// Check if it has been deleted
	err := st.SetSqlQuery(`SELECT COUNT(*) FROM "persons"`)
	if err != nil {
		log.Println("failed to execute query")
	}

	rdr, _, err := st.ExecuteQuery(ctx)
	if err != nil {
		log.Println("failed to execute query")
	}
	defer rdr.Release()

	// Print arrow formatted query result
	result := "0"
	for rdr.Next() {
		rec := rdr.Record()
		result = rec.Columns()[0].ValueStr(0)
		//rec.Release()
	}
	return result
}

func deleteFromDuckDB(st adbc.Statement, ctx context.Context) {
	err := st.SetSqlQuery(`DELETE FROM "persons"`)
	if err != nil {
		log.Println("failed to set query")
	}
	_, err = st.ExecuteUpdate(ctx)
	if err != nil {
		log.Println("failed to execute query")
	}
}

func vacuumDB(st adbc.Statement, ctx context.Context) {
	// Query the data from table
	_ = st.SetSqlQuery(`VACUUM;`)
	_, err := st.ExecuteUpdate(ctx)
	if err != nil {
		log.Println("failed to vacuum")
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup connection to duckdb database
	conn, db := connectToDuckDB(ctx)
	defer db.Close()
	defer conn.Close()

	// initialize our statement
	st, err := conn.NewStatement()
	if err != nil {
		log.Println("failed to create statement")
	}
	defer st.Close()

	printMemUsage()
	numRowsinBatch := 10000

	// Ingest Batch of Arrow data into DuckDB table
	appendFalse := false
	ingestArrowBatch(ctx, numRowsinBatch, st, appendFalse)

	for {
		deleteFromDuckDB(st, ctx)
		time.Sleep(10 * time.Millisecond)

		// Run garbage Collection
		// runtime.GC()

		// Run vacuum command
		// vacuumDB(st, ctx)

		// Ingest New Arrow Data of same size
		append := true
		ingestArrowBatch(ctx, numRowsinBatch, st, append)
		time.Sleep(10 * time.Millisecond)
	}
}

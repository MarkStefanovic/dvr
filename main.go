package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/kjk/dailyrotate"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Dependency struct {
	Schema string
	Table  string
	Field  string
	Load   pgtype.Timestamptz
	Seen   pgtype.Timestamptz
	Stale  bool
}

func (d Dependency) FullName() string {
	return fmt.Sprintf("%s.%s.%s", d.Schema, d.Table, d.Field)
}

func init() {
	log.SetOutput(os.Stdout)
}

func main() {
	fh, err := openLogFile()
	if err != nil {
		log.Fatalf("openLogFile failed: %v", err)
	}
	defer func(fh *dailyrotate.File) {
		err := fh.Close()
		if err != nil {
			log.Fatalf("An error occurred while closing the log file: %v", err)
		}
	}(fh)

	log.SetOutput(fh)

	schemaPtr := flag.String("schema", "", "Schema of table to refresh")
	tablePtr := flag.String("table", "", "Name of table to refresh")
	tsFieldPtr := flag.String("ts", "", "Comma-seperated list of timestamp fields")
	spNamePtr := flag.String("sp", "", "Name of the stored procedure used to refresh the table (defaults to refresh_<table_name>)")
	timeoutSecondsPtr := flag.Int("timeout", 3600, "Maximum seconds to wait for stored procedure to finish")
	flag.Parse()

	log.Printf(
		"Running refresh with the following parameters: schema = %s, table = %s, tsFields = %s, sp = %s, tiemout = %d",
		*schemaPtr, *tablePtr, *tsFieldPtr, *spNamePtr, *timeoutSecondsPtr,
	)

	if *schemaPtr == "" {
		log.Fatalf("No schema was provided.")
	}

	if *tablePtr == "" {
		log.Fatalf("No table was provided.")
	}

	if *tsFieldPtr == "" {
		log.Fatalf("No timestamp fields were provided.")
	}

	var spName string
	if *spNamePtr == "" {
		spName = fmt.Sprintf("refresh_%s", *tablePtr)
	} else {
		spName = *spNamePtr
	}

	config := getConfig()

	con, err := pgx.Connect(context.Background(), config["connection_string"].(string))
	if err != nil {
		log.Fatalf("Unable to connect to database: %v", err)
	}
	defer func(conn *pgx.Conn, ctx context.Context) {
		err := conn.Close(ctx)
		if err != nil {
			log.Fatalf("An error occurred while closing the database: %v", err)
		}
		log.Println("Connection closed.")
	}(con, context.Background())

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Duration(*timeoutSecondsPtr)*time.Second)
	defer cancel()

	err = setStatus(ctx, con, *schemaPtr, spName, "running")
	if err != nil {
		logError(
			ctx, con, *schemaPtr, spName,
			fmt.Sprintf("An error occurred while setting the status to idle: %v", err),
		)
	}

	dependencies, err := getDependenciesForTable(ctx, con, *schemaPtr, *tablePtr)
	if err != nil {
		logError(
			ctx, con, *schemaPtr, spName,
			fmt.Sprintf("An error occurred while fetching the dependencies for table, %s: %v", *tablePtr, err),
		)
	}

	dependenciesReadyToUpdate := map[string]bool{}
	for _, dependency := range dependencies {
		if dependency.Stale {
			dependenciesReadyToUpdate[fmt.Sprintf("%s.%s", dependency.Schema, dependency.Table)] = true
		}
	}

	tsFields := strings.Split(*tsFieldPtr, ",")

	if len(dependenciesReadyToUpdate) == 0 {
		log.Println("There were no dependencies ready to update.")

		err := logSkip(ctx, con, *schemaPtr, spName)
		if err != nil {
			logError(
				ctx, con, *schemaPtr, spName,
				fmt.Sprintf("An error occurred while logging skip for %s.%s: %v", *schemaPtr, spName, err),
			)
		}
	} else {
		log.Println("The following dependencies are ready to be updated:")
		for dependencyName := range dependenciesReadyToUpdate {
			log.Printf("- %s", dependencyName)
		}

		seenTimestamps := map[string]pgtype.Timestamptz{}
		for _, dependency := range dependencies {
			ts, err := getLoad(ctx, con, dependency.Schema, dependency.Table, dependency.Field)
			if err != nil {
				logError(
					ctx, con, *schemaPtr, spName,
					fmt.Sprintf("An error occurred while getting the initial load timestamp for %s: %v", dependency.FullName(), err),
				)
			}
			seenTimestamps[dependency.FullName()] = ts
		}

		err = runStoredProcedure(
			ctx,
			con,
			*schemaPtr,
			spName,
			dependencies,
		)
		if err != nil {
			logError(
				ctx, con, *schemaPtr, spName,
				fmt.Sprintf("An error occurred while running %s.%s: %v", *schemaPtr, spName, err),
			)
		}

		for _, tsField := range tsFields {
			ts, err := getMaxTs(ctx, con, *schemaPtr, *tablePtr, tsField)
			if err != nil {
				logError(
					ctx, con, *schemaPtr, spName,
					fmt.Sprintf("An error occurred while getting the maximum timestamp for %s.%s.%s: %v", *schemaPtr, *tablePtr, tsField, err),
				)
			}

			err = setLoad(ctx, con, *schemaPtr, *tablePtr, tsField, ts)
			if err != nil {
				logError(
					ctx, con, *schemaPtr, spName,
					fmt.Sprintf("An error occurred while setting the load timestamp for %s.%s.%s: %v", *schemaPtr, *tablePtr, tsField, err),
				)
			}
		}

		for _, dependency := range dependencies {
			err := setSeen(ctx, con, *schemaPtr, *tablePtr, dependency.Schema, dependency.Table, dependency.Field, seenTimestamps[dependency.FullName()])
			if err != nil {
				logError(
					ctx, con, *schemaPtr, spName,
					fmt.Sprintf("An error occurred while setting the seen timestamp for %s: %v", dependency.FullName(), err),
				)
			}
		}
	}

	err = setStatus(ctx, con, *schemaPtr, spName, "idle")
	if err != nil {
		logError(
			ctx, con, *schemaPtr, spName,
			fmt.Sprintf("An error occurred while setting the status to idle: %v", err),
		)
	}
}

func getConfig() map[string]interface{} {
	jsonFile, err := os.Open("config.json")
	if err != nil {
		log.Fatalf("An error occurred while opening config.json: %v", err)
	}
	defer func(jsonFile *os.File) {
		err := jsonFile.Close()
		if err != nil {
			log.Fatalf("An error occurred while closing config.json: %v", err)
		}
		log.Println("Closed config.json.")
	}(jsonFile)

	byteValue, err := ioutil.ReadAll(jsonFile)
	if err != nil {
		log.Fatalf("An error occurred while reading config.json: %v", err)
	}

	var result map[string]interface{}
	err = json.Unmarshal(byteValue, &result)
	if err != nil {
		log.Fatalf("An error occurred while unmarshalling config.json: %v", err)
	}

	return result
}

func getDependenciesForTable(
	ctx context.Context,
	con *pgx.Conn,
	schema string,
	table string,
) ([]Dependency, error) {
	sql := `
		SELECT
			d.dependency_schema_name AS schema_name
		,	d.dependency_table_name AS table_name
		,	d.dependency_field_name AS field_name
		,	COALESCE(l.ts, '1900-01-01 00:00:00 +0') AS load
		,	COALESCE(d.seen, '1900-01-01 00:00:00 +0') AS seen
		,	COALESCE(l.ts, '1900-01-01 00:00:00 +0') > COALESCE(d.seen, '1900-01-01 00:00:00 +0') AS stale
		FROM dvr.dependency AS d
		LEFT JOIN dvr.load AS l 
			ON d.dependency_schema_name = l.schema_name
			AND d.dependency_table_name = l.table_name
			AND d.dependency_field_name = l.field_name
		WHERE
			d.schema_name = $1
			AND d.table_name = $2
		ORDER BY 
			1, 2, 3
	`
	rows, err := con.Query(ctx, sql, schema, table)
	if err != nil {
		return []Dependency{}, err
	}
	defer rows.Close()

	var dependencies []Dependency
	for rows.Next() {
		var schemaName string
		var tableName string
		var fieldName string
		var load pgtype.Timestamptz
		var seen pgtype.Timestamptz
		var stale bool
		if err := rows.Scan(
			&schemaName,
			&tableName,
			&fieldName,
			&load,
			&seen,
			&stale,
		); err != nil {
			return nil, err
		}

		dependency := Dependency{
			Schema: schemaName,
			Table:  tableName,
			Field:  fieldName,
			Load:   load,
			Seen:   seen,
			Stale:  stale,
		}
		dependencies = append(dependencies, dependency)
	}

	return dependencies, nil
}

func getMaxTs(
	ctx context.Context,
	con *pgx.Conn,
	schema string,
	table string,
	field string,
) (pgtype.Timestamptz, error) {
	sql := fmt.Sprintf(
		`
			SELECT MAX(t2.ts) AS ts
			FROM (
				SELECT MAX(t."%s") AS ts 
				FROM "%s"."%s" AS t

				UNION ALL 

				SELECT '1900-01-01 00:00:00 +0'::TIMESTAMPTZ(0) AS ts
			) AS t2
		`, field, schema, table,
	)
	var ts pgtype.Timestamptz
	err := con.QueryRow(ctx, sql).Scan(&ts)
	if err != nil {
		return pgtype.Timestamptz{}, err
	}

	return ts, nil
}

func getLoad(
	ctx context.Context,
	con *pgx.Conn,
	schema string,
	table string,
	field string,
) (pgtype.Timestamptz, error) {
	sql := `
		SELECT MAX(t2.ts) AS ts
		FROM (
			SELECT t.ts
			FROM dvr.load AS t
			WHERE
				t.schema_name = $1
				AND t.table_name = $2
				AND t.field_name = $3
	
			UNION ALL
		
			SELECT '1900-01-01 00:00:00 +0'::TIMESTAMPTZ(0) AS ts
		) AS t2
	`
	var ts pgtype.Timestamptz
	err := con.QueryRow(ctx, sql, schema, table, field).Scan(&ts)
	if err != nil {
		return pgtype.Timestamptz{}, err
	}

	return ts, nil
}

func logError(
	ctx context.Context,
	con *pgx.Conn,
	schema string,
	spName string,
	errorMessage string,
) {
	sql := `
		INSERT INTO dvr.error (
			schema_name
		,	sp_name
		,	error_message
		,	ts
		) VALUES (
			$1
		,	$2
		,	$3
		,	NOW()
		)
		ON CONFLICT (sp_name, schema_name)
		DO UPDATE SET
			error_message = EXCLUDED.error_message
		,	ts = NOW()
	`
	_, err := con.Exec(ctx, sql, schema, spName, errorMessage)
	if err != nil {
		log.Fatalf("An error occurred while logging error to dvr.error: %v\nOriginal error message: %s", err, errorMessage)
	}

	log.Fatalf(errorMessage)
}

func logSkip(
	ctx context.Context,
	con *pgx.Conn,
	schema string,
	spName string,
) error {
	sql := `
		INSERT INTO dvr.skip (schema_name, sp_name)
		VALUES ($1, $2)
		ON CONFLICT (sp_name, schema_name) 
		DO UPDATE SET ts = NOW()
	`
	_, err := con.Exec(ctx, sql, schema, spName)
	if err != nil {
		return err
	}

	return nil
}

func onLogClose(path string, didRotate bool) {
	fmt.Printf("closed file '%s', didRotate: %v\n", path, didRotate)
	if !didRotate {
		return
	}
}

func openLogFile() (*dailyrotate.File, error) {
	logDir := "logs"

	// ensure folder exists
	err := os.MkdirAll(logDir, 0755)
	if err != nil {
		log.Fatalf("os.MkdirAll(): %v", err)
	}

	pathFormat := filepath.Join(logDir, "2006-01-02.txt")

	fileHandle, err := dailyrotate.NewFile(pathFormat, onLogClose)
	if err != nil {
		return nil, err
	}
	return fileHandle, nil
}

func runStoredProcedure(
	ctx context.Context,
	con *pgx.Conn,
	schema string,
	spName string,
	dependencies []Dependency,
) error {
	fullSpName := fmt.Sprintf("%s.%s", schema, spName)

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CALL %s(p_incremental := TRUE", fullSpName))

	i := 0
	var params []interface{}
	for _, dependency := range dependencies {
		i++
		sb.WriteString(fmt.Sprintf(", p_%s_%s_%s := $%d", dependency.Schema, dependency.Table, dependency.Field, i))
		params = append(params, dependency.Seen)
	}
	sb.WriteString(")")
	sql := sb.String()

	log.Printf("Running %s", sql)

	start := time.Now()

	_, err := con.Exec(ctx, sql, params...)
	if err != nil {
		return fmt.Errorf(
			"runStoredProcedure(con: ..., schema: %s, spName: %s, dependencies: %v, %s: %v",
			schema, spName, dependencies, sql, err,
		)
	}

	elapsedMillis := time.Since(start).Milliseconds()

	err = setElapsedMillis(ctx, con, schema, spName, elapsedMillis)
	if err != nil {
		return fmt.Errorf(
			"logElapsedMillis(con: ..., schema: %s, spName: %s, elapsedMillis: %v): %v",
			schema, spName, elapsedMillis, err,
		)
	}

	return nil
}

func setElapsedMillis(
	ctx context.Context,
	con *pgx.Conn,
	schema string,
	spName string,
	millis int64,
) error {
	sql := `
		INSERT INTO dvr.execution_time (
			schema_name
		,	sp_name
		,	ct
		,	millis
		,	latest_millis
		,	max_millis
		,	min_millis
		,	ts
		) 
		VALUES (
			$1
		,	$2
		,	1
		,	$3
		,	$3
		,	$3
		,	$3
		,	NOW()
		)
		ON CONFLICT (sp_name, schema_name)
		DO UPDATE SET
			ct = dvr.execution_time.ct + 1
		,	millis = dvr.execution_time.millis + EXCLUDED.millis
		,	latest_millis = EXCLUDED.latest_millis
		,	min_millis = CASE WHEN EXCLUDED.millis < dvr.execution_time.min_millis THEN EXCLUDED.millis ELSE dvr.execution_time.min_millis END
		,	max_millis = CASE WHEN EXCLUDED.millis > dvr.execution_time.max_millis THEN EXCLUDED.millis ELSE dvr.execution_time.max_millis END
		,	ts = NOW()
	`
	_, err := con.Exec(ctx, sql, schema, spName, millis)
	if err != nil {
		return err
	}

	return nil
}

func setLoad(
	ctx context.Context,
	con *pgx.Conn,
	schema string,
	table string,
	field string,
	ts pgtype.Timestamptz,
) error {
	updateSQL := `
		INSERT INTO dvr.load (
			schema_name
		,	table_name
		,	field_name
		,	ts
		) VALUES (
			$1
		,	$2
		,	$3
		,	$4
		)
		ON CONFLICT (schema_name, table_name, field_name)
		DO UPDATE SET
			ts = EXCLUDED.ts
		WHERE
			dvr.load.ts IS DISTINCT FROM EXCLUDED.ts
	`
	_, err := con.Exec(ctx, updateSQL, schema, table, field, ts)
	if err != nil {
		return err
	}

	return nil
}

func setSeen(
	ctx context.Context,
	con *pgx.Conn,
	schema string,
	table string,
	dependencySchema string,
	dependencyTable string,
	dependencyField string,
	ts pgtype.Timestamptz,
) error {
	sql := `
		UPDATE dvr.dependency AS d 
		SET 
			seen = $1
		WHERE
			d.table_name = $2
			AND d.schema_name = $3
			AND d.dependency_table_name = $4
			AND d.dependency_field_name = $5
			AND d.dependency_schema_name = $6
	`
	_, err := con.Exec(ctx, sql, ts, table, schema, dependencyTable, dependencyField, dependencySchema)
	if err != nil {
		return err
	}

	return nil
}

func setStatus(
	ctx context.Context,
	con *pgx.Conn,
	schema string,
	spName string,
	status string,
) error {
	sql := `
		INSERT INTO dvr.status (
			schema_name
		,	sp_name
		,	status 
		,	ts
		) VALUES (
			$1
		,	$2
		,	$3
		,	NOW()
		)
		ON CONFLICT (sp_name, schema_name) 
		DO UPDATE SET 
			status = EXCLUDED.status
		,	ts = NOW()
	`
	_, err := con.Exec(ctx, sql, schema, spName, status)
	if err != nil {
		return err
	}

	return nil
}

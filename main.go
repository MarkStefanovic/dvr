package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"io/ioutil"
	"log"
	"os"
	"strings"
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
	schemaPtr := flag.String("schema", "", "Schema of table to refresh")
	tablePtr := flag.String("table", "", "Name of table to refresh")
	tsFieldPtr := flag.String("ts", "", "Comma-seperated list of timestamp fields")
	flag.Parse()

	log.Printf("Running refresh with the following parameters: schema = %s, table = %s, tsFields = %s", *schemaPtr, *tablePtr, *tsFieldPtr)

	if *schemaPtr == "" {
		log.Fatalf("No schema was provided.")
	}

	if *tablePtr == "" {
		log.Fatalf("No table was provided.")
	}

	if *tsFieldPtr == "" {
		log.Fatalf("No timestamp fields were provided.")
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

	dependencies, err := getDependenciesForTable(con, *schemaPtr, *tablePtr)
	if err != nil {
		log.Fatalf("An error occurred while fetching the dependencies for table, %s: %v", *tablePtr, err)
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
	} else {
		log.Println("The following dependencies are ready to be updated:")
		for dependencyName, _ := range dependenciesReadyToUpdate {
			log.Printf("- %s", dependencyName)
		}

		seenTimestamps := map[string]pgtype.Timestamptz{}
		for _, dependency := range dependencies {
			ts, err := getLoad(con, dependency.Schema, dependency.Table, dependency.Field)
			if err != nil {
				log.Fatalf("An error occurred while getting the initial load timestamp for %s: %v", dependency.FullName(), err)
			}
			seenTimestamps[dependency.FullName()] = ts
		}

		err = runStoredProcedure(
			con,
			*schemaPtr,
			fmt.Sprintf("refresh_%s", *tablePtr),
			dependencies,
		)
		if err != nil {
			log.Fatalf("An error occurred while running the stored procedure: %v", err)
		}

		log.Println("Refresh was successful.")

		for _, tsField := range tsFields {
			ts, err := getMaxTs(con, *schemaPtr, *tablePtr, tsField)
			if err != nil {
				log.Fatalf("An error occurred while getting the maximum timestamp for %s.%s.%s: %v", *schemaPtr, *tablePtr, tsField, err)
			}

			err = setLoad(con, *schemaPtr, *tablePtr, tsField, ts)
			if err != nil {
				log.Fatalf("An error occurred while setting the load timestamp for %s.%s.%s: %v", *schemaPtr, *tablePtr, tsField, err)
			}
		}

		for _, dependency := range dependencies {
			err := setSeen(con, *schemaPtr, *tablePtr, dependency.Schema, dependency.Table, dependency.Field, seenTimestamps[dependency.FullName()])
			if err != nil {
				log.Fatalf("An error occurred while setting the seen timestamp for %s: %v", dependency.FullName(), err)
			}
		}
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
	err = json.Unmarshal([]byte(byteValue), &result)
	if err != nil {
		log.Fatalf("An error occurred while unmarshalling config.json: %v", err)
	}

	return result
}

func getDependenciesForTable(
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
		FROM etl.dependency AS d
		LEFT JOIN etl.load AS l 
			ON d.dependency_schema_name = l.schema_name
			AND d.dependency_table_name = l.table_name
			AND d.dependency_field_name = l.field_name
		WHERE
			d.schema_name = $1
			AND d.table_name = $2
		ORDER BY 
			1, 2, 3
	`

	rows, err := con.Query(context.Background(), sql, schema, table)
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

func getMaxTs(con *pgx.Conn, schema string, table string, field string) (pgtype.Timestamptz, error) {
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
	err := con.QueryRow(context.Background(), sql).Scan(&ts)
	if err != nil {
		return pgtype.Timestamptz{}, err
	}

	return ts, nil
}

func getLoad(
	con *pgx.Conn,
	schema string,
	table string,
	field string,
) (pgtype.Timestamptz, error) {
	sql := `
		SELECT MAX(t2.ts) AS ts
		FROM (
			SELECT t.ts
			FROM etl.load AS t
			WHERE
				t.schema_name = $1
				AND t.table_name = $2
				AND t.field_name = $3
	
			UNION ALL
		
			SELECT '1900-01-01 00:00:00 +0'::TIMESTAMPTZ(0) AS ts
		) AS t2
	`
	var ts pgtype.Timestamptz
	err := con.QueryRow(context.Background(), sql, schema, table, field).Scan(&ts)
	if err != nil {
		return pgtype.Timestamptz{}, err
	}

	return ts, nil
}

func getSeen(
	con *pgx.Conn,
	schema string,
	table string,
	dependency Dependency,
) (pgtype.Timestamptz, error) {
	sql := `
		SELECT MAX(t2.ts) AS ts
		FROM (
			SELECT t.seen AS ts
			FROM etl.dependency AS t
			WHERE
				t.schema_name = $1
				AND t.table_name = $2
				AND t.dependency_schema_name = $3
				AND t.dependency_table_name = $4
				AND t.dependency_field_name = $5
	
			UNION ALL
		
			SELECT '1900-01-01 00:00:00 +0'::TIMESTAMPTZ(0) AS ts
		) AS t2
	`
	var ts pgtype.Timestamptz
	err := con.QueryRow(context.Background(), sql, schema, table, dependency.Schema, dependency.Table, dependency.Field).Scan(&ts)
	if err != nil {
		return pgtype.Timestamptz{}, err
	}

	return ts, nil
}

func runStoredProcedure(
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

	_, err := con.Exec(context.Background(), sql, params...)
	if err != nil {
		return fmt.Errorf(
			"runStoredProcedure(con: ..., schema: %s, spName: %s, dependencies: %v, %s: %v",
			schema, spName, dependencies, sql, err,
		)
	}

	return nil
}

func setLoad(
	con *pgx.Conn,
	schema string,
	table string,
	field string,
	ts pgtype.Timestamptz,
) error {
	updateSQL := `
		INSERT INTO etl.load (
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
			etl.load.ts IS DISTINCT FROM EXCLUDED.ts
		;
	`
	_, err := con.Exec(context.Background(), updateSQL, schema, table, field, ts)
	if err != nil {
		return err
	}

	return nil
}

func setSeen(
	con *pgx.Conn,
	schema string,
	table string,
	dependencySchema string,
	dependencyTable string,
	dependencyField string,
	ts pgtype.Timestamptz,
) error {
	sql := `
		UPDATE etl.dependency AS d 
		SET 
			seen = $1
		WHERE
			d.table_name = $2
			AND d.schema_name = $3
			AND d.dependency_table_name = $4
			AND d.dependency_field_name = $5
			AND d.dependency_schema_name = $6
	`

	_, err := con.Exec(context.Background(), sql, ts, table, schema, dependencyTable, dependencyField, dependencySchema)
	if err != nil {
		return err
	}

	return nil
}

package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	mssql "github.com/microsoft/go-mssqldb"
	"github.com/schollz/progressbar/v3"
)

type sourceDB struct {
	db *sqlx.DB
}

func newSourceDB(user, pass, host, dbname string) (*sourceDB, error) {
	db, err := sqlx.Connect("sqlserver", fmt.Sprintf(
		"sqlserver://%s:%s@%s?database=%s&encrypt=disable",
		user, pass, host, dbname,
	))
	if err != nil {
		return nil, fmt.Errorf("open src db: %w", err)
	}

	return &sourceDB{db}, nil
}

func (db *sourceDB) getTables(tablesToPutLast, excludes []string) ([]tableInfo, error) {
	// From https://stackoverflow.com/a/7892349
	rows, err := db.db.Queryx(
		`SELECT
			s.name AS schema_name,
			t.name AS table_name
		FROM
			sys.tables t
		INNER JOIN
			sys.indexes i ON t.OBJECT_ID = i.object_id
		INNER JOIN
			sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
		INNER JOIN
			sys.allocation_units a ON p.partition_id = a.container_id
		LEFT OUTER JOIN
			sys.schemas s ON t.schema_id = s.schema_id
		WHERE
			t.name NOT LIKE 'dt%'
			AND t.is_ms_shipped = 0
			AND i.OBJECT_ID > 255
		GROUP BY
			t.name, s.name, p.Rows
		ORDER BY
			SUM(a.used_pages) ASC, t.name ASC`,
	)

	if err != nil {
		return nil, fmt.Errorf("sql query: %w", err)
	}

	exts := make([]tableInfo, len(excludes))
	for i, s := range excludes {
		ss := strings.Split(s, ".")
		exts[i] = tableInfo{schema: ss[0], name: ss[1]}
	}

	tables := []tableInfo{}
	for rows.Next() {
		var t tableInfo
		if err := rows.Scan(&t.schema, &t.name); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}

		excluded := false
		for _, ext := range exts {
			if t.schema == ext.schema && t.name == ext.name {
				excluded = true
				break
			}
		}
		if !excluded {
			tables = append(tables, t)
		}
	}

	var firstTables, lastTables []tableInfo
	for _, t := range tablesToPutLast {
		ss := strings.Split(t, ".")
		for _, tt := range tables {
			if tt.schema == ss[0] && tt.name == ss[1] {
				lastTables = append(lastTables, tt)
				break
			}
		}
	}
	for _, t := range tables {
		isLast := false
		for _, tt := range lastTables {
			if t.schema == tt.schema && t.name == tt.name {
				isLast = true
				break
			}
		}
		if !isLast {
			firstTables = append(firstTables, t)
		}
	}

	return append(firstTables, lastTables...), nil
}

func (db *sourceDB) getChangeTrackingCurrentVersion(t tableInfo) (int64, error) {
	var ver int64
	err := db.db.QueryRowx(`SELECT CHANGE_TRACKING_CURRENT_VERSION()`).Scan(&ver)
	return ver, err
}

func (db *sourceDB) enableChangeTracking(t tableInfo) error {
	var count uint
	err := db.db.QueryRowx(
		`SELECT count(*)
		FROM sys.change_tracking_databases
		WHERE database_id = db_id()`,
	).Scan(&count)
	if err != nil {
		return fmt.Errorf("check change_tracking_databases: %w", err)
	}

	if count == 0 {
		_, err = db.db.Exec(
			`ALTER DATABASE CURRENT
			SET CHANGE_TRACKING = ON
			(CHANGE_RETENTION = 14 DAYS, AUTO_CLEANUP = ON)`,
		)
		if err != nil {
			return fmt.Errorf("enable db change tracking: %w", err)
		}
	}

	err = db.db.QueryRowx(fmt.Sprintf(
		`SELECT count(*)
		FROM sys.change_tracking_tables
		WHERE object_id = object_id('%s.%s')`,
		t.schema, t.name,
	)).Scan(&count)
	if err != nil {
		return fmt.Errorf(`check change_tracking_tables: %w`, err)
	}

	if count == 1 {
		return nil
	}

	_, err = db.db.Exec(fmt.Sprintf(
		`ALTER TABLE [%s].[%s] ENABLE CHANGE_TRACKING`,
		t.schema, t.name,
	))
	if err != nil {
		return fmt.Errorf("enable table change tracking: %w", err)
	}

	return nil
}

func (db *sourceDB) readRows(ctx context.Context, t tableInfo, output chan<- rowdata) error {
	rows, err := db.db.QueryxContext(ctx, fmt.Sprintf(
		`SELECT * FROM [%s].[%s] WITH (NOLOCK)`,
		t.schema, t.name,
	))
	if err != nil {
		return fmt.Errorf("sql query: %w", err)
	}
	defer rows.Close()

	cts, err := rows.ColumnTypes()
	if err != nil {
		return fmt.Errorf("read column types: %w", err)
	}

	// From progressbar.Default()
	bar := progressbar.NewOptions64(
		-1,
		progressbar.OptionSetDescription(""),
		progressbar.OptionSetWriter(os.Stderr),
		progressbar.OptionSetWidth(10),
		progressbar.OptionThrottle(500*time.Millisecond),
		progressbar.OptionShowCount(),
		progressbar.OptionShowIts(),
		progressbar.OptionSetItsString("rows"),
		progressbar.OptionOnCompletion(func() {
			fmt.Fprint(os.Stderr, "\n")
		}),
		progressbar.OptionSpinnerType(14),
		progressbar.OptionFullWidth(),
		progressbar.OptionSetRenderBlankState(true),
	)

	for rows.Next() {
		row, err := rows.SliceScan()
		if err != nil {
			return fmt.Errorf("row scan: %w", err)
		}

		bar.Add(1)

		rd := rowdata{}
		for i, ct := range cts {
			rd[ct.Name()] = db.fixValueByType(row[i], ct)
		}

		select {
		case output <- rd:
		case <-ctx.Done():
			return fmt.Errorf("data read aborted, reason: %w", ctx.Err())
		}
	}
	fmt.Println()

	return nil
}

func (db *sourceDB) fixValueByType(v any, ct *sql.ColumnType) any {
	if v == nil {
		return nil
	}

	switch ct.DatabaseTypeName() {
	case "UNIQUEIDENTIFIER":
		b := v.([]byte)
		b[0], b[1], b[2], b[3] = b[3], b[2], b[1], b[0]
		b[4], b[5] = b[5], b[4]
		b[6], b[7] = b[7], b[6]

		return strings.ToLower(mssql.UniqueIdentifier(b).String())
	case "DECIMAL":
		v, _ := strconv.ParseFloat(string(v.([]byte)), 64)
		return v
	case "TEXT":
		v, _ := v.(string)
		if strings.Contains(v, "\x00") {
			log.Printf("Warning: removed NULL characters in column '%s'", ct.Name())
		}
		return strings.ReplaceAll(v, "\x00", "")
	default:
		return v
	}
}

func (db *sourceDB) getChangeTrackingMinValidVersion(t tableInfo) (int64, error) {
	var ver int64
	err := db.db.QueryRowx(fmt.Sprintf(
		"SELECT CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID('%s.%s'))",
		t.schema, t.name,
	)).Scan(&ver)
	if err != nil {
		return 0, fmt.Errorf("sql query: %w", err)
	}

	return ver, nil
}

type tablechange struct {
	operation   string
	primaryKeys rowdata
	rowdata     rowdata
}

func (db *sourceDB) readTableChanges(ctx context.Context, t tableInfo, lastSyncVersion int64, output chan<- tablechange) (uint, error) {
	rows, err := db.db.Queryx(fmt.Sprintf(
		`SELECT * FROM CHANGETABLE(CHANGES [%s].[%s], %d) AS ct ORDER BY SYS_CHANGE_VERSION`,
		t.schema, t.name, lastSyncVersion,
	))
	if err != nil {
		return 0, fmt.Errorf("sql select changetable: %w", err)
	}

	cts, err := rows.ColumnTypes()
	if err != nil {
		return 0, fmt.Errorf("read column types: %w", err)
	}

	var n uint
	for rows.Next() {
		n++

		row, err := rows.SliceScan()
		if err != nil {
			return 0, fmt.Errorf("scan row: %w", err)
		}

		tc := tablechange{
			primaryKeys: rowdata{},
		}

		for i, ct := range cts {
			if ct.Name() == "SYS_CHANGE_OPERATION" {
				tc.operation = row[i].(string)
			} else if !strings.HasPrefix(ct.Name(), "SYS_CHANGE_") {
				tc.primaryKeys[ct.Name()] = db.fixValueByType(row[i], ct)
			}
		}

		if tc.operation != "D" {
			rd, err := db.getRow(t, tc.primaryKeys)
			if err != nil {
				return 0, fmt.Errorf("read row data: %w", err)
			}
			tc.rowdata = rd
		}

		select {
		case output <- tc:
		case <-ctx.Done():
			return 0, fmt.Errorf("read change table aborted, reason: %w", ctx.Err())
		}
	}

	return n, nil
}

func (db *sourceDB) getRow(t tableInfo, primaryKeys rowdata) (rowdata, error) {
	filters := make([]string, len(primaryKeys))
	values := make([]any, len(primaryKeys))
	i := 0
	for k, v := range primaryKeys {
		filters[i] = fmt.Sprintf("[%s] = @p%d", k, i+1)
		values[i] = v
		i++
	}

	row := db.db.QueryRowx(
		fmt.Sprintf(
			`SELECT * FROM [%s].[%s] WHERE %s`,
			t.schema, t.name, strings.Join(filters, " AND "),
		),
		values...,
	)

	cts, err := row.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("read column types: %w", err)
	}

	vals, err := row.SliceScan()
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("scan row: %w", err)
	}

	rd := rowdata{}
	for i, ct := range cts {
		rd[ct.Name()] = db.fixValueByType(vals[i], ct)
	}

	return rd, nil
}

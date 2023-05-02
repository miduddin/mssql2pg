package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/jmoiron/sqlx"
	gomssql "github.com/microsoft/go-mssqldb"
	"github.com/rs/zerolog/log"
)

type mssql struct {
	db *sqlx.DB
}

func newMssql(user, pass, host, dbname string) (*mssql, error) {
	db, err := sqlx.Connect("sqlserver", fmt.Sprintf(
		"sqlserver://%s:%s@%s?database=%s&encrypt=disable",
		user, pass, host, dbname,
	))
	if err != nil {
		return nil, fmt.Errorf("open src db: %w", err)
	}

	return &mssql{db}, nil
}

func (db *mssql) getTables(tablesToPutLast, excludes []string) ([]tableInfo, error) {
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

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read rows: %w", err)
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

func (db *mssql) getChangeTrackingCurrentVersion(t tableInfo) (int64, error) {
	var ver int64
	err := db.db.QueryRowx(`SELECT CHANGE_TRACKING_CURRENT_VERSION()`).Scan(&ver)
	return ver, err
}

func (db *mssql) enableChangeTracking(t tableInfo, retentionDays uint) error {
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
		_, err = db.db.Exec(fmt.Sprintf(
			`ALTER DATABASE CURRENT
			SET CHANGE_TRACKING = ON
			(CHANGE_RETENTION = %d DAYS, AUTO_CLEANUP = ON)`,
			retentionDays,
		))
		if err != nil {
			return fmt.Errorf("enable db change tracking: %w", err)
		}
	} else {
		_, err = db.db.Exec(fmt.Sprintf(
			`ALTER DATABASE CURRENT
				SET CHANGE_TRACKING
				(CHANGE_RETENTION = %d DAYS, AUTO_CLEANUP = ON)`,
			retentionDays,
		))
		if err != nil {
			return fmt.Errorf("update db change tracking params: %w", err)
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

func (db *mssql) readRowsWithPK(ctx context.Context, t tableInfo, afterPK any, output chan<- rowData) error {
	pks, err := db.getPrimaryKeys(t)
	if err != nil {
		return fmt.Errorf("get primary keys: %w", err)
	}

	orders := make([]string, len(pks))
	for i, pk := range pks {
		orders[i] = pk + " ASC"
	}

	var query string
	var args []any

	if afterPK != nil {
		// Only support reading from the middle of data if table has single PK column
		// because query performance may not be good with multi column PK.
		if len(pks) != 1 {
			return fmt.Errorf("filtered read not supported on table with multi column primary key")
		}

		query = fmt.Sprintf(
			`SELECT * FROM [%s].[%s] WITH (NOLOCK) WHERE %s > @p1 ORDER BY %s`,
			t.schema, t.name, pks[0], strings.Join(orders, ","),
		)
		args = []any{afterPK}
	} else {
		query = fmt.Sprintf(
			`SELECT * FROM [%s].[%s] WITH (NOLOCK) ORDER BY %s`,
			t.schema, t.name, strings.Join(orders, ","),
		)
	}

	return db.readRows(ctx, t, output, query, args...)
}

func (db *mssql) readRows(ctx context.Context, t tableInfo, output chan<- rowData, query string, args ...any) error {
	rows, err := db.db.QueryxContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("sql query: %w", err)
	}
	defer rows.Close()

	cts, err := rows.ColumnTypes()
	if err != nil {
		return fmt.Errorf("read column types: %w", err)
	}

	for rows.Next() {
		row, err := rows.SliceScan()
		if err != nil {
			return fmt.Errorf("row scan: %w", err)
		}

		rd := rowData{}
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

	if err := rows.Err(); err != nil {
		return fmt.Errorf("read rows: %w", err)
	}

	return nil
}

func (db *mssql) getPrimaryKeys(t tableInfo) ([]string, error) {
	rows, err := db.db.Queryx(
		`SELECT column_name
		FROM information_schema.key_column_usage
		WHERE objectproperty(object_id(constraint_schema + '.' + quotename(constraint_name)), 'IsPrimaryKey') = 1
		AND table_schema = @p1 AND table_name = @p2
		ORDER BY ordinal_position`,
		t.schema, t.name,
	)
	if err != nil {
		return nil, fmt.Errorf("sql select: %w", err)
	}

	var ret []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}

		ret = append(ret, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scan row: %w", err)
	}

	return ret, nil
}

func (db *mssql) fixValueByType(v any, ct *sql.ColumnType) any {
	if v == nil {
		return nil
	}

	switch ct.DatabaseTypeName() {
	case "UNIQUEIDENTIFIER":
		b := v.([]byte)
		b[0], b[1], b[2], b[3] = b[3], b[2], b[1], b[0]
		b[4], b[5] = b[5], b[4]
		b[6], b[7] = b[7], b[6]

		return strings.ToLower(gomssql.UniqueIdentifier(b).String())
	case "DECIMAL":
		v, _ := strconv.ParseFloat(string(v.([]byte)), 64)
		return v
	case "TEXT":
		v, _ := v.(string)
		if strings.Contains(v, "\x00") {
			log.Warn().Msgf("Removed null characters in column '%s'", ct.Name())
		}
		return strings.ReplaceAll(v, "\x00", "")
	default:
		return v
	}
}

func (db *mssql) getChangeTrackingMinValidVersion(t tableInfo) (int64, error) {
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

func (db *mssql) readRowChanges(ctx context.Context, t tableInfo, lastSyncVersion int64, output chan<- rowChange) (uint, error) {
	rows, err := db.db.Queryx(fmt.Sprintf(
		`SELECT * FROM CHANGETABLE(CHANGES [%s].[%s], %d) AS ct`,
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

		tc := rowChange{
			primaryKeys: rowData{},
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

	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("read rows: %w", err)
	}

	return n, nil
}

func (db *mssql) getRow(t tableInfo, primaryKeys rowData) (rowData, error) {
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

	rd := rowData{}
	for i, ct := range cts {
		rd[ct.Name()] = db.fixValueByType(vals[i], ct)
	}

	return rd, nil
}

func (db *mssql) getRowCount(t tableInfo) (int64, error) {
	var ret int64
	err := db.db.QueryRowx(fmt.Sprintf(
		`SELECT SUM(st.row_count)
		FROM sys.dm_db_partition_stats st
		WHERE object_id = object_id('%s.%s') AND (index_id < 2)`,
		t.schema, t.name,
	)).Scan(&ret)
	if err != nil {
		return -1, fmt.Errorf("sql select: %w", err)
	}

	return ret, nil
}

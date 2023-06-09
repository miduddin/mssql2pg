package main

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

type postgres struct {
	db *sqlx.DB
}

func newPostgres(user, pass, host, dbname string) (*postgres, error) {
	os.Unsetenv("PGSERVICEFILE")
	db, err := sqlx.Connect("postgres", fmt.Sprintf(
		"postgres://%s:%s@%s/%s?sslmode=disable",
		user, pass, host, dbname,
	))
	if err != nil {
		return nil, fmt.Errorf("open dst db: %w", err)
	}

	return &postgres{db: db}, nil
}

type foreignKey struct {
	t          tableInfo
	name       string
	definition string
}

func (db *postgres) getForeignKeys() ([]foreignKey, error) {
	rows, err := db.db.Queryx(
		`SELECT
			conrelid::regclass,
			conname,
			pg_get_constraintdef(oid)
		FROM pg_constraint
		WHERE contype = 'f'
		ORDER BY conname ASC`,
	)
	if err != nil {
		return nil, fmt.Errorf("sql select: %w", err)
	}

	ret := []foreignKey{}
	for rows.Next() {
		var schemaTable string
		fk := foreignKey{}
		if err := rows.Scan(&schemaTable, &fk.name, &fk.definition); err != nil {
			return nil, fmt.Errorf("row scan: %w", err)
		}

		ss := strings.Split(schemaTable, ".")
		if len(ss) == 1 {
			fk.t.schema = "public"
			fk.t.name = ss[0]
		} else {
			fk.t.schema = ss[0]
			fk.t.name = ss[1]
		}

		ret = append(ret, fk)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read rows: %w", err)
	}

	return ret, nil
}

func (db *postgres) dropForeignKeys(fks []foreignKey) error {
	for _, fk := range fks {
		_, err := db.db.Exec(fmt.Sprintf(
			`ALTER TABLE "%s"."%s" DROP CONSTRAINT IF EXISTS "%s"`,
			fk.t.schema, fk.t.name, fk.name,
		))
		if err != nil {
			return fmt.Errorf("drop fk '%s': %w", fk.name, err)
		}
	}

	return nil
}

func (db *postgres) createForeignKeys(fks []foreignKey) error {
	tx, err := db.db.Beginx()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	for _, fk := range fks {
		_, err := tx.Exec(fmt.Sprintf(
			`ALTER TABLE "%s"."%s" ADD CONSTRAINT "%s" %s`,
			fk.t.schema, fk.t.name, fk.name, fk.definition,
		))
		if err != nil {
			return fmt.Errorf("alter table: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

func (db *postgres) getPrimaryKeys(t tableInfo) ([]string, error) {
	tableName := t.name
	if t.schema != "public" {
		tableName = t.schema + "." + tableName
	}

	rows, err := db.db.Queryx(
		`SELECT a.attname
		FROM pg_attribute a
			JOIN (
				SELECT *, GENERATE_SUBSCRIPTS(indkey, 1) AS indkey_subscript
				FROM pg_index
				WHERE indrelid = CAST($1 AS regclass)
			) AS i
			ON i.indisprimary AND i.indrelid = a.attrelid AND a.attnum = i.indkey[i.indkey_subscript]
		WHERE a.attrelid = CAST($1 AS regclass)
		ORDER BY i.indkey_subscript`,
		tableName,
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
		return nil, fmt.Errorf("read rows: %w", err)
	}

	return ret, nil
}

type index struct {
	table tableInfo
	name  string
	def   string
}

func (db *postgres) getIndexes(t tableInfo) ([]index, error) {
	rows, err := db.db.Queryx(
		`SELECT indexname, indexdef
		FROM pg_indexes
		WHERE schemaname = $1 AND tablename = $2
		ORDER BY indexname`,
		t.schema, t.name,
	)
	if err != nil {
		return nil, fmt.Errorf("sql query: %w", err)
	}

	var ret []index
	for rows.Next() {
		var ixn, ixd string
		if err := rows.Scan(&ixn, &ixd); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}

		ret = append(ret, index{table: t, name: ixn, def: ixd})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read rows: %w", err)
	}

	return ret, nil
}

func (db *postgres) dropIndexes(ixs []index) error {
	re := regexp.MustCompile(".+? cannot drop index .+? because constraint .+? on table .+? requires it")

	for _, ix := range ixs {
		_, err := db.db.Exec(fmt.Sprintf(`DROP INDEX "%s"."%s"`, ix.table.schema, ix.name))

		// Ignore indexes from constraints.
		if err != nil && !re.MatchString(err.Error()) {
			return fmt.Errorf("drop index '%s.%s': %w", ix.table.schema, ix.name, err)
		}
	}

	return nil
}

func (db *postgres) createIndexes(ctx context.Context, ixs []index) error {
	for _, ix := range ixs {
		_, err := db.db.ExecContext(ctx, strings.Replace(ix.def, " INDEX ", " INDEX IF NOT EXISTS ", 1))
		if err != nil {
			return fmt.Errorf("create index '%s': %w", ix.name, err)
		}
	}

	return nil
}

func (db *postgres) getRowCount(t tableInfo) (int64, error) {
	var count int64
	err := db.db.QueryRowx(fmt.Sprintf(
		`SELECT count(*) FROM "%s"."%s"`,
		t.schema, t.name,
	)).Scan(&count)

	return count, err
}

func (db *postgres) insertRows(ctx context.Context, t tableInfo, truncateFirst bool, batchSize uint, input <-chan rowData, cb func()) (lastInsertedID any, err error) {
	if truncateFirst {
		_, err := db.db.Exec(fmt.Sprintf(`TRUNCATE TABLE "%s"."%s"`, t.schema, t.name))
		if err != nil {
			return nil, fmt.Errorf("truncate table: %w", err)
		}
	}

	pks, err := db.getPrimaryKeys(t)
	if err != nil {
		return nil, fmt.Errorf("get primary keys: %w", err)
	}

	var (
		tx   *sqlx.Tx
		stmt *sqlx.Stmt
		cols []string
		vals []any
	)

	var lastTempInsertedID any
	var count uint = 0
	for {
		select {
		case rd, ok := <-input:
			count++

			if !ok || count == batchSize {
				// stmt can be nil when input is empty.
				if stmt != nil {
					if _, err := stmt.ExecContext(ctx); err != nil {
						return lastInsertedID, fmt.Errorf("flush copy: %w", err)
					}

					if err := tx.Commit(); err != nil {
						return lastInsertedID, fmt.Errorf("commit tx: %w", err)
					}
					lastInsertedID = lastTempInsertedID
				}

				if !ok {
					return lastInsertedID, nil
				}

				stmt.Close()
				tx = nil
				stmt = nil
				count = 0
				cols = nil
			}

			if tx == nil {
				tx, err = db.db.Beginx()
				if err != nil {
					return lastInsertedID, fmt.Errorf("begin tx: %w", err)
				}
				defer tx.Rollback()
			}

			if stmt == nil {
				for k := range rd {
					cols = append(cols, k)
				}
				vals = make([]any, len(cols))

				stmt, err = tx.PreparexContext(ctx, pq.CopyInSchema(t.schema, t.name, cols...))
				if err != nil {
					return lastInsertedID, fmt.Errorf("prepare statement: %w", err)
				}
				defer stmt.Close()
			}

			for i, c := range cols {
				vals[i] = rd[c]
			}

			if _, err := stmt.ExecContext(ctx, vals...); err != nil {
				return lastInsertedID, fmt.Errorf("exec statement: %w", err)
			}
			if len(pks) == 1 {
				lastTempInsertedID = rd[pks[0]]
			}
			if cb != nil {
				cb()
			}

		case <-ctx.Done():
			return lastInsertedID, fmt.Errorf("insert data aborted, reason: %w", ctx.Err())
		}
	}
}

func (db *postgres) writeRowChanges(ctx context.Context, t tableInfo, input <-chan rowChange, cb func(*rowChange)) (uint, error) {
	var n uint = 0
	for {
		select {
		case tc, ok := <-input:
			if !ok {
				return n, nil
			}

			switch tc.operation {
			case "I", "U":
				// Row can be deleted in source inbetween the time when we query the changetable
				// and the time when we fetch the row. In such case, next changetable query will
				// report the row as deleted, so no additional action here after that.
				if tc.rowdata == nil {
					continue
				}

				n++
				if err := db.upsertRow(t, tc.primaryKeys, tc.rowdata); err != nil {
					return 0, fmt.Errorf("upsert dst data: %w", err)
				}
			case "D":
				n++
				if err := db.deleteRow(t, tc.primaryKeys); err != nil {
					return 0, fmt.Errorf("delete dst data: %w", err)
				}
			}

			if cb != nil {
				cb(&tc)
			}

		case <-ctx.Done():
			return 0, fmt.Errorf("write change table aborted, reason: %w", ctx.Err())
		}
	}
}

func (db *postgres) upsertRow(t tableInfo, primaryKeys, data rowData) error {
	n := len(data)
	cols := make([]string, n)
	insertParams := make([]string, n)
	updateParams := make([]string, n)
	values := make([]any, n)
	i := 0
	for k, v := range data {
		cols[i] = `"` + k + `"`
		insertParams[i] = fmt.Sprintf("$%d", i+1)
		updateParams[i] = fmt.Sprintf("$%d", i+1)
		values[i] = v
		i++
	}
	colStr := strings.Join(cols, ",")

	pksConds := make([]string, len(primaryKeys))
	pkVals := make([]any, len(primaryKeys))
	i = 0
	for k, v := range primaryKeys {
		pksConds[i] = fmt.Sprintf(`"%s" = $%d`, k, len(values)+i+1)
		pkVals[i] = v
		i++
	}

	res, err := db.db.Exec(
		fmt.Sprintf(
			`UPDATE "%s"."%s" SET (%s) = ROW(%s) WHERE %s`,
			t.schema, t.name, colStr, strings.Join(updateParams, ","), strings.Join(pksConds, " AND "),
		),
		append(values, pkVals...)...,
	)
	if err != nil {
		return fmt.Errorf("sql update: %w", err)
	}

	count, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("get rows affected: %w", err)
	}
	if count > 0 {
		return nil
	}

	_, err = db.db.Exec(
		fmt.Sprintf(
			`INSERT INTO "%s"."%s" (%s) VALUES (%s)`,
			t.schema, t.name, colStr, strings.Join(insertParams, ","),
		),
		values...,
	)
	if err != nil {
		return fmt.Errorf("sql insert: %w", err)
	}

	return nil
}

func (db *postgres) deleteRow(t tableInfo, primaryKeys rowData) error {
	filters := make([]string, len(primaryKeys))
	values := make([]any, len(primaryKeys))
	i := 0
	for k, v := range primaryKeys {
		filters[i] = fmt.Sprintf(`"%s" = $%d`, k, i+1)
		values[i] = v
		i++
	}

	_, err := db.db.Exec(
		fmt.Sprintf(
			`DELETE FROM "%s"."%s" WHERE %s`,
			t.schema, t.name, strings.Join(filters, " AND "),
		),
		values...,
	)

	return err
}

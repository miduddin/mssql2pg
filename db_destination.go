package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

type destinationDB struct {
	db *sqlx.DB
}

func newDestinationDB(user, pass, host, dbname string) (*destinationDB, error) {
	os.Unsetenv("PGSERVICEFILE")
	db, err := sqlx.Connect("postgres", fmt.Sprintf(
		"postgres://%s:%s@%s/%s?sslmode=disable",
		user, pass, host, dbname,
	))
	if err != nil {
		return nil, fmt.Errorf("open dst db: %w", err)
	}

	return &destinationDB{db}, nil
}

type dstForeignKey struct {
	t          tableInfo
	name       string
	definition string
}

func (db *destinationDB) getForeignKeys() ([]dstForeignKey, error) {
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

	ret := []dstForeignKey{}
	for rows.Next() {
		var schemaTable string
		fk := dstForeignKey{}
		if err := rows.Scan(&schemaTable, &fk.name, &fk.definition); err != nil {
			return nil, fmt.Errorf("row scan: %w", err)
		}

		ss := strings.Split(schemaTable, ".")
		fk.t.schema = ss[0]
		fk.t.name = ss[1]

		ret = append(ret, fk)
	}

	return ret, nil
}

func (db *destinationDB) dropForeignKeys(fks []dstForeignKey) error {
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

func (db *destinationDB) createForeignKeys(fks []dstForeignKey) error {
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

func (db *destinationDB) insertRows(ctx context.Context, t tableInfo, input <-chan rowdata) error {
	tx, err := db.db.Beginx()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`TRUNCATE TABLE "%s"."%s"`, t.schema, t.name)); err != nil {
		return fmt.Errorf("truncate table: %w", err)
	}

	var (
		stmt *sqlx.Stmt
		cols []string
		vals []any
	)

	for {
		select {
		case rd, ok := <-input:
			if !ok {
				if _, err := stmt.ExecContext(ctx); err != nil {
					return fmt.Errorf("flush copy: %w", err)
				}

				if err := tx.Commit(); err != nil {
					return fmt.Errorf("commit tx: %w", err)
				}

				return nil
			}

			if stmt == nil {
				for k := range rd {
					cols = append(cols, k)
				}
				vals = make([]any, len(cols))

				stmt, err = tx.PreparexContext(ctx, pq.CopyInSchema(t.schema, t.name, cols...))
				if err != nil {
					return fmt.Errorf("prepare statement: %w", err)
				}
				defer stmt.Close()
			}

			for i, c := range cols {
				vals[i] = rd[c]
			}

			if _, err := stmt.ExecContext(ctx, vals...); err != nil {
				return fmt.Errorf("exec statement: %w", err)
			}

		case <-ctx.Done():
			return fmt.Errorf("insert data aborted, reason: %w", ctx.Err())
		}
	}
}

func (db *destinationDB) writeTableChanges(ctx context.Context, t tableInfo, input <-chan tablechange) (uint, error) {
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
		case <-ctx.Done():
			return 0, fmt.Errorf("write change table aborted, reason: %w", ctx.Err())
		}
	}
}

func (db *destinationDB) upsertRow(t tableInfo, primaryKeys, data rowdata) error {
	n := len(data)
	cols := make([]string, n)
	insertParams := make([]string, n)
	updateParams := make([]string, n)
	values := make([]any, n)
	i := 0
	for k, v := range data {
		cols[i] = `"` + k + `"`
		insertParams[i] = fmt.Sprintf("$%d", i+1)
		updateParams[i] = fmt.Sprintf("$%d", i+1+n)
		values[i] = v
		i++
	}
	colStr := strings.Join(cols, ",")
	values = append(values, values...)

	pks := make([]string, len(primaryKeys))
	i = 0
	for k := range primaryKeys {
		pks[i] = `"` + k + `"`
		i++
	}

	_, err := db.db.Exec(
		fmt.Sprintf(
			`INSERT INTO "%s"."%s" (%s) VALUES (%s)
			ON CONFLICT (%s) DO UPDATE SET (%s) = ROW(%s)`,
			t.schema, t.name, colStr, strings.Join(insertParams, ","),
			strings.Join(pks, ","), colStr, strings.Join(updateParams, ","),
		),
		values...,
	)
	if err != nil {
		return fmt.Errorf("sql insert: %w", err)
	}

	return nil
}

func (db *destinationDB) deleteRow(t tableInfo, primaryKeys rowdata) error {
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

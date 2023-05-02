package main

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func Test_mssql_getTables(t *testing.T) {
	srcDB, _, _ := openTestDB(t)

	initDB := func(t *testing.T) {
		srcDB.db.MustExec(`
			CREATE SCHEMA schema1;
		`)
		srcDB.db.MustExec(`
			CREATE SCHEMA schema2;
		`)
		srcDB.db.MustExec(`
			CREATE TABLE schema1.table1(id INT PRIMARY KEY);
			CREATE TABLE schema2.table2(id VARCHAR(1000) PRIMARY KEY);
			CREATE TABLE schema2.table3(id INT PRIMARY KEY);
		`)

		ints := make([]string, 1000)
		texts := make([]string, 1000)
		for i := 0; i < 1000; i++ {
			ints[i] = fmt.Sprintf("(%d)", i+1)
			texts[i] = fmt.Sprintf("('%s')", uuid.New())
		}

		srcDB.db.MustExec(fmt.Sprintf(
			`INSERT INTO schema1.table1 VALUES %s;
			INSERT INTO schema2.table2 VALUES %s;
			INSERT INTO schema2.table2 VALUES %s;`,
			strings.Join(ints[:1000], ","),
			strings.Join(texts[:500], ","),
			strings.Join(ints[:500], ","),
		))

		t.Cleanup(func() {
			srcDB.db.MustExec(`
				DROP TABLE schema1.table1;
				DROP TABLE schema2.table2;
				DROP TABLE schema2.table3;
				DROP SCHEMA schema1;
				DROP SCHEMA schema2;
			`)
		})
	}

	t.Run("returns list of tables ordered by size asc", func(t *testing.T) {
		initDB(t)

		tables, err := srcDB.getTables(nil, nil)

		assert.NoError(t, err)
		assert.Equal(t,
			[]tableInfo{
				{schema: "schema2", name: "table3"},
				{schema: "schema1", name: "table1"},
				{schema: "schema2", name: "table2"},
			},
			tables,
		)
	})

	t.Run("able to put tables to the end of list", func(t *testing.T) {
		initDB(t)

		tables, err := srcDB.getTables([]string{"schema1.table1", "schema2.table3"}, nil)

		assert.NoError(t, err)
		assert.Equal(t,
			[]tableInfo{
				{schema: "schema2", name: "table2"},
				{schema: "schema1", name: "table1"},
				{schema: "schema2", name: "table3"},
			},
			tables,
		)
	})

	t.Run("able to exclude tables", func(t *testing.T) {
		initDB(t)
		srcDB.db.MustExec("CREATE TABLE schema1.table4 (id INT)")
		defer srcDB.db.MustExec("DROP TABLE schema1.table4")

		tables, err := srcDB.getTables(nil, []string{"schema1.table4"})

		assert.NoError(t, err)
		assert.Equal(t,
			[]tableInfo{
				{schema: "schema2", name: "table3"},
				{schema: "schema1", name: "table1"},
				{schema: "schema2", name: "table2"},
			},
			tables,
		)
	})
}

func Test_mssql_enableChangeTracking(t *testing.T) {
	srcDB, _, _ := openTestDB(t)
	table := tableInfo{schema: "test", name: "some_table"}

	initDB := func(t *testing.T) {
		srcDB.db.MustExec("CREATE SCHEMA test")
		srcDB.db.MustExec("CREATE TABLE test.some_table(id INT PRIMARY KEY)")

		t.Cleanup(func() {
			srcDB.db.MustExec(`
				DROP TABLE test.some_table;
				DROP SCHEMA test;
				ALTER DATABASE CURRENT SET CHANGE_TRACKING = OFF
			`)
		})
	}

	t.Run("enables change tracking config for database & table", func(t *testing.T) {
		initDB(t)

		err := srcDB.enableChangeTracking(table, 1)

		assert.NoError(t, err)

		var count int
		srcDB.db.QueryRowx("SELECT count(*) FROM sys.change_tracking_databases WHERE database_id = db_id()").Scan(&count)
		assert.Equal(t, 1, count)

		srcDB.db.QueryRowx("SELECT count(*) FROM sys.change_tracking_tables WHERE object_id = object_id('test.some_table')").Scan(&count)
		assert.Equal(t, 1, count)
	})

	t.Run("updates change tracking parameter when called with different ones", func(t *testing.T) {
		initDB(t)

		err := srcDB.enableChangeTracking(table, 1)

		data := getAllData(t, srcDB.db, tableInfo{schema: "sys", name: "change_tracking_databases"}, "database_id")
		assert.Equal(t, int64(1), data[0]["retention_period"])
		assert.NoError(t, err)

		err = srcDB.enableChangeTracking(table, 3)

		data = getAllData(t, srcDB.db, tableInfo{schema: "sys", name: "change_tracking_databases"}, "database_id")
		assert.Equal(t, int64(3), data[0]["retention_period"])
		assert.NoError(t, err)
	})
}

func Test_mssql_readRowsWithPK(t *testing.T) {
	srcDB, _, _ := openTestDB(t)
	table := tableInfo{schema: "test", name: "some_table"}

	initDB := func(t *testing.T) {
		srcDB.db.MustExec(`
			CREATE SCHEMA test;
		`)
		srcDB.db.MustExec(`
			CREATE TABLE test.some_table (
				id1  UNIQUEIDENTIFIER,
				id2  INT,
				val1 VARCHAR(15),
				val2 NUMERIC(2, 0),
				val3 DATE,
				val4 DATETIME,
				val5 DATETIME,
				val6 CHAR(1),
				PRIMARY KEY (id1, id2)
			);

			INSERT INTO test.some_table (id1, id2, val1, val2, val3, val4, val5, val6) VALUES
				('1a2b3c4d-5a6b-7c8d-9910-111213141516', 1, 'foo', 2, '2020-01-02', '2020-01-02T15:04:05Z', '2020-01-02T15:04:05Z', 'A'),
				('1a2b3c4d-5a6b-7c8d-9910-111213141517', 3, 'bar', 4, '2020-01-03', '2020-01-03T15:04:05Z', '2020-01-03T15:04:05Z', 'B');
		`)

		t.Cleanup(func() {
			srcDB.db.MustExec(`
				DROP TABLE test.some_table;
				DROP SCHEMA test;
			`)
		})
	}

	t.Run("reads data from source DB", func(t *testing.T) {
		initDB(t)
		ch := make(chan rowData, 2)

		err := srcDB.readRowsWithPK(context.Background(), table, nil, ch)

		assert.NoError(t, err)

		var rows []rowData
		close(ch)
		for rd := range ch {
			rows = append(rows, rd)
		}

		assert.Equal(t,
			[]rowData{
				{
					"id1":  "1a2b3c4d-5a6b-7c8d-9910-111213141516",
					"id2":  int64(1),
					"val1": "foo",
					"val2": float64(2),
					"val3": time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC),
					"val4": time.Date(2020, 1, 2, 15, 4, 5, 0, time.UTC),
					"val5": time.Date(2020, 1, 2, 15, 4, 5, 0, time.UTC),
					"val6": "A",
				},
				{
					"id1":  "1a2b3c4d-5a6b-7c8d-9910-111213141517",
					"id2":  int64(3),
					"val1": "bar",
					"val2": float64(4),
					"val3": time.Date(2020, 1, 3, 0, 0, 0, 0, time.UTC),
					"val4": time.Date(2020, 1, 3, 15, 4, 5, 0, time.UTC),
					"val5": time.Date(2020, 1, 3, 15, 4, 5, 0, time.UTC),
					"val6": "B",
				},
			},
			rows,
		)
	})

	t.Run("returns error when context is done", func(t *testing.T) {
		initDB(t)
		ch := make(chan rowData)
		ctx, cancel := context.WithCancel(context.Background())
		wait := make(chan struct{})

		go func() {
			err := srcDB.readRowsWithPK(ctx, table, nil, ch)

			assert.EqualError(t, err, "data read aborted, reason: context canceled")
			close(wait)
		}()

		time.Sleep(50 * time.Millisecond)
		cancel()
		<-wait
	})

	t.Run("able to read from the middle of data for table with single column PK", func(t *testing.T) {
		initDB(t)
		srcDB.db.MustExec(`
			CREATE TABLE test.other_table (
				id  INT PRIMARY KEY,
				val INT
			);
			INSERT INTO test.other_table VALUES
				(2, 3),
				(1, 4);
		`)
		t.Cleanup(func() {
			srcDB.db.MustExec("DROP TABLE test.other_table")
		})
		ch := make(chan rowData, 2)

		err := srcDB.readRowsWithPK(context.Background(), tableInfo{schema: "test", name: "other_table"}, 1, ch)

		assert.NoError(t, err)

		var rows []rowData
		close(ch)
		for rd := range ch {
			rows = append(rows, rd)
		}

		assert.Equal(t, []rowData{{"id": int64(2), "val": int64(3)}}, rows)

		ch = make(chan rowData, 2)

		err = srcDB.readRowsWithPK(context.Background(), tableInfo{schema: "test", name: "other_table"}, 2, ch)

		assert.NoError(t, err)

		rows = []rowData{}
		close(ch)
		for rd := range ch {
			rows = append(rows, rd)
		}

		assert.Empty(t, rows)
	})

	t.Run("returns error when trying to read from the middle of data when table has multi column PK", func(t *testing.T) {
		initDB(t)
		srcDB.db.MustExec(`
			CREATE TABLE test.other_table (
				id  INT,
				val INT,

				PRIMARY KEY (id, val)
			);
			INSERT INTO test.other_table VALUES
				(2, 3),
				(1, 4);
		`)
		t.Cleanup(func() {
			srcDB.db.MustExec("DROP TABLE test.other_table")
		})
		ch := make(chan rowData, 2)

		err := srcDB.readRowsWithPK(context.Background(), tableInfo{schema: "test", name: "other_table"}, 1, ch)

		assert.EqualError(t, err, "filtered read not supported on table with multi column primary key")
	})
}

func Test_mssql_getPrimaryKeys(t *testing.T) {
	srcDB, _, _ := openTestDB(t)

	initDB := func(t *testing.T) {
		srcDB.db.MustExec(`
			CREATE SCHEMA test;
		`)
		srcDB.db.MustExec(`
			CREATE TABLE test.some_table (
				id1  UNIQUEIDENTIFIER,
				id2  INT,
				val1 VARCHAR(15),
				val2 NUMERIC(2, 0),
				val3 DATE,
				val4 DATETIME,
				val5 DATETIME,
				val6 CHAR(1),
				PRIMARY KEY (id1, id2)
			);

			CREATE TABLE test.other_table (
				id  INT,
				val TEXT
			);
		`)

		t.Cleanup(func() {
			srcDB.db.MustExec(`
				DROP TABLE test.other_table;
				DROP TABLE test.some_table;
				DROP SCHEMA test;
			`)
		})
	}

	t.Run("returns table primary keys", func(t *testing.T) {
		initDB(t)

		res, err := srcDB.getPrimaryKeys(tableInfo{schema: "test", name: "some_table"})

		assert.NoError(t, err)
		assert.Equal(t, []string{"id1", "id2"}, res)

		res, err = srcDB.getPrimaryKeys(tableInfo{schema: "test", name: "other_table"})

		assert.NoError(t, err)
		assert.Empty(t, res)
	})
}

func Test_mssql_readTableChanges(t *testing.T) {
	srcDB, _, _ := openTestDB(t)
	table := tableInfo{schema: "test", name: "some_table"}

	initDB := func(t *testing.T) {
		srcDB.db.MustExec(`
			CREATE SCHEMA test;
		`)
		srcDB.db.MustExec(`
			CREATE TABLE test.some_table(
				id1     int,
				id2     int,
				content text,
				PRIMARY KEY (id1, id2)
			);

			INSERT INTO test.some_table VALUES
				(1, 2, 'foo'),
				(1, 3, 'bar');

			ALTER DATABASE CURRENT SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 14 DAYS, AUTO_CLEANUP = ON);
			ALTER TABLE test.some_table ENABLE CHANGE_TRACKING;
		`)

		t.Cleanup(func() {
			srcDB.db.MustExec(`
				DROP TABLE test.some_table;
				DROP SCHEMA test;
				ALTER DATABASE CURRENT SET CHANGE_TRACKING = OFF;
			`)
		})
	}

	t.Run("returns changetable entries", func(t *testing.T) {
		initDB(t)
		ch := make(chan rowChange, 3)

		n, err := srcDB.readRowChanges(context.Background(), table, 0, ch)

		assert.NoError(t, err)
		assert.Equal(t, uint(0), n)
		assert.Empty(t, ch)

		srcDB.db.MustExec(`
			INSERT INTO test.some_table VALUES (1, 4, 'baz');
			UPDATE test.some_table SET content = 'qux' WHERE id1 = 1 AND id2 = 2;
			DELETE FROM test.some_table WHERE id1 = 1 AND id2 = 3;
		`)

		n, err = srcDB.readRowChanges(context.Background(), table, 0, ch)

		assert.NoError(t, err)
		assert.Equal(t, uint(3), n)

		var entries []rowChange
		close(ch)
		for e := range ch {
			entries = append(entries, e)
		}

		assert.Len(t, entries, 3)
		assert.Contains(t, entries, rowChange{
			operation:   "I",
			primaryKeys: rowData{"id1": int64(1), "id2": int64(4)},
			rowdata:     rowData{"id1": int64(1), "id2": int64(4), "content": "baz"},
		})
		assert.Contains(t, entries, rowChange{
			operation:   "U",
			primaryKeys: rowData{"id1": int64(1), "id2": int64(2)},
			rowdata:     rowData{"id1": int64(1), "id2": int64(2), "content": "qux"},
		})
		assert.Contains(t, entries, rowChange{
			operation:   "D",
			primaryKeys: rowData{"id1": int64(1), "id2": int64(3)},
			rowdata:     nil,
		})
	})

	t.Run("returns error when context is done", func(t *testing.T) {
		initDB(t)
		ch := make(chan rowChange)
		ctx, cancel := context.WithCancel(context.Background())
		wait := make(chan struct{})

		srcDB.db.MustExec(`
			INSERT INTO test.some_table VALUES (1, 4, 'baz');
			UPDATE test.some_table SET content = 'qux' WHERE id1 = 1 AND id2 = 2;
			DELETE FROM test.some_table WHERE id1 = 1 AND id2 = 3;
		`)

		go func() {
			_, err := srcDB.readRowChanges(ctx, table, 0, ch)

			assert.EqualError(t, err, "read change table aborted, reason: context canceled")
			close(wait)
		}()

		time.Sleep(50 * time.Millisecond)
		cancel()
		<-wait
	})
}

func Test_mssql_getRow(t *testing.T) {
	srcDB, _, _ := openTestDB(t)
	table := tableInfo{schema: "test", name: "some_table"}

	initDB := func(t *testing.T) {
		srcDB.db.MustExec(`
			CREATE SCHEMA test;
		`)
		srcDB.db.MustExec(`
			CREATE TABLE test.some_table (
				id1  UNIQUEIDENTIFIER,
				id2  INT,
				val1 VARCHAR(15),
				val2 NUMERIC(2, 0),
				val3 DATE,
				val4 DATETIME,
				val5 DATETIME,
				val6 CHAR(1),
				PRIMARY KEY (id1, id2)
			);

			INSERT INTO test.some_table (id1, id2, val1, val2, val3, val4, val5, val6) VALUES
				('1a2b3c4d-5a6b-7c8d-9910-111213141516', 1, 'foo', 2, '2020-01-02', '2020-01-02T15:04:05Z', '2020-01-02T15:04:05Z', 'A'),
				('1a2b3c4d-5a6b-7c8d-9910-111213141517', 3, 'bar', 4, '2020-01-03', '2020-01-03T15:04:05Z', '2020-01-03T15:04:05Z', 'B');
		`)

		t.Cleanup(func() {
			srcDB.db.MustExec(`
				DROP TABLE test.some_table;
				DROP SCHEMA test;
			`)
		})
	}

	t.Run("returns existing row", func(t *testing.T) {
		initDB(t)

		row, err := srcDB.getRow(table, rowData{"id1": "1a2b3c4d-5a6b-7c8d-9910-111213141517", "id2": 3})

		assert.NoError(t, err)
		assert.Equal(t,
			rowData{
				"id1":  "1a2b3c4d-5a6b-7c8d-9910-111213141517",
				"id2":  int64(3),
				"val1": "bar",
				"val2": float64(4),
				"val3": time.Date(2020, time.January, 3, 0, 0, 0, 0, time.UTC),
				"val4": time.Date(2020, time.January, 3, 15, 4, 5, 0, time.UTC),
				"val5": time.Date(2020, time.January, 3, 15, 4, 5, 0, time.UTC),
				"val6": "B",
			},
			row,
		)
	})

	t.Run("returns nil without error on missing row", func(t *testing.T) {
		initDB(t)

		row, err := srcDB.getRow(table, rowData{"id1": "1a2b3c4d-5a6b-7c8d-9910-111213141511", "id2": 4})

		assert.NoError(t, err)
		assert.Nil(t, row)
	})
}

package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_destinationDB_getForeignKeys(t *testing.T) {
	_, dstDB, _ := openTestDB(t)

	initDB := func(t *testing.T) {
		dstDB.db.MustExec(`
			CREATE SCHEMA test;
			CREATE TABLE test.table1 (
				id INT PRIMARY KEY
			);
			CREATE TABLE test.table2 (
				id        INT PRIMARY KEY,
				table1_id INT CONSTRAINT table2_table1_fk REFERENCES test.table1(id)
			);
			CREATE TABLE test.table3 (
				id        INT PRIMARY KEY,
				table1_id INT CONSTRAINT table3_table1_fk REFERENCES test.table1(id)
			);

			CREATE TABLE table4 (
				id        INT PRIMARY KEY,
				table1_id INT CONSTRAINT table4_table1_fk REFERENCES test.table1(id)
			);
		`)

		t.Cleanup(func() {
			dstDB.db.MustExec(`
				DROP TABLE table4;
				DROP SCHEMA test CASCADE;
			`)
		})
	}

	t.Run("returns foreign keys info on dst DB", func(t *testing.T) {
		initDB(t)

		fks, err := dstDB.getForeignKeys()

		assert.NoError(t, err)
		assert.Equal(t,
			[]dstForeignKey{
				{
					t:          tableInfo{schema: "test", name: "table2"},
					name:       "table2_table1_fk",
					definition: "FOREIGN KEY (table1_id) REFERENCES test.table1(id)",
				},
				{
					t:          tableInfo{schema: "test", name: "table3"},
					name:       "table3_table1_fk",
					definition: "FOREIGN KEY (table1_id) REFERENCES test.table1(id)",
				},
				{
					t:          tableInfo{schema: "public", name: "table4"},
					name:       "table4_table1_fk",
					definition: "FOREIGN KEY (table1_id) REFERENCES test.table1(id)",
				},
			},
			fks,
		)
	})
}

func Test_destinationDB_dropForeignKeys(t *testing.T) {
	_, dstDB, _ := openTestDB(t)

	initDB := func(t *testing.T) {
		dstDB.db.MustExec(`
			CREATE SCHEMA test;
			CREATE TABLE test.table1 (
				id INT PRIMARY KEY
			);
			CREATE TABLE test.table2 (
				id        INT PRIMARY KEY,
				table1_id INT CONSTRAINT table2_table1_fk REFERENCES test.table1(id)
			);
			CREATE TABLE test.table3 (
				id        INT PRIMARY KEY,
				table1_id INT CONSTRAINT table3_table1_fk REFERENCES test.table1(id)
			)
		`)

		t.Cleanup(func() {
			dstDB.db.MustExec("DROP SCHEMA test CASCADE")
		})
	}

	countFKs := func() int {
		var count int
		dstDB.db.QueryRowx("SELECT count(*) FROM pg_constraint WHERE contype = 'f'").Scan(&count)
		return count
	}

	t.Run("drops given foreign keys", func(t *testing.T) {
		initDB(t)

		assert.Equal(t, 2, countFKs())

		err := dstDB.dropForeignKeys([]dstForeignKey{
			{t: tableInfo{schema: "test", name: "table2"}, name: "table2_table1_fk"},
			{t: tableInfo{schema: "test", name: "table3"}, name: "table3_table1_fk"},
			{t: tableInfo{schema: "test", name: "table3"}, name: "table3_table2_fake_fk"},
		})

		assert.NoError(t, err)
		assert.Equal(t, 0, countFKs())
	})
}

func Test_destinationDB_insertRows(t *testing.T) {
	_, dstDB, _ := openTestDB(t)
	table := tableInfo{schema: "test", name: "some_table"}

	initDB := func(t *testing.T) {
		dstDB.db.MustExec(`
			CREATE SCHEMA test;
			CREATE TABLE test.some_table (
				id1  UUID,
				id2  INT,
				val1 TEXT,
				val2 INT2,
				val3 TIMESTAMP,
				val4 DATE,
				val5 TIMESTAMP,
				val6 CHAR(1),
				PRIMARY KEY (id1, id2)
			);
		`)

		t.Cleanup(func() {
			dstDB.db.MustExec("DROP SCHEMA test CASCADE")
		})
	}

	t.Run("truncating existing table & writes given input data", func(t *testing.T) {
		initDB(t)
		dstDB.db.MustExec("INSERT INTO test.some_table (id1, id2) VALUES ('1a2b3c4d-5a6b-7c8d-9910-111213141516', 13)")
		ch := make(chan rowdata, 3)
		ch <- rowdata{
			"id1":  "1a2b3c4d-5a6b-7c8d-9910-111213141516",
			"id2":  1,
			"val1": "foo",
			"val2": 2,
			"val3": time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC),
			"val4": time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC),
			"val5": time.Date(2020, 1, 2, 15, 4, 5, 0, time.UTC),
			"val6": "A",
		}
		ch <- rowdata{
			"id1":  "1a2b3c4d-5a6b-7c8d-9910-111213141517",
			"id2":  3,
			"val1": "bar",
			"val2": 4,
			"val3": time.Date(2020, 1, 3, 0, 0, 0, 0, time.UTC),
			"val4": time.Date(2020, 1, 3, 0, 0, 0, 0, time.UTC),
			"val5": time.Date(2020, 1, 3, 15, 4, 5, 0, time.UTC),
			"val6": "B",
		}
		close(ch)

		err := dstDB.insertRows(context.Background(), table, ch)

		assert.NoError(t, err)
		assert.Equal(t,
			[]rowdata{
				{
					"id1":  []byte("1a2b3c4d-5a6b-7c8d-9910-111213141516"),
					"id2":  int64(1),
					"val1": "foo",
					"val2": int64(2),
					"val3": time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC),
					"val4": time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC),
					"val5": time.Date(2020, 1, 2, 15, 4, 5, 0, time.UTC),
					"val6": []byte("A"),
				},
				{
					"id1":  []byte("1a2b3c4d-5a6b-7c8d-9910-111213141517"),
					"id2":  int64(3),
					"val1": "bar",
					"val2": int64(4),
					"val3": time.Date(2020, 1, 3, 0, 0, 0, 0, time.UTC),
					"val4": time.Date(2020, 1, 3, 0, 0, 0, 0, time.UTC),
					"val5": time.Date(2020, 1, 3, 15, 4, 5, 0, time.UTC),
					"val6": []byte("B"),
				},
			},
			getAllData(t, dstDB.db, table, "id1 ASC, id2 ASC"),
		)
	})

	t.Run("returns error when context is done", func(t *testing.T) {
		initDB(t)
		ch := make(chan rowdata)
		ctx, cancel := context.WithCancel(context.Background())
		wait := make(chan struct{})

		go func() {
			err := dstDB.insertRows(ctx, table, ch)

			assert.EqualError(t, err, "insert data aborted, reason: context canceled")
			close(wait)
		}()

		time.Sleep(50 * time.Millisecond)
		cancel()
		<-wait
	})
}

func Test_destinationDB_writeTableChanges(t *testing.T) {
	_, dstDB, _ := openTestDB(t)
	table := tableInfo{schema: "test", name: "some_table"}

	initDB := func(t *testing.T) {
		dstDB.db.MustExec(`
			CREATE SCHEMA test;
			CREATE TABLE test.some_table (
				id1 INT,
				id2 INT,
				val TEXT,
				PRIMARY KEY (id1, id2)
			);

			INSERT INTO test.some_table VALUES
				(1, 2, 'foo'),
				(3, 4, 'bar');
		`)

		t.Cleanup(func() {
			dstDB.db.MustExec("DROP SCHEMA test CASCADE")
		})
	}

	t.Run("writes table changes to dst DB", func(t *testing.T) {
		initDB(t)
		ch := make(chan tablechange, 5)
		ch <- tablechange{
			operation:   "I",
			primaryKeys: rowdata{"id1": 1, "id2": 5},
			rowdata:     rowdata{"id1": 1, "id2": 5, "val": "baz"},
		}
		ch <- tablechange{
			operation:   "U",
			primaryKeys: rowdata{"id1": 1, "id2": 2},
			rowdata:     rowdata{"id1": 1, "id2": 2, "val": "qux"},
		}
		ch <- tablechange{
			operation:   "D",
			primaryKeys: rowdata{"id1": 3, "id2": 4},
			rowdata:     nil,
		}
		ch <- tablechange{ // Data deleted from source after table change is queried.
			operation:   "I",
			primaryKeys: rowdata{"id1": 1, "id2": 6},
			rowdata:     nil,
		}
		ch <- tablechange{ // Repeat operation (e.g. caused by previously failed iteration).
			operation:   "I",
			primaryKeys: rowdata{"id1": 1, "id2": 5},
			rowdata:     rowdata{"id1": 1, "id2": 5, "val": "baz"},
		}
		close(ch)

		n, err := dstDB.writeTableChanges(context.Background(), table, ch)

		assert.NoError(t, err)
		assert.Equal(t, uint(4), n)
		assert.Equal(t,
			[]rowdata{
				{"id1": int64(1), "id2": int64(2), "val": "qux"},
				{"id1": int64(1), "id2": int64(5), "val": "baz"},
			},
			getAllData(t, dstDB.db, table, "id1, id2"),
		)
	})

	t.Run("returns error when context is done", func(t *testing.T) {
		initDB(t)
		ch := make(chan tablechange)
		ctx, cancel := context.WithCancel(context.Background())
		wait := make(chan struct{})

		go func() {
			_, err := dstDB.writeTableChanges(ctx, table, ch)

			assert.EqualError(t, err, "write change table aborted, reason: context canceled")
			close(wait)
		}()

		time.Sleep(50 * time.Millisecond)
		cancel()
		<-wait
	})
}

func Test_destinationDB_upsertRow(t *testing.T) {
	_, dstDB, _ := openTestDB(t)
	table := tableInfo{schema: "test", name: "some_table"}

	initDB := func(t *testing.T) {
		dstDB.db.MustExec(`
			CREATE SCHEMA test;

			CREATE TABLE test.some_table(
				id1     int,
				id2     int,
				content text,
				PRIMARY KEY (id1, id2)
			);

			INSERT INTO test.some_table VALUES
				(1, 2, 'foo'),
				(1, 3, 'bar');
		`)

		t.Cleanup(func() {
			dstDB.db.MustExec("DROP SCHEMA test CASCADE")
		})
	}

	t.Run("inserts new row", func(t *testing.T) {
		initDB(t)

		err := dstDB.upsertRow(
			table,
			rowdata{"id1": 1, "id2": 4},
			rowdata{"id1": 1, "id2": 4, "content": "baz"},
		)

		assert.NoError(t, err)
		assert.Equal(t,
			[]rowdata{
				{"id1": int64(1), "id2": int64(2), "content": "foo"},
				{"id1": int64(1), "id2": int64(3), "content": "bar"},
				{"id1": int64(1), "id2": int64(4), "content": "baz"},
			},
			getAllData(t, dstDB.db, table, "id1 ASC, id2 ASC"),
		)
	})

	t.Run("updates existing row", func(t *testing.T) {
		initDB(t)

		err := dstDB.upsertRow(
			table,
			rowdata{"id1": 1, "id2": 2},
			rowdata{"id1": 1, "id2": 2, "content": "baz"},
		)

		assert.NoError(t, err)
		assert.Equal(t,
			[]rowdata{
				{"id1": int64(1), "id2": int64(2), "content": "baz"},
				{"id1": int64(1), "id2": int64(3), "content": "bar"},
			},
			getAllData(t, dstDB.db, table, "id1 ASC, id2 ASC"),
		)
	})
}

func Test_destinationDB_deleteRow(t *testing.T) {
	_, dstDB, _ := openTestDB(t)
	table := tableInfo{schema: "test", name: "some_table"}

	initDB := func(t *testing.T) {
		dstDB.db.MustExec(`
			CREATE SCHEMA test;
			CREATE TABLE test.some_table(
				id1 int,
				id2 int,
				PRIMARY KEY (id1, id2)
			);

			INSERT INTO test.some_table VALUES
				(1, 2),
				(1, 3);
		`)

		t.Cleanup(func() {
			dstDB.db.MustExec("DROP SCHEMA test CASCADE")
		})
	}

	t.Run("deletes existing data", func(t *testing.T) {
		initDB(t)

		err := dstDB.deleteRow(table, rowdata{"id1": 1, "id2": 2})

		assert.NoError(t, err)
		assert.Equal(t,
			[]rowdata{
				{"id1": int64(1), "id2": int64(3)},
			},
			getAllData(t, dstDB.db, table, "id1 ASC, id2 ASC"),
		)
	})

	t.Run("does not return error when deleting non-existent data", func(t *testing.T) {
		initDB(t)

		err := dstDB.deleteRow(table, rowdata{"id1": 2, "id2": 3})

		assert.NoError(t, err)
		assert.Equal(t,
			[]rowdata{
				{"id1": int64(1), "id2": int64(2)},
				{"id1": int64(1), "id2": int64(3)},
			},
			getAllData(t, dstDB.db, table, "id1 ASC, id2 ASC"),
		)
	})
}

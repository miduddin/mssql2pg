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

func Test_destinationDB_getPrimaryKeys(t *testing.T) {
	_, dstDB, _ := openTestDB(t)

	initDB := func(t *testing.T) {
		dstDB.db.MustExec(`
			CREATE SCHEMA test;
			CREATE TABLE test.table1 (
				id1 INT,
				id2 TEXT,
				id3 INT,
				val INT,

				PRIMARY KEY (id2, id1, id3)
			);

			CREATE TABLE table2 (
				id  INT PRIMARY KEY,
				val INT
			);
		`)

		t.Cleanup(func() {
			dstDB.db.MustExec(`
				DROP SCHEMA test CASCADE;
				DROP TABLE table2;
			`)
		})
	}

	t.Run("returns table primary keys", func(t *testing.T) {
		initDB(t)

		pks, err := dstDB.getPrimaryKeys(tableInfo{schema: "test", name: "table1"})

		assert.NoError(t, err)
		assert.Equal(t, []string{"id2", "id1", "id3"}, pks)

		pks, err = dstDB.getPrimaryKeys(tableInfo{schema: "public", name: "table2"})

		assert.NoError(t, err)
		assert.Equal(t, []string{"id"}, pks)
	})
}

func Test_destinationDB_getIndexes(t *testing.T) {
	_, dstDB, _ := openTestDB(t)
	table := tableInfo{schema: "test", name: "table1"}

	initDB := func(t *testing.T) {
		dstDB.db.MustExec(`
			CREATE SCHEMA test;
			CREATE TABLE test.table1 (
				id  INT PRIMARY KEY,
				val INT
			);
			CREATE TABLE test.table2 (
				id  INT PRIMARY KEY,
				val INT
			);
			CREATE TABLE test.table3 (
				id  INT,
				val INT
			);

			CREATE INDEX index1 ON test.table1 (id, val);
			CREATE INDEX index2 ON test.table1 (val);
			CREATE INDEX index3 ON test.table2 (val);
		`)

		t.Cleanup(func() {
			dstDB.db.MustExec("DROP SCHEMA test CASCADE")
		})
	}

	t.Run("returns indexes of a given table", func(t *testing.T) {
		initDB(t)

		ixs, err := dstDB.getIndexes(table)

		assert.NoError(t, err)
		assert.Equal(t,
			[]dstIndex{
				{t: table, name: "index1", def: "CREATE INDEX index1 ON test.table1 USING btree (id, val)"},
				{t: table, name: "index2", def: "CREATE INDEX index2 ON test.table1 USING btree (val)"},
				{t: table, name: "table1_pkey", def: "CREATE UNIQUE INDEX table1_pkey ON test.table1 USING btree (id)"},
			},
			ixs,
		)
	})

	t.Run("returns nothing if given table does not have any index", func(t *testing.T) {
		initDB(t)

		ixs, err := dstDB.getIndexes(tableInfo{schema: "test", name: "table3"})

		assert.NoError(t, err)
		assert.Empty(t, ixs)
	})

	t.Run("returns nothing if given table is not found", func(t *testing.T) {
		initDB(t)

		ixs, err := dstDB.getIndexes(tableInfo{schema: "test", name: "table4"})

		assert.NoError(t, err)
		assert.Empty(t, ixs)
	})
}

func Test_destinationDB_dropIndexes(t *testing.T) {
	_, dstDB, _ := openTestDB(t)
	table := tableInfo{schema: "test", name: "table1"}

	initDB := func(t *testing.T) {
		dstDB.db.MustExec(`
			CREATE SCHEMA test;
			CREATE TABLE test.table1 (
				id  INT PRIMARY KEY,
				val INT
			);
			CREATE TABLE test.table2 (
				id  INT PRIMARY KEY,
				val INT
			);
			CREATE TABLE test.table3 (
				id  INT,
				val INT
			);

			CREATE INDEX index1 ON test.table1 (id, val);
			CREATE INDEX index2 ON test.table1 (val);
			CREATE INDEX index3 ON test.table2 (val);
		`)

		t.Cleanup(func() {
			dstDB.db.MustExec("DROP SCHEMA test CASCADE")
		})
	}

	countIndexes := func(t tableInfo) int {
		var count int
		dstDB.db.QueryRowx(
			"SELECT count(*) FROM pg_indexes WHERE schemaname = $1 AND tablename = $2",
			t.schema, t.name,
		).Scan(&count)
		return count
	}

	t.Run("drops given indexes", func(t *testing.T) {
		initDB(t)

		assert.Equal(t, 3, countIndexes(table))

		err := dstDB.dropIndexes([]dstIndex{
			{t: tableInfo{schema: "test"}, name: "table1_pkey"},
			{t: tableInfo{schema: "test"}, name: "index1"},
			{t: tableInfo{schema: "test"}, name: "index2"},
		})

		assert.NoError(t, err)
		assert.Equal(t, 1, countIndexes(table))
	})
}

func Test_destinationDB_createIndexes(t *testing.T) {
	_, dstDB, _ := openTestDB(t)
	table := tableInfo{schema: "test", name: "table1"}

	initDB := func(t *testing.T) {
		dstDB.db.MustExec(`
			CREATE SCHEMA test;
			CREATE TABLE test.table1 (
				id  INT PRIMARY KEY,
				val INT
			);
			CREATE TABLE test.table2 (
				id  INT PRIMARY KEY,
				val INT
			);
			CREATE TABLE test.table3 (
				id  INT,
				val INT
			);
		`)

		t.Cleanup(func() {
			dstDB.db.MustExec("DROP SCHEMA test CASCADE")
		})
	}

	countIndexes := func(t tableInfo) int {
		var count int
		dstDB.db.QueryRowx(
			"SELECT count(*) FROM pg_indexes WHERE schemaname = $1 AND tablename = $2",
			t.schema, t.name,
		).Scan(&count)
		return count
	}

	t.Run("creates given indexes", func(t *testing.T) {
		initDB(t)

		assert.Equal(t, 1, countIndexes(table))

		err := dstDB.createIndexes(context.Background(), []dstIndex{
			{def: "CREATE INDEX index1 ON test.table1 USING btree (id, val)"},
			{def: "CREATE INDEX index2 ON test.table1 USING btree (val)"},
			{def: "CREATE UNIQUE INDEX table1_pkey ON test.table1 USING btree (id)"},
		})

		assert.NoError(t, err)
		assert.Equal(t, 3, countIndexes(table))
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

		_, err := dstDB.insertRows(context.Background(), table, true, 10, ch)

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
			_, err := dstDB.insertRows(ctx, table, true, 10, ch)

			assert.EqualError(t, err, "insert data aborted, reason: context canceled")
			close(wait)
		}()

		time.Sleep(50 * time.Millisecond)
		cancel()
		<-wait
	})

	t.Run("only truncates table when input is empty", func(t *testing.T) {
		initDB(t)
		dstDB.db.MustExec("INSERT INTO test.some_table (id1, id2) VALUES ('1a2b3c4d-5a6b-7c8d-9910-111213141516', 13)")
		ch := make(chan rowdata, 3)
		close(ch)

		_, err := dstDB.insertRows(context.Background(), table, true, 10, ch)

		assert.NoError(t, err)
		assert.Equal(t,
			[]rowdata{},
			getAllData(t, dstDB.db, table, "id1 ASC, id2 ASC"),
		)
	})

	t.Run("able to process large amount of input in batch", func(t *testing.T) {
		initDB(t)
		dstDB.db.MustExec(`CREATE TABLE test.more_table(id INT)`)
		ch := make(chan rowdata)
		go func() {
			for i := 0; i < 212; i++ {
				ch <- rowdata{"id": i + 1}
			}
			close(ch)
		}()

		_, err := dstDB.insertRows(context.Background(), tableInfo{schema: "test", name: "more_table"}, true, 100, ch)

		assert.NoError(t, err)

		data := getAllData(t, dstDB.db, tableInfo{schema: "test", name: "more_table"}, "id")
		assert.Len(t, data, 212)
		for i, r := range data {
			assert.Equal(t, rowdata{"id": int64(i + 1)}, r)
		}
	})

	t.Run("able to insert data without truncating existing data", func(t *testing.T) {
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

		_, err := dstDB.insertRows(context.Background(), table, false, 10, ch)

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
					"id1":  []byte("1a2b3c4d-5a6b-7c8d-9910-111213141516"),
					"id2":  int64(13),
					"val1": nil,
					"val2": nil,
					"val3": nil,
					"val4": nil,
					"val5": nil,
					"val6": nil,
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

	t.Run("does not last inserted ID if table has multi column PK", func(t *testing.T) {
		initDB(t)
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

		lastID, err := dstDB.insertRows(context.Background(), table, true, 10, ch)

		assert.NoError(t, err)
		assert.Nil(t, lastID)
	})

	t.Run("returns last inserted ID if table has single column PK", func(t *testing.T) {
		initDB(t)
		dstDB.db.MustExec("CREATE TABLE test.more_table(id uuid PRIMARY KEY, val text)")

		ch := make(chan rowdata, 3)
		ch <- rowdata{"id": "1a2b3c4d-5a6b-7c8d-9910-111213141517", "val": "foo"}
		ch <- rowdata{"id": "1a2b3c4d-5a6b-7c8d-9910-111213141516", "val": "bar"}
		close(ch)

		lastID, err := dstDB.insertRows(context.Background(), tableInfo{schema: "test", name: "more_table"}, true, 10, ch)

		assert.NoError(t, err)
		assert.Equal(t, "1a2b3c4d-5a6b-7c8d-9910-111213141516", lastID)
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

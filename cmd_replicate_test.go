package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
)

func Test_cmdReplicate_start(t *testing.T) {
	srcDB, dstDB, metaDB := openTestDB(t)
	cmd := newCmdReplicate(srcDB, dstDB, metaDB, nil, nil, 10, 1, runMetricsClient(t))
	cmd.changeTrackingCopyMinInterval = 50 * time.Millisecond

	initDB := func(t *testing.T) {
		srcDB.db.MustExec(`
			CREATE SCHEMA s1;
		`)
		srcDB.db.MustExec(`
			CREATE SCHEMA s2;
		`)
		srcDB.db.MustExec(`
			CREATE TABLE s1.t1 (
				id INT PRIMARY KEY
			);

			CREATE TABLE s1.t2 (
				id INT PRIMARY KEY,
				val TEXT
			);

			CREATE TABLE s2.t3 (
				id1  UNIQUEIDENTIFIER,
				id2  INT,
				val1 VARCHAR(15),
				val2 NUMERIC(2, 0),
				val3 DATE,
				val4 DATETIME,
				val5 DATETIME,
				val6 CHAR(1),
				val7 INT CONSTRAINT t3_t2_fk REFERENCES s1.t2(id),
				PRIMARY KEY (id1, id2)
			);

			INSERT INTO s1.t1 VALUES
				(1);

			INSERT INTO s1.t2 VALUES
				(2, 'lorem'),
				(3, 'ipsum'),
				(4, 'dolor');

			INSERT INTO s2.t3 (id1, id2, val1, val2, val3, val4, val5, val6, val7) VALUES
				('1a2b3c4d-5a6b-7c8d-9910-111213141516', 1, 'foo', 2, '2020-01-02', '2020-01-02T15:04:05Z', '2020-01-02T15:04:05Z', 'A', 2),
				('1a2b3c4d-5a6b-7c8d-9910-111213141517', 3, 'bar', 4, '2020-01-03', '2020-01-03T15:04:05Z', '2020-01-03T15:04:05Z', 'B', 4);

			ALTER DATABASE CURRENT SET CHANGE_TRACKING = ON
				(CHANGE_RETENTION = 14 DAYS, AUTO_CLEANUP = ON);

			ALTER TABLE s1.t1 ENABLE CHANGE_TRACKING;
			ALTER TABLE s1.t2 ENABLE CHANGE_TRACKING;
			ALTER TABLE s2.t3 ENABLE CHANGE_TRACKING;
		`)

		dstDB.db.MustExec(`
			CREATE SCHEMA s1;
			CREATE SCHEMA s2;

			CREATE TABLE s1.t1 (
				id int PRIMARY KEY
			);

			CREATE TABLE s1.t2 (
				id int PRIMARY KEY,
				val text
			);

			CREATE TABLE s2.t3 (
				id1  uuid,
				id2  int,
				val1 text,
				val2 int2,
				val3 timestamp,
				val4 date,
				val5 timestamp,
				val6 char(1),
				val7 int CONSTRAINT t3_t2_fk REFERENCES s1.t2(id),
				PRIMARY KEY (id1, id2)
			)
		`)

		t.Cleanup(func() {
			srcDB.db.MustExec(`
				DROP TABLE s2.t3;
				DROP TABLE s1.t2;
				DROP TABLE s1.t1;
				DROP SCHEMA s2;
				DROP SCHEMA s1;

				ALTER DATABASE CURRENT SET CHANGE_TRACKING = OFF;
			`)

			dstDB.db.MustExec(`
				DROP SCHEMA s2 CASCADE;
				DROP SCHEMA s1 CASCADE;
			`)

			metaDB.db.MustExec(`
				DELETE FROM replication_progress;
				DELETE FROM foreign_keys;
			`)
		})
	}

	t.Run("replicates data from source to destination DB", func(t *testing.T) {
		initDB(t)
		ctx, cancel := context.WithCancel(context.Background())
		wait := make(chan struct{})

		go func() {
			err := cmd.start(ctx)

			assert.NoError(t, err)
			close(wait)
		}()

		time.Sleep(400 * time.Millisecond)

		assert.Equal(t,
			[]rowData{
				{"id": int64(1)},
			},
			getAllData(t, dstDB.db, tableInfo{schema: "s1", name: "t1"}, "id"),
		)

		assert.Equal(t,
			[]rowData{
				{"id": int64(2), "val": "lorem"},
				{"id": int64(3), "val": "ipsum"},
				{"id": int64(4), "val": "dolor"},
			},
			getAllData(t, dstDB.db, tableInfo{schema: "s1", name: "t2"}, "id"),
		)

		assert.Equal(t,
			[]rowData{
				{
					"id1":  []byte("1a2b3c4d-5a6b-7c8d-9910-111213141516"),
					"id2":  int64(1),
					"val1": "foo",
					"val2": int64(2),
					"val3": time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC),
					"val4": time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC),
					"val5": time.Date(2020, 1, 2, 15, 4, 5, 0, time.UTC),
					"val6": []byte("A"),
					"val7": int64(2),
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
					"val7": int64(4),
				},
			},
			getAllData(t, dstDB.db, tableInfo{schema: "s2", name: "t3"}, "id1, id2"),
		)

		srcDB.db.MustExec(`
			INSERT INTO s1.t1 VALUES (2);
			UPDATE s2.t3 SET val1 = 'baz' WHERE id1 = '1a2b3c4d-5a6b-7c8d-9910-111213141516';
			DELETE FROM s1.t2 WHERE id = 3;
		`)

		time.Sleep(100 * time.Millisecond)

		assert.Equal(t,
			[]rowData{
				{"id": int64(1)},
				{"id": int64(2)},
			},
			getAllData(t, dstDB.db, tableInfo{schema: "s1", name: "t1"}, "id"),
		)

		assert.Equal(t,
			[]rowData{
				{"id": int64(2), "val": "lorem"},
				{"id": int64(4), "val": "dolor"},
			},
			getAllData(t, dstDB.db, tableInfo{schema: "s1", name: "t2"}, "id"),
		)

		assert.Equal(t,
			[]rowData{
				{
					"id1":  []byte("1a2b3c4d-5a6b-7c8d-9910-111213141516"),
					"id2":  int64(1),
					"val1": "baz",
					"val2": int64(2),
					"val3": time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC),
					"val4": time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC),
					"val5": time.Date(2020, 1, 2, 15, 4, 5, 0, time.UTC),
					"val6": []byte("A"),
					"val7": int64(2),
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
					"val7": int64(4),
				},
			},
			getAllData(t, dstDB.db, tableInfo{schema: "s2", name: "t3"}, "id1, id2"),
		)

		cancel()
		<-wait
	})

	t.Run("able to copy changes from when the program is not running", func(t *testing.T) {
		initDB(t)
		row, _ := srcDB.db.QueryRowx(`SELECT CHANGE_TRACKING_CURRENT_VERSION()`).SliceScan()
		metaDB.db.MustExec(
			`INSERT INTO replication_progress (schema_name, table_name, initial_copy_done, change_tracking_last_version)
			VALUES ('s1', 't1', 1, ?);`,
			row...,
		)
		srcDB.db.MustExec(`
			INSERT INTO s1.t1 VALUES (2), (3);
			UPDATE s1.t1 SET id = 4 WHERE id = 2;
			DELETE FROM s1.t1 WHERE id = 1;
		`)
		ctx, cancel := context.WithCancel(context.Background())
		wait := make(chan struct{})

		go func() {
			err := cmd.start(ctx)

			assert.NoError(t, err)
			close(wait)
		}()

		time.Sleep(400 * time.Millisecond)

		assert.Equal(t,
			[]rowData{
				{"id": int64(3)},
				{"id": int64(4)},
			},
			getAllData(t, dstDB.db, tableInfo{schema: "s1", name: "t1"}, "id"),
		)

		cancel()
		<-wait
	})
}

func Test_cmdReplicate_copyInitial(t *testing.T) {
	srcDB, dstDB, metaDB := openTestDB(t)
	cmd := newCmdReplicate(srcDB, dstDB, metaDB, nil, nil, 10, 1, runMetricsClient(t))
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
				val7 TEXT,
				PRIMARY KEY (id1, id2)
			);

			INSERT INTO test.some_table (id1, id2, val1, val2, val3, val4, val5, val6, val7) VALUES
				('1a2b3c4d-5a6b-7c8d-9910-111213141516', 1, 'foo', 2, '2020-01-02', '2020-01-02T15:04:05Z', '2020-01-02T15:04:05Z', 'A', 'lorem'),
				('1a2b3c4d-5a6b-7c8d-9910-111213141517', 3, 'bar', 4, '2020-01-03', '2020-01-03T15:04:05Z', '2020-01-03T15:04:05Z', 'B', 'ipsum'+char(0));

			ALTER DATABASE CURRENT SET CHANGE_TRACKING = ON
				(CHANGE_RETENTION = 1 DAYS, AUTO_CLEANUP = ON);

			ALTER TABLE test.some_table ENABLE CHANGE_TRACKING;
		`)

		dstDB.db.MustExec(`
			CREATE SCHEMA test;
			CREATE TABLE test.some_table (
				id1  uuid,
				id2  int,
				val1 text,
				val2 int2,
				val3 timestamp,
				val4 date,
				val5 timestamp,
				val6 char(1),
				val7 text,
				PRIMARY KEY (id1, id2)
			);

			CREATE UNIQUE INDEX index_id2 ON test.some_table (id2);
			CREATE INDEX index_val3 ON test.some_Table (val3);

			INSERT INTO test.some_table (id1, id2) VALUES
				('1a2b3c4d-5a6b-7c8d-9910-111213141518', 4),
				('1a2b3c4d-5a6b-7c8d-9910-111213141519', 5);
		`)

		t.Cleanup(func() {
			srcDB.db.MustExec(`
				DROP TABLE test.some_table;
				DROP SCHEMA test;

				ALTER DATABASE CURRENT SET CHANGE_TRACKING = OFF;
			`)

			dstDB.db.MustExec(`
				DROP SCHEMA test CASCADE
			`)

			metaDB.db.MustExec(`
				DELETE FROM replication_progress;
			`)
		})
	}

	t.Run("truncates existing table & copies data from source to destination", func(t *testing.T) {
		initDB(t)

		err := cmd.copyInitial(context.Background(), table)

		assert.NoError(t, err)
		assert.Equal(t,
			[]rowData{
				{
					"id1":  []byte("1a2b3c4d-5a6b-7c8d-9910-111213141516"),
					"id2":  int64(1),
					"val1": "foo",
					"val2": int64(2),
					"val3": time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC),
					"val4": time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC),
					"val5": time.Date(2020, 1, 2, 15, 4, 5, 0, time.UTC),
					"val6": []byte("A"),
					"val7": "lorem",
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
					"val7": "ipsum",
				},
			},
			getAllData(t, dstDB.db, table, "id1, id2"),
		)
	})

	t.Run("able to resume partionally done copy for single column table (uuid PK)", func(t *testing.T) {
		initDB(t)
		srcDB.db.MustExec(`
			CREATE TABLE test.more_table (
				id UNIQUEIDENTIFIER PRIMARY KEY,
				val INT
			);
			INSERT INTO test.more_table VALUES
				('1a2b3c4c-5a6b-7c8d-9910-111213141519', 1),
				('1a2b3c4d-5a6b-7c8d-9910-111213141518', 2);
		`)
		t.Cleanup(func() {
			srcDB.db.MustExec("DROP TABLE test.more_table")
		})
		dstDB.db.MustExec(`
			CREATE TABLE test.more_table (
				id uuid PRIMARY KEY,
				val INT
			);
		`)
		metaDB.db.MustExec(`
			INSERT INTO replication_progress (schema_name, table_name, initial_copy_last_id)
			VALUES ('test', 'more_table', '1a2b3c4d-5a6b-7c8d-9910-111213141518')
		`)

		err := cmd.copyInitial(context.Background(), tableInfo{schema: "test", name: "more_table"})

		assert.NoError(t, err)
		assert.Equal(t,
			[]rowData{
				{"id": []byte("1a2b3c4c-5a6b-7c8d-9910-111213141519"), "val": int64(1)},
			},
			getAllData(t, dstDB.db, tableInfo{schema: "test", name: "more_table"}, "id"),
		)
	})

	t.Run("able to resume partionally done copy for single column table (int PK)", func(t *testing.T) {
		initDB(t)
		srcDB.db.MustExec(`
			CREATE TABLE test.more_table (
				id INT PRIMARY KEY,
				val INT
			);
			INSERT INTO test.more_table VALUES
				(2, 3),
				(1, 4);
		`)
		t.Cleanup(func() {
			srcDB.db.MustExec("DROP TABLE test.more_table")
		})
		dstDB.db.MustExec(`
			CREATE TABLE test.more_table (
				id int PRIMARY KEY,
				val int
			);
		`)
		metaDB.db.MustExec(`
			INSERT INTO replication_progress (schema_name, table_name, initial_copy_last_id)
			VALUES ('test', 'more_table', 1)
		`)

		err := cmd.copyInitial(context.Background(), tableInfo{schema: "test", name: "more_table"})

		assert.NoError(t, err)
		assert.Equal(t,
			[]rowData{
				{"id": int64(2), "val": int64(3)},
			},
			getAllData(t, dstDB.db, tableInfo{schema: "test", name: "more_table"}, "id"),
		)
	})

	t.Run("marks table as processed when initial copy is done", func(t *testing.T) {
		initDB(t)

		err := cmd.copyInitial(context.Background(), table)

		assert.NoError(t, err)
		assert.NoError(t, err)

		row, _ := metaDB.db.QueryRowx(`
			SELECT initial_copy_done FROM replication_progress
			WHERE schema_name = 'test' AND table_name = 'some_table';
		`).SliceScan()

		assert.Equal(t, []any{int64(1)}, row)
	})

	t.Run("skips copy if table is already marked as processed", func(t *testing.T) {
		initDB(t)
		metaDB.db.MustExec(`
			INSERT INTO replication_progress (schema_name, table_name, initial_copy_done)
			VALUES ('test', 'some_table', 1)
		`)

		err := cmd.copyInitial(context.Background(), table)

		assert.NoError(t, err)
		assert.Equal(t,
			[]rowData{
				{
					"id1":  []byte("1a2b3c4d-5a6b-7c8d-9910-111213141518"),
					"id2":  int64(4),
					"val1": nil,
					"val2": nil,
					"val3": nil,
					"val4": nil,
					"val5": nil,
					"val6": nil,
					"val7": nil,
				},
				{
					"id1":  []byte("1a2b3c4d-5a6b-7c8d-9910-111213141519"),
					"id2":  int64(5),
					"val1": nil,
					"val2": nil,
					"val3": nil,
					"val4": nil,
					"val5": nil,
					"val6": nil,
					"val7": nil,
				},
			},
			getAllData(t, dstDB.db, table, "id1, id2"),
		)
	})

	t.Run("does not modify indexes", func(t *testing.T) {
		initDB(t)

		before, err := cmd.dstDB.getIndexes(table)
		assert.NoError(t, err)

		err = cmd.copyInitial(context.Background(), table)

		after, _ := cmd.dstDB.getIndexes(table)

		assert.NoError(t, err)
		assert.Equal(t, before, after)
	})
}

func Test_cmdReplicate_copyChangeTracking(t *testing.T) {
	srcDB, dstDB, metaDB := openTestDB(t)
	cmd := newCmdReplicate(srcDB, dstDB, metaDB, nil, nil, 10, 1, runMetricsClient(t))

	initDB := func(t *testing.T) {
		srcDB.db.MustExec(`
			CREATE SCHEMA s1;
		`)
		srcDB.db.MustExec(`
			CREATE SCHEMA s2;
		`)
		srcDB.db.MustExec(`
			CREATE TABLE s1.t1 (
				id INT PRIMARY KEY
			);

			CREATE TABLE s1.t2 (
				id INT PRIMARY KEY,
				val TEXT
			);

			CREATE TABLE s2.t3 (
				id1  UNIQUEIDENTIFIER,
				id2  INT,
				val1 VARCHAR(15),
				val2 NUMERIC(2, 0),
				val3 DATE,
				val4 DATETIME,
				val5 DATETIME,
				val6 CHAR(1),
				val7 INT CONSTRAINT t3_t2_fk REFERENCES s1.t2(id),
				PRIMARY KEY (id1, id2)
			);

			ALTER DATABASE CURRENT SET CHANGE_TRACKING = ON
				(CHANGE_RETENTION = 14 DAYS, AUTO_CLEANUP = ON);

			ALTER TABLE s1.t1 ENABLE CHANGE_TRACKING;
			ALTER TABLE s1.t2 ENABLE CHANGE_TRACKING;
			ALTER TABLE s2.t3 ENABLE CHANGE_TRACKING;
		`)

		dstDB.db.MustExec(`
			CREATE SCHEMA s1;
			CREATE SCHEMA s2;

			CREATE TABLE s1.t1 (
				id int PRIMARY KEY
			);

			CREATE TABLE s1.t2 (
				id int PRIMARY KEY,
				val text
			);

			CREATE TABLE s2.t3 (
				id1  uuid,
				id2  int,
				val1 text,
				val2 int2,
				val3 timestamp,
				val4 date,
				val5 timestamp,
				val6 char(1),
				val7 int,
				PRIMARY KEY (id1, id2)
			)
		`)

		var ver int64
		srcDB.db.QueryRowx(`SELECT CHANGE_TRACKING_CURRENT_VERSION()`).Scan(&ver)
		metaDB.db.MustExec(
			`INSERT INTO replication_progress
				(schema_name, table_name, change_tracking_last_version)
			VALUES
				('s1', 't1', ?),
				('s1', 't2', ?),
				('s2', 't3', ?)`,
			ver, ver, ver,
		)

		t.Cleanup(func() {
			srcDB.db.MustExec(`
				DROP TABLE s2.t3;
				DROP TABLE s1.t2;
				DROP TABLE s1.t1;
				DROP SCHEMA s2;
				DROP SCHEMA s1;

				ALTER DATABASE CURRENT SET CHANGE_TRACKING = OFF;
			`)

			dstDB.db.MustExec(`
				DROP SCHEMA s2 CASCADE;
				DROP SCHEMA s1 CASCADE;
			`)

			metaDB.db.MustExec(`
				DELETE FROM replication_progress;
			`)
		})
	}

	t.Run("copies data changes from source to destination DB", func(t *testing.T) {
		initDB(t)
		cmd.changeTrackingCopyMinInterval = 50 * time.Millisecond
		ctx, cancel := context.WithCancel(context.Background())
		wait := make(chan struct{})
		ch := make(chan tableInfo, 3)
		ch <- tableInfo{schema: "s2", name: "t3"}
		ch <- tableInfo{schema: "s1", name: "t1"}
		ch <- tableInfo{schema: "s1", name: "t2"}

		go func() {
			err := cmd.copyChangeTracking(ctx, ch)

			assert.EqualError(t, err, "context canceled")
			close(wait)
		}()

		srcDB.db.MustExec(`
			INSERT INTO s1.t1 VALUES
				(1);

			INSERT INTO s1.t2 VALUES
				(2, 'lorem'),
				(3, 'ipsum'),
				(4, 'dolor');
		`)

		time.Sleep(100 * time.Millisecond)

		assert.Equal(t,
			[]rowData{
				{"id": int64(1)},
			},
			getAllData(t, dstDB.db, tableInfo{schema: "s1", name: "t1"}, "id"),
		)

		assert.Equal(t,
			[]rowData{
				{"id": int64(2), "val": "lorem"},
				{"id": int64(3), "val": "ipsum"},
				{"id": int64(4), "val": "dolor"},
			},
			getAllData(t, dstDB.db, tableInfo{schema: "s1", name: "t2"}, "id"),
		)

		assert.Equal(t,
			[]rowData{},
			getAllData(t, dstDB.db, tableInfo{schema: "s2", name: "t3"}, "id1, id2"),
		)

		srcDB.db.MustExec(`
			INSERT INTO s2.t3 (id1, id2, val1, val2, val3, val4, val5, val6, val7) VALUES
				('1a2b3c4d-5a6b-7c8d-9910-111213141516', 1, 'foo', 2, '2020-01-02', '2020-01-02T15:04:05Z', '2020-01-02T15:04:05Z', 'A', 2),
				('1a2b3c4d-5a6b-7c8d-9910-111213141517', 3, 'bar', 4, '2020-01-03', '2020-01-03T15:04:05Z', '2020-01-03T15:04:05Z', 'B', 4);

			UPDATE s2.t3 SET val1 = 'baz' WHERE id1 = '1a2b3c4d-5a6b-7c8d-9910-111213141516';

			DELETE FROM s1.t2 WHERE id = 3;
		`)

		time.Sleep(100 * time.Millisecond)

		assert.Equal(t,
			[]rowData{
				{"id": int64(1)},
			},
			getAllData(t, dstDB.db, tableInfo{schema: "s1", name: "t1"}, "id"),
		)

		assert.Equal(t,
			[]rowData{
				{"id": int64(2), "val": "lorem"},
				{"id": int64(4), "val": "dolor"},
			},
			getAllData(t, dstDB.db, tableInfo{schema: "s1", name: "t2"}, "id"),
		)

		assert.Equal(t,
			[]rowData{
				{
					"id1":  []byte("1a2b3c4d-5a6b-7c8d-9910-111213141516"),
					"id2":  int64(1),
					"val1": "baz",
					"val2": int64(2),
					"val3": time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC),
					"val4": time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC),
					"val5": time.Date(2020, 1, 2, 15, 4, 5, 0, time.UTC),
					"val6": []byte("A"),
					"val7": int64(2),
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
					"val7": int64(4),
				},
			},
			getAllData(t, dstDB.db, tableInfo{schema: "s2", name: "t3"}, "id1, id2"),
		)

		cancel()
		select {
		case <-wait:
		case <-time.After(500 * time.Millisecond):
			assert.Fail(t, "Timed out waiting for function to return.")
		}
	})

	t.Run("records new change tracking version", func(t *testing.T) {
		initDB(t)
		srcDB.db.MustExec("INSERT INTO s1.t1 VALUES (1)")
		ctx, cancel := context.WithCancel(context.Background())
		wait := make(chan struct{})
		ch := make(chan tableInfo, 3)
		ch <- tableInfo{schema: "s2", name: "t3"}
		ch <- tableInfo{schema: "s1", name: "t1"}
		ch <- tableInfo{schema: "s1", name: "t2"}

		go func() {
			err := cmd.copyChangeTracking(ctx, ch)

			assert.EqualError(t, err, "context canceled")
			close(wait)
		}()

		time.Sleep(200 * time.Millisecond)
		cancel()
		select {
		case <-wait:
		case <-time.After(3 * time.Second):
			assert.Fail(t, "Timed out waiting for function to return.")
		}

		var ver int64
		srcDB.db.QueryRowx(`SELECT CHANGE_TRACKING_CURRENT_VERSION()`).Scan(&ver)

		assert.Equal(t,
			[]rowData{
				{"schema_name": "s1", "table_name": "t1", "initial_copy_done": int64(0), "initial_copy_last_id": nil, "change_tracking_last_version": ver},
				{"schema_name": "s1", "table_name": "t2", "initial_copy_done": int64(0), "initial_copy_last_id": nil, "change_tracking_last_version": ver},
				{"schema_name": "s2", "table_name": "t3", "initial_copy_done": int64(0), "initial_copy_last_id": nil, "change_tracking_last_version": ver},
			},
			getAllData(t, metaDB.db, tableInfo{name: "replication_progress"}, "table_name"),
		)
	})
}

func Test_cmdReplicate_getLastValidSyncVersion(t *testing.T) {
	var (
		srcDB, _, metaDB = openTestDB(t)
		cmd              = newCmdReplicate(srcDB, nil, metaDB, nil, nil, 10, 1, runMetricsClient(t))
		table            = tableInfo{schema: "test", name: "some_table"}
	)

	initDB := func(t *testing.T) {
		srcDB.db.MustExec(`
			CREATE SCHEMA test;
		`)
		srcDB.db.MustExec(`
			CREATE TABLE test.some_table(id INT PRIMARY KEY);

			ALTER DATABASE CURRENT SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 14 DAYS, AUTO_CLEANUP = ON);
			ALTER TABLE test.some_table ENABLE CHANGE_TRACKING;
		`)

		t.Cleanup(func() {
			srcDB.db.MustExec(`
				DROP TABLE test.some_table;
				DROP SCHEMA test;

				ALTER DATABASE CURRENT SET CHANGE_TRACKING = OFF;
			`)

			metaDB.db.MustExec("DELETE FROM replication_progress")
		})
	}

	minValidVersion := func(t *testing.T) int64 {
		t.Helper()

		var ret int64
		err := cmd.srcDB.db.QueryRowx("SELECT CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID('test.some_table'))").Scan(&ret)
		if !assert.NoError(t, err) {
			assert.FailNow(t, "Error querying src DB")
		}

		return ret
	}

	t.Run("returns version from database", func(t *testing.T) {
		initDB(t)
		lastSyncVer := minValidVersion(t)
		metaDB.db.MustExec(
			`INSERT INTO replication_progress
				(schema_name, table_name, change_tracking_last_version)
			VALUES
				('test', 'some_table', ?)`,
			lastSyncVer,
		)

		ver, err := cmd.getLastValidSyncVersion(table)

		assert.NoError(t, err)
		assert.Equal(t, lastSyncVer, ver)
	})

	t.Run("returns error when last sync version is less than min valid version", func(t *testing.T) {
		initDB(t)
		ver := minValidVersion(t)
		metaDB.db.MustExec(
			`INSERT INTO replication_progress
				(schema_name, table_name, change_tracking_last_version)
			VALUES
				('test', 'some_table', ?)`,
			ver-1,
		)

		_, err := cmd.getLastValidSyncVersion(table)

		assert.EqualError(t, err, fmt.Sprintf("min valid version is newer than last sync version: %d, min valid version: %d", ver-1, ver))
	})

	t.Run("returns error when no data for given table", func(t *testing.T) {
		initDB(t)

		_, err := cmd.getLastValidSyncVersion(tableInfo{schema: "test", name: "invalid_table"})

		assert.Error(t, err)
	})
}

func Test_dstTable(t *testing.T) {
	assert.Equal(t, tableInfo{schema: "public", name: "table1"}, dstTable(tableInfo{schema: "dbo", name: "table1"}))
	assert.Equal(t, tableInfo{schema: "test", name: "table1"}, dstTable(tableInfo{schema: "test", name: "table1"}))
}

func getAllData(t *testing.T, db *sqlx.DB, table tableInfo, order string) []rowData {
	t.Helper()

	tableName := table.name
	if table.schema != "" {
		tableName = table.schema + "." + table.name
	}

	rows, err := db.Queryx(fmt.Sprintf(
		`SELECT * FROM %s ORDER BY %s`,
		tableName, order,
	))
	if !assert.NoError(t, err) {
		assert.FailNow(t, "error reading db data")
	}

	ret := []rowData{}
	for rows.Next() {
		row := rowData{}
		if !assert.NoError(t, rows.MapScan(row)) {
			assert.FailNow(t, "error scanning row")
		}

		for k, v := range row {
			if v, ok := v.(time.Time); ok {
				row[k] = v.UTC()
			}
		}

		ret = append(ret, row)
	}

	return ret
}

func openTestDB(t *testing.T) (*mssql, *postgres, *sqlite) {
	t.Helper()

	cfg, err := loadConfig("mssql2pg_test.json")
	if !assert.NoError(t, err) {
		assert.FailNow(t, "error loading test config")
	}

	src, dst, meta, err := openDatabases(cfg)
	if !assert.NoError(t, err) {
		assert.FailNow(t, "error opening test databases")
	}

	t.Cleanup(func() {
		src.db.Close()
		dst.db.Close()
		meta.db.Close()
		os.Remove(cfg.MetaDatabasePath)
	})

	return src, dst, meta
}

func runMetricsClient(t *testing.T) *metricsClient {
	mc := newMetricsClient(8080)
	t.Cleanup(func() { mc.shutdown() })

	return mc
}

package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_cmdFullCopy_start(t *testing.T) {
	srcDB, dstDB, metaDB := openTestDB(t)
	initDB := func(t *testing.T) {
		srcDB.db.MustExec(`
			CREATE SCHEMA s1;
		`)
		srcDB.db.MustExec(`
			CREATE TABLE t1 (
				id INT
			);

			CREATE TABLE s1.t2 (
				id INT PRIMARY KEY,
				val TEXT
			);

			CREATE TABLE s1.t3 (
				id1  UNIQUEIDENTIFIER,
				id2  INT,
				val1 VARCHAR(15),
				val2 NUMERIC(2, 0),
				val3 DATE,
				val4 DATETIME,
				val5 DATETIME,
				val6 CHAR(1),
				val7 INT CONSTRAINT t3_t2_fk REFERENCES s1.t2(id)
			);

			INSERT INTO t1 VALUES
				(1);

			INSERT INTO s1.t2 VALUES
				(2, 'lorem'),
				(3, 'ipsum'),
				(4, 'dolor');

			INSERT INTO s1.t3 (id1, id2, val1, val2, val3, val4, val5, val6, val7) VALUES
				('1a2b3c4d-5a6b-7c8d-9910-111213141516', 1, 'foo', 2, '2020-01-02', '2020-01-02T15:04:05Z', '2020-01-02T15:04:05Z', 'A', 2),
				('1a2b3c4d-5a6b-7c8d-9910-111213141517', 3, 'bar', 4, '2020-01-03', '2020-01-03T15:04:05Z', '2020-01-03T15:04:05Z', 'B', 4);
		`)

		dstDB.db.MustExec(`
			CREATE SCHEMA s1;

			CREATE TABLE t1 (
				id int
			);
			INSERT INTO t1 VALUES
				(11),
				(12),
				(13);

			CREATE TABLE s1.t2 (
				id int PRIMARY KEY,
				val text
			);

			CREATE TABLE s1.t3 (
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
			);

			CREATE INDEX index_val1 ON s1.t3 (val1);
		`)

		t.Cleanup(func() {
			srcDB.db.MustExec(`
				DROP TABLE s1.t3;
				DROP TABLE s1.t2;
				DROP TABLE t1;
				DROP SCHEMA s1;
			`)

			dstDB.db.MustExec(`
				DROP TABLE IF EXISTS t1;
				DROP SCHEMA IF EXISTS s1 CASCADE;
			`)
		})
	}

	t.Run("Copies data from specified tables from source to destination DB", func(t *testing.T) {
		initDB(t)
		cmd := newCmdFullCopy(srcDB, dstDB, metaDB, 2, []string{"dbo.t1", "s1.t3"})

		err := cmd.start(context.Background())

		assert.NoError(t, err)

		assert.Equal(t,
			[]rowdata{
				{"id": int64(1)},
			},
			getAllData(t, dstDB.db, tableInfo{schema: "public", name: "t1"}, "id"),
		)

		assert.Empty(t, getAllData(t, dstDB.db, tableInfo{schema: "s1", name: "t2"}, "id"))

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
			getAllData(t, dstDB.db, tableInfo{schema: "s1", name: "t3"}, "id1"),
		)

		var indexCount int
		dstDB.db.QueryRowx("SELECT count(*) FROM pg_indexes WHERE indexname = 'index_val1'").Scan(&indexCount)
		assert.Equal(t, 1, indexCount)
	})
}

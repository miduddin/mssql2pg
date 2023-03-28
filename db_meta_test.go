package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_metaDB_hasSavedForeignKeys(t *testing.T) {
	_, _, metaDB := openTestDB(t)

	initDB := func(t *testing.T) {
		t.Cleanup(func() {
			metaDB.db.MustExec("DELETE FROM foreign_keys")
		})
	}

	t.Run("returns true if any foreign key data is in meta db", func(t *testing.T) {
		initDB(t)

		ok, err := metaDB.hasSavedForeignKeys()

		assert.NoError(t, err)
		assert.Equal(t, false, ok)

		metaDB.db.MustExec(`
			INSERT INTO foreign_keys (schema_name, table_name, fk_name, fk_definition)
			VALUES ('schema1', 'table1', 'fk1', 'def1');
		`)

		ok, err = metaDB.hasSavedForeignKeys()

		assert.NoError(t, err)
		assert.Equal(t, true, ok)
	})
}

func Test_metaDB_saveForeignKeys(t *testing.T) {
	_, _, metaDB := openTestDB(t)

	initDB := func(t *testing.T) {
		t.Cleanup(func() {
			metaDB.db.MustExec("DELETE FROM foreign_keys")
		})
	}

	t.Run("persists given foreign keys info to meta DB", func(t *testing.T) {
		initDB(t)

		err := metaDB.saveForeignKeys([]dstForeignKey{
			{t: tableInfo{schema: "schema1", name: "table1"}, name: "fk1", definition: "def1"},
			{t: tableInfo{schema: "schema1", name: "table1"}, name: "fk2", definition: "def2"},
			{t: tableInfo{schema: "schema2", name: "table2"}, name: "fk3", definition: "def3"},
		})

		assert.NoError(t, err)
		assert.Equal(t,
			[]rowdata{
				{"schema_name": "schema1", "table_name": "table1", "fk_name": "fk1", "fk_definition": "def1"},
				{"schema_name": "schema1", "table_name": "table1", "fk_name": "fk2", "fk_definition": "def2"},
				{"schema_name": "schema2", "table_name": "table2", "fk_name": "fk3", "fk_definition": "def3"},
			},
			getAllData(t, metaDB.db, tableInfo{name: "foreign_keys"}, "fk_name"),
		)
	})
}

func Test_metaDB_saveDstIndexes(t *testing.T) {
	_, _, metaDB := openTestDB(t)

	initDB := func(t *testing.T) {
		t.Cleanup(func() {
			metaDB.db.MustExec("DELETE FROM indexes")
		})
	}

	t.Run("persists given indexes info to meta DB", func(t *testing.T) {
		initDB(t)

		err := metaDB.saveDstIndexes([]dstIndex{
			{t: tableInfo{schema: "schema1", name: "table1"}, name: "ix1", def: "def1"},
			{t: tableInfo{schema: "schema1", name: "table1"}, name: "ix2", def: "def2"},
			{t: tableInfo{schema: "schema2", name: "table2"}, name: "ix3", def: "def3"},
		})

		assert.NoError(t, err)
		assert.Equal(t,
			[]rowdata{
				{"schema_name": "schema1", "table_name": "table1", "index_name": "ix1", "index_def": "def1"},
				{"schema_name": "schema1", "table_name": "table1", "index_name": "ix2", "index_def": "def2"},
				{"schema_name": "schema2", "table_name": "table2", "index_name": "ix3", "index_def": "def3"},
			},
			getAllData(t, metaDB.db, tableInfo{name: "indexes"}, "index_name"),
		)
	})
}

func Test_metaDB_getDstIndexes(t *testing.T) {
	_, _, metaDB := openTestDB(t)
	table := tableInfo{schema: "schema1", name: "table1"}

	initDB := func(t *testing.T) {
		metaDB.db.MustExec(`
			INSERT INTO indexes VALUES
				("schema1", "table1", "ix1", "def1"),
				("schema1", "table1", "ix2", "def22"),
				("schema2", "table3", "ix3", "def3");
		`)

		t.Cleanup(func() {
			metaDB.db.MustExec("DELETE FROM indexes")
		})
	}

	t.Run("returns indexes saved in metaDB", func(t *testing.T) {
		initDB(t)

		ixs, err := metaDB.getDstIndexes(table)

		assert.NoError(t, err)
		assert.Equal(t,
			[]dstIndex{
				{t: table, name: "ix1", def: "def1"},
				{t: table, name: "ix2", def: "def22"},
			},
			ixs,
		)
	})
}

func Test_metaDB_saveChangeTrackingVersion(t *testing.T) {
	_, _, metaDB := openTestDB(t)
	table := tableInfo{schema: "test", name: "some_table"}

	initDB := func(t *testing.T) {
		metaDB.db.MustExec(`
			INSERT INTO replication_progress (schema_name, table_name, change_tracking_last_version) VALUES
				('test', 'some_table', 42);
		`)

		t.Cleanup(func() {
			metaDB.db.MustExec("DELETE FROM replication_progress")
		})
	}

	t.Run("inserts new row for new table", func(t *testing.T) {
		initDB(t)

		err := metaDB.saveChangeTrackingVersion(tableInfo{schema: "foo", name: "bar"}, 13)

		assert.NoError(t, err)
		assert.Equal(t,
			[]rowdata{
				{
					"schema_name":                  "foo",
					"table_name":                   "bar",
					"change_tracking_last_version": int64(13),
					"initial_copy_done":            int64(0),
				},
				{
					"schema_name":                  "test",
					"table_name":                   "some_table",
					"change_tracking_last_version": int64(42),
					"initial_copy_done":            int64(0),
				},
			},
			getAllData(t, metaDB.db, tableInfo{name: "replication_progress"}, "table_name"),
		)
	})

	t.Run("updates data for existing table", func(t *testing.T) {
		initDB(t)

		err := metaDB.saveChangeTrackingVersion(table, 13)

		assert.NoError(t, err)
		assert.Equal(t,
			[]rowdata{
				{
					"schema_name":                  "test",
					"table_name":                   "some_table",
					"change_tracking_last_version": int64(13),
					"initial_copy_done":            int64(0),
				},
			},
			getAllData(t, metaDB.db, tableInfo{name: "replication_progress"}, "table_name"),
		)
	})
}

func Test_metaDB_getInitialCopyProgress(t *testing.T) {
	_, _, metaDB := openTestDB(t)
	table := tableInfo{schema: "test", name: "some_table"}

	initDB := func(t *testing.T) {
		metaDB.db.MustExec(`
			INSERT INTO replication_progress (schema_name, table_name, initial_copy_done) VALUES
				('test', 'some_table', 1),
				('test', 'more_table', 0);
		`)

		t.Cleanup(func() {
			metaDB.db.MustExec("DELETE FROM replication_progress")
		})
	}

	t.Run("retrieves data for known tables without modifying data", func(t *testing.T) {
		initDB(t)

		done, err := metaDB.getInitialCopyProgress(table)

		assert.NoError(t, err)
		assert.Equal(t, true, done)

		done, err = metaDB.getInitialCopyProgress(tableInfo{schema: "test", name: "more_table"})

		assert.NoError(t, err)
		assert.Equal(t, false, done)

		assert.Equal(t,
			[]rowdata{
				{"schema_name": "test", "table_name": "more_table", "initial_copy_done": int64(0), "change_tracking_last_version": int64(0)},
				{"schema_name": "test", "table_name": "some_table", "initial_copy_done": int64(1), "change_tracking_last_version": int64(0)},
			},
			getAllData(t, metaDB.db, tableInfo{name: "replication_progress"}, "table_name"),
		)
	})

	t.Run("inserts new row for unknown table", func(t *testing.T) {
		initDB(t)

		done, err := metaDB.getInitialCopyProgress(tableInfo{schema: "test", name: "other_table"})

		assert.NoError(t, err)
		assert.Equal(t, false, done)

		assert.Equal(t,
			[]rowdata{
				{"schema_name": "test", "table_name": "more_table", "initial_copy_done": int64(0), "change_tracking_last_version": int64(0)},
				{"schema_name": "test", "table_name": "other_table", "initial_copy_done": int64(0), "change_tracking_last_version": int64(0)},
				{"schema_name": "test", "table_name": "some_table", "initial_copy_done": int64(1), "change_tracking_last_version": int64(0)},
			},
			getAllData(t, metaDB.db, tableInfo{name: "replication_progress"}, "table_name"),
		)
	})
}

func Test_metaDB_markInitialCopyDone(t *testing.T) {
	_, _, metaDB := openTestDB(t)
	table := tableInfo{schema: "test", name: "some_table"}

	initDB := func(t *testing.T) {
		metaDB.db.MustExec("INSERT INTO replication_progress (schema_name, table_name) VALUES ('test', 'some_table')")

		t.Cleanup(func() {
			metaDB.db.MustExec("DELETE FROM replication_progress")
		})
	}

	t.Run("persists data for known table", func(t *testing.T) {
		initDB(t)

		err := metaDB.markInitialCopyDone(table)

		assert.NoError(t, err)
		assert.Equal(t,
			[]rowdata{
				{"schema_name": "test", "table_name": "some_table", "initial_copy_done": int64(1), "change_tracking_last_version": int64(0)},
			},
			getAllData(t, metaDB.db, tableInfo{name: "replication_progress"}, "table_name"),
		)
	})

	t.Run("does nothing given unknown table", func(t *testing.T) {
		initDB(t)

		err := metaDB.markInitialCopyDone(tableInfo{schema: "test", name: "invalid_table"})

		assert.NoError(t, err)
		assert.Equal(t,
			[]rowdata{
				{"schema_name": "test", "table_name": "some_table", "initial_copy_done": int64(0), "change_tracking_last_version": int64(0)},
			},
			getAllData(t, metaDB.db, tableInfo{name: "replication_progress"}, "table_name"),
		)
	})
}

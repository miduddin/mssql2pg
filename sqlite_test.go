package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_sqlite_hasSavedForeignKeys(t *testing.T) {
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

func Test_sqlite_insertSavedForeignKeys(t *testing.T) {
	_, _, metaDB := openTestDB(t)

	initDB := func(t *testing.T) {
		t.Cleanup(func() {
			metaDB.db.MustExec("DELETE FROM foreign_keys")
		})
	}

	t.Run("persists given foreign keys info to meta DB", func(t *testing.T) {
		initDB(t)

		err := metaDB.insertSavedForeignKeys([]foreignKey{
			{t: tableInfo{schema: "schema1", name: "table1"}, name: "fk1", definition: "def1"},
			{t: tableInfo{schema: "schema1", name: "table1"}, name: "fk2", definition: "def2"},
			{t: tableInfo{schema: "schema2", name: "table2"}, name: "fk3", definition: "def3"},
		})

		assert.NoError(t, err)
		assert.Equal(t,
			[]rowData{
				{"schema_name": "schema1", "table_name": "table1", "fk_name": "fk1", "fk_definition": "def1"},
				{"schema_name": "schema1", "table_name": "table1", "fk_name": "fk2", "fk_definition": "def2"},
				{"schema_name": "schema2", "table_name": "table2", "fk_name": "fk3", "fk_definition": "def3"},
			},
			getAllData(t, metaDB.db, tableInfo{name: "foreign_keys"}, "fk_name"),
		)
	})
}

func Test_sqlite_insertSavedIndexes(t *testing.T) {
	_, _, metaDB := openTestDB(t)

	initDB := func(t *testing.T) {
		t.Cleanup(func() {
			metaDB.db.MustExec("DELETE FROM indexes")
		})
	}

	t.Run("persists given indexes info to meta DB", func(t *testing.T) {
		initDB(t)

		err := metaDB.insertSavedIndexes([]index{
			{table: tableInfo{schema: "schema1", name: "table1"}, name: "ix1", def: "def1"},
			{table: tableInfo{schema: "schema1", name: "table1"}, name: "ix2", def: "def2"},
			{table: tableInfo{schema: "schema2", name: "table2"}, name: "ix3", def: "def3"},
		})

		assert.NoError(t, err)
		assert.Equal(t,
			[]rowData{
				{"schema_name": "schema1", "table_name": "table1", "index_name": "ix1", "index_def": "def1"},
				{"schema_name": "schema1", "table_name": "table1", "index_name": "ix2", "index_def": "def2"},
				{"schema_name": "schema2", "table_name": "table2", "index_name": "ix3", "index_def": "def3"},
			},
			getAllData(t, metaDB.db, tableInfo{name: "indexes"}, "index_name"),
		)
	})
}

func Test_sqlite_getSavedIndexes(t *testing.T) {
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

		ixs, err := metaDB.getSavedIndexes(table)

		assert.NoError(t, err)
		assert.Equal(t,
			[]index{
				{table: table, name: "ix1", def: "def1"},
				{table: table, name: "ix2", def: "def22"},
			},
			ixs,
		)
	})
}

func Test_sqlite_upsertChangeTrackingVersion(t *testing.T) {
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

		err := metaDB.upsertChangeTrackingVersion(tableInfo{schema: "foo", name: "bar"}, 13)

		assert.NoError(t, err)
		assert.Equal(t,
			[]rowData{
				{
					"schema_name":                  "foo",
					"table_name":                   "bar",
					"change_tracking_last_version": int64(13),
					"initial_copy_done":            int64(0),
					"initial_copy_last_id":         nil,
				},
				{
					"schema_name":                  "test",
					"table_name":                   "some_table",
					"change_tracking_last_version": int64(42),
					"initial_copy_done":            int64(0),
					"initial_copy_last_id":         nil,
				},
			},
			getAllData(t, metaDB.db, tableInfo{name: "replication_progress"}, "table_name"),
		)
	})

	t.Run("updates data for existing table", func(t *testing.T) {
		initDB(t)

		err := metaDB.upsertChangeTrackingVersion(table, 13)

		assert.NoError(t, err)
		assert.Equal(t,
			[]rowData{
				{
					"schema_name":                  "test",
					"table_name":                   "some_table",
					"change_tracking_last_version": int64(13),
					"initial_copy_done":            int64(0),
					"initial_copy_last_id":         nil,
				},
			},
			getAllData(t, metaDB.db, tableInfo{name: "replication_progress"}, "table_name"),
		)
	})
}

func Test_sqlite_getInitialCopyStatus(t *testing.T) {
	_, _, metaDB := openTestDB(t)
	table := tableInfo{schema: "test", name: "some_table"}

	initDB := func(t *testing.T) {
		metaDB.db.MustExec(`
			INSERT INTO replication_progress (schema_name, table_name, initial_copy_done, initial_copy_last_id) VALUES
				('test', 'some_table', 1, '42'),
				('test', 'more_table', 0, NULL);
		`)

		t.Cleanup(func() {
			metaDB.db.MustExec("DELETE FROM replication_progress")
		})
	}

	t.Run("retrieves data for known tables without modifying data", func(t *testing.T) {
		initDB(t)

		done, lastID, err := metaDB.getInitialCopyStatus(table)

		assert.NoError(t, err)
		assert.Equal(t, true, done)
		assert.Equal(t, "42", lastID)

		done, lastID, err = metaDB.getInitialCopyStatus(tableInfo{schema: "test", name: "more_table"})

		assert.NoError(t, err)
		assert.Equal(t, false, done)
		assert.Equal(t, "", lastID)

		assert.Equal(t,
			[]rowData{
				{"schema_name": "test", "table_name": "more_table", "initial_copy_done": int64(0), "initial_copy_last_id": nil, "change_tracking_last_version": int64(0)},
				{"schema_name": "test", "table_name": "some_table", "initial_copy_done": int64(1), "initial_copy_last_id": "42", "change_tracking_last_version": int64(0)},
			},
			getAllData(t, metaDB.db, tableInfo{name: "replication_progress"}, "table_name"),
		)
	})

	t.Run("inserts new row for unknown table", func(t *testing.T) {
		initDB(t)

		done, lastID, err := metaDB.getInitialCopyStatus(tableInfo{schema: "test", name: "other_table"})

		assert.NoError(t, err)
		assert.Equal(t, false, done)
		assert.Equal(t, "", lastID)

		assert.Equal(t,
			[]rowData{
				{"schema_name": "test", "table_name": "more_table", "initial_copy_done": int64(0), "initial_copy_last_id": nil, "change_tracking_last_version": int64(0)},
				{"schema_name": "test", "table_name": "other_table", "initial_copy_done": int64(0), "initial_copy_last_id": nil, "change_tracking_last_version": int64(0)},
				{"schema_name": "test", "table_name": "some_table", "initial_copy_done": int64(1), "initial_copy_last_id": "42", "change_tracking_last_version": int64(0)},
			},
			getAllData(t, metaDB.db, tableInfo{name: "replication_progress"}, "table_name"),
		)
	})
}

func Test_sqlite_markInitialCopyDone(t *testing.T) {
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
			[]rowData{
				{"schema_name": "test", "table_name": "some_table", "initial_copy_done": int64(1), "initial_copy_last_id": nil, "change_tracking_last_version": int64(0)},
			},
			getAllData(t, metaDB.db, tableInfo{name: "replication_progress"}, "table_name"),
		)
	})

	t.Run("does nothing given unknown table", func(t *testing.T) {
		initDB(t)

		err := metaDB.markInitialCopyDone(tableInfo{schema: "test", name: "invalid_table"})

		assert.NoError(t, err)
		assert.Equal(t,
			[]rowData{
				{"schema_name": "test", "table_name": "some_table", "initial_copy_done": int64(0), "initial_copy_last_id": nil, "change_tracking_last_version": int64(0)},
			},
			getAllData(t, metaDB.db, tableInfo{name: "replication_progress"}, "table_name"),
		)
	})
}

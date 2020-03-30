package migration

// CockroachMigrations contains the migrations for cockroach.
// The key is the migration name, and the value the migration script.
// /!\ The migration tools sort the migration by key. The sorting looks
// for a numeric prefix and sort by increasing prefix order. It is advised
// to prefix migration keys with increasing number, e.g. 1_..., 2_...
var CockroachMigrations = map[string]string{
	"1_xtz_initial_tables": `
-- +migrate Up

----------------
-- XTZ addresses
----------------
-- +migrate StatementBegin
CREATE TABLE IF NOT EXISTS xtz_addresses
(
	address STRING PRIMARY KEY,
	chunk_id INT64,
	INDEX(chunk_id)
)
-- +migrate StatementEnd

----------------
-- XTZ block
----------------
-- +migrate StatementBegin
CREATE TABLE IF NOT EXISTS xtz_block
(
	block_number INT64 PRIMARY KEY,
	block_hash STRING,
	block_timestamp TIMESTAMPTZ,
	created_at TIMESTAMPTZ
)
-- +migrate StatementEnd

----------------
-- XTZ chunk
----------------
-- +migrate StatementBegin
CREATE TABLE IF NOT EXISTS xtz_chunk
(
	id INT64 PRIMARY KEY,
	last_processed_time TIMESTAMPTZ NOT NULL,
	locked BOOL NOT NULL,
	locked_uuid STRING NOT NULL,
	locked_until TIMESTAMPTZ NOT NULL,
	INDEX(last_processed_time)
)
-- +migrate StatementEnd

----------------
-- XTZ TX
----------------
-- +migrate StatementBegin
CREATE TABLE IF NOT EXISTS xtz_tx
(
	id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	hash STRING,
	idx INT64,
	block_number INT64,
	addr_to STRING,
	addr_from STRING,
	amount DECIMAL(38),
	fee DECIMAL(38),
	counter DECIMAL(38),
	timestamp TIMESTAMPTZ,
	pinned BOOL NOT NULL,
	broadcasted BOOL NOT NULL,
	rawtx STRING,
	status INT NOT NULL,
	message STRING,
	created_at TIMESTAMPTZ,
	created_at_block INT64,
	broadcasted_at_block INT64,
	UNIQUE (hash, idx),
	INDEX xtz_tx_addr_from_timestamp_idx (addr_from, timestamp),
	INDEX xtz_tx_addr_to_timestamp_idx (addr_to, timestamp),
	INDEX xtz_tx_addr_from_pinned_idx (addr_from, pinned),
	INDEX xtz_tx_addr_to_pinned_idx (addr_to, pinned),
	INDEX xtz_tx_block_number_idx (block_number),
	INDEX xtz_tx_addr_from_block_number_idx (addr_from, block_number),
	INDEX xtz_tx_addr_to_block_number_idx (addr_to, block_number),
	INDEX xtz_tx_broadcasted_status_block_number_broadcasted_at_block_idx (broadcasted, status, block_number, broadcasted_at_block),
	INDEX xtz_tx_pinned_broadcasted_block_number_idx (pinned, broadcasted, block_number)
)
-- +migrate StatementEnd

----------------
-- XTZ Transaction attributes
----------------
-- +migrate StatementBegin
CREATE TABLE IF NOT EXISTS xtz_tx_attributes
(
	customer_id STRING,
	hash STRING,
	key STRING,
	value STRING,
	PRIMARY KEY (customer_id, hash, key),
	INDEX (customer_id, key, value)
)
-- +migrate StatementEnd

-- +migrate Down
`,
}

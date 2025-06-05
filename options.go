package simplewaldb

type config struct {
	rootDir   string
	tables    []TableKey
	separator recordSeparator
}

// Option defines a config option of the database.
type Option func(*config)

// WithRootDir defines the root dir of the database. This MUST be a directory.
// If it does not exist, it will be created when the database is opened.
func WithRootDir(dir string) Option {
	return func(c *config) {
		c.rootDir = dir
	}
}

// WithTables defines the tables that should exist in the database.
func WithTables(keys ...TableKey) Option {
	return func(c *config) {
		c.tables = keys
	}
}

// WithSeparatorHex defines the separator key for entries in the database. To
// allow for manual recovery scenarios, this SHOULD be a random, 31-byte (i.e.
// 62 hex chars) string. This SHOULD NOT be changed across DB invocations and
// SHOULD be kept secret to avoid users attempting to replicate them in their
// data.
func WithSeparatorHex(hexData string) Option {
	return func(c *config) {
		must(c.separator.fromHex(hexData))
	}
}

// defineOptions generates a new config object.
func defineOptions(opts ...Option) *config {
	// Defaults.
	c := &config{}
	must(c.separator.fromHex("ce6dcbb021ea09d2c6e77714d7cdefcdf28fe1e0b4221e24d78648efe10ed8"))

	// Apply config.
	for _, o := range opts {
		o(c)
	}
	return c
}

type prepTxCfg struct {
	readTables  []TableKey
	writeTables []TableKey
}

// TxOption is an option when preparing a transaction.
type TxOption func(c *prepTxCfg)

// WithReadTables defines tables that will be available only for reading.
func WithReadTables(tables ...TableKey) TxOption {
	return func(c *prepTxCfg) {
		c.readTables = tables
	}
}

// WithWriteTables defines tables that will be available for reading and
// writing.
func WithWriteTables(tables ...TableKey) TxOption {
	return func(c *prepTxCfg) {
		c.writeTables = tables
	}
}

// definePrepTxCfg defines the config for preparing a tx.
func definePrepTxCfg(opts ...TxOption) *prepTxCfg {
	c := &prepTxCfg{}
	for _, o := range opts {
		o(c)
	}
	return c
}

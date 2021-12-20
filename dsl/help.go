package dsl

import "fmt"

var helpText = `keybite
A flexible, serverless key-value store

USAGE:
keybite (keybite will start in standalone server mode when started with no args)
keybite [command] [index] [options] [data]

FLAGS:
	-h,	--help
		Display this help text.

QUERY COMMANDS:
- Auto-incrementing indexes (keybite assigns an integer ID):
	query
		Retrieve a value from an auto-index by ID
		Example: query user 13
	insert
		Insert a value into an auto index. Returns the assigned integer ID.
		Example: insert user admin@example.com
	update
		Update an existing value in an auto index. Returns the ID.
		Example: update user 10 admin2@example.com
	delete
		Delete a record from an auto index. Returns the ID.
		Example: delete user 10
	list
		List the contents of an index in the order of insertion. Optional limit and offset.
		Example: list user 10 50
	count
		Count the records in an index.
		Example: count user

- Map indexes (user assigns a string or integer key)
	query_key
		Retrieve a value from a map index by key
		Example: query_key user_email admin@example.com
	insert_key
		Insert a value into a map index with the specified key. Returns the key.
		Example: insert_key user_email admin@example.com 10
	update_key
		Update an existing value at the provided key. Returns the key.
		Example: update_key user_email admin@example.com 9
	upsert_key
		If a record with the specified key exists, update it, else insert a new one. Returns the key
		Example: upsert_key user_email admin@example.com 9
	delete_key
		Delete the record with the specified key if it exists.
		Example: delete_key user_email admin@example.com
	list_key
		List the contents of an index in the order of the key hashes (roughly alphabetical,
		but long keys can cause integer overflow and break alphabetization). Optional limit and offset.
		Example: list_key user_email 10 50
	count_key
		Count the records in an index.
		Example: count user_email

CONFIGURATION:
Keybite requires some configuration to work. All configuration is pulled from the environment,
so exporting environment variables or prefixing the keybite binary launch with environment variables
is a valid approach. The recommended approach is to use a .env file. 

ENVIRONMENT VARS:
	DATA_DIR
		The directory where keybite should store its data.
	AUTO_PAGE_SIZE
		The number of records to store per file for auto indexes. This value should be decided based on
		the environment and use case for the server. When using an S3 bucket, the entire page file must
		be transmitted across the network when retrieving records, so smaller sizes are preferable. In
		local environments. Because IDs are automatically incremented in auto-indexes, each page will be
		completely filled before a new page is created. When records are deleted, the size of a page will
		be reduced.
	MAP_PAGE_SIZE
		The number of records to store per file for map indexes. Since string keys are hashed to integers
		and stored in a page file based on the hashed ID, map pages will usually be much more sparse than
		auto pages. In most cases, the map page size should be quite a bit larger than the auto page size.
	HTTP_PORT
		Required when running as a standalone server. Unused when running in CLI or Lambda modes.
	DRIVER
		The storage driver for storing data. Should be set to 'filesystem' when running on a server or when
		using an EFS volume with Lambda, and 's3' when using an S3 bucket.
	PAGE_EXTENSION=.kb
		The file extension for keybite data files.
	AWS_ACCESS_KEY_ID
		The AWS access key ID. Only required when using the S3 driver. This environment variable is set
		automatically in Lambda environments.
	AWS_SECRET_ACCESS_KEY
		The AWS access key ID and access key. Only required when using the S3 driver. This environment
		variable is set automatically in Lambda environments.
	BUCKET_NAME
		The name of the S3 bucket where keybite should store data.
	ENVIRONMENT=linux
		The environment in which keybite is running. Either 'linux' or 'lambda'.
	LOG_LEVEL=debug
		The detail level of logs that should be printed to stderr. One of 'error', 'warn', 'info' or 'debug'.
		'error' only logs critical errors. Default is 'warn' when an invalid log level is provided.
	LOCK_DURATION_FS
		Duration of write locks in milliseconds when using the filesystem driver. Unnecessary when using S3
		driver.
	LOCK_DURATION_S3
		Duration of write locks in milliseconds when using the S3 driver. Unnecessary when using filesystem
		driver.
	

`

// DisplayHelp displays help text
func DisplayHelp() {
	fmt.Println(helpText)
}

/**
 * Configuration settings for the database system.
 */
export interface DuckDbDatabaseOptions {
    /** The current calendar. @default System (locale) calendar */
    calendar: string

    /** The current time zone. @default System (locale) timezone */
    timeZone: string

    /** Access mode of the database (AUTOMATIC, READ_ONLY or READ_WRITE). @default automatic */
    access_mode: "AUTOMATIC" | "READ_ONLY" | "READ_WRITE"

    /** Peak allocation threshold at which to flush the allocator after completing a task. @default 128.0 MiB */
    allocator_flush_threshold: string

    /** Allow the creation of persistent secrets, that are stored and loaded on restarts. @default true */
    allow_persistent_secrets: boolean

    /** Allow to load extensions with invalid or missing signatures. @default false */
    allow_unsigned_extensions: boolean

    /** If arrow buffers for strings, blobs, uuids and bits should be exported using large buffers. @default false */
    arrow_large_buffer_size: boolean

    /** Overrides the custom endpoint for extension installation on autoloading. */
    autoinstall_extension_repository?: string

    /** Whether known extensions are allowed to be automatically installed when a query depends on them. @default true */
    autoinstall_known_extensions: boolean

    /** Whether known extensions are allowed to be automatically loaded when a query depends on them. @default true */
    autoload_known_extensions: boolean

    /** In Parquet files, interpret binary data as a string. */
    binary_as_string?: boolean

    /** The WAL size threshold at which to automatically trigger a checkpoint (e.g., 1GB). @default 16.0 MiB */
    checkpoint_threshold: string

    /** Overrides the custom endpoint for remote extension installation. */
    custom_extension_repository?: string

    /** Metadata from DuckDB callers. */
    custom_user_agent?: string

    /** The collation setting used when none is specified. */
    default_collation?: string

    /** Null ordering used when none is specified (NULLS_FIRST or NULLS_LAST). @default NULLS_LAST */
    default_null_order: "NULLS_FIRST" | "NULLS_LAST"

    /** The order type used when none is specified (ASC or DESC). @default ASC */
    default_order: "ASC" | "DESC"

    /** Allows switching the default storage for secrets. @default local_file */
    default_secret_storage: string

    /** Disable specific file systems preventing access (e.g., LocalFileSystem). */
    disabled_filesystems?: string

    /** DuckDB API surface. @default duckdb/v0.9.3-dev1990(linux_amd64_gcc4) */
    duckdb_api: string

    /** Allow the database to access external state. @default true */
    enable_external_access: boolean

    /** Allow scans on FSST compressed segments to emit compressed vectors. @default false */
    enable_fsst_vectors: boolean

    /** Whether or not the global http metadata is used to cache HTTP metadata. @default false */
    enable_http_metadata_cache: boolean

    /** Whether or not object cache is used to cache e.g., Parquet metadata. @default false */
    enable_object_cache: boolean

    /** Enables profiling, and sets the output format (JSON, QUERY_TREE, QUERY_TREE_OPTIMIZER). */
    enable_profiling?: string

    /** Controls the printing of the progress bar, when ‘enable_progress_bar’ is true. @default true */
    enable_progress_bar_print: boolean

    /** Enables the progress bar, printing progress to the terminal for long queries. @default false */
    enable_progress_bar: boolean

    /** Output of EXPLAIN statements (ALL, OPTIMIZED_ONLY, PHYSICAL_ONLY). @default physical_only */
    explain_output: "ALL" | "OPTIMIZED_ONLY" | "PHYSICAL_ONLY"

    /** Set the directory to store extensions in. */
    extension_directory?: string

    /** The number of external threads that work on DuckDB tasks. @default 0 */
    external_threads: bigint

    /** A comma separated list of directories to search for input files. */
    file_search_path?: string

    /** Forces upfront download of file. @default false */
    force_download: boolean

    /** Sets the home directory used by the system. */
    home_directory?: string

    /** Keep alive connections. Setting this to false can help when running into connection failures. @default true */
    http_keep_alive: boolean

    /** HTTP retries on I/O error (default 3). @default 3 */
    http_retries: bigint

    /** Backoff factor for exponentially increasing retry wait time (default 4). @default 4 */
    http_retry_backoff: number

    /** Time between retries (default 100ms). @default 100 */
    http_retry_wait_ms: bigint

    /** HTTP timeout read/write/connection/retry (default 30000ms). @default 30000 */
    http_timeout: bigint

    /** Whether transactions should be started lazily when needed, or immediately when BEGIN TRANSACTION is called. @default false */
    immediate_transaction_mode: boolean

    /** Whether or not the / operator defaults to integer division, or to floating point division. @default false */
    integer_division: boolean

    /** Whether or not the configuration can be altered. @default false */
    lock_configuration: boolean

    /** Specifies the path to which queries should be logged (default: empty string, queries are not logged). */
    log_query_path?: string

    /** The maximum expression depth limit in the parser. WARNING: increasing this setting and using very deep expressions might lead to stack overflow errors. @default 1000 */
    max_expression_depth: bigint

    /** The maximum memory of the system (e.g., 1GB). @default 80% of RAM */
    max_memory: string

    /** The number of rows to accumulate before sorting, used for tuning. @default 262144 */
    ordered_aggregate_threshold: bigint

    /** The password to use. Ignored for legacy compatibility. */
    password?: string

    /** Threshold in bytes for when to use a perfect hash table (default: 12). @default 12 */
    perfect_ht_threshold: bigint

    /** The threshold to switch from using filtered aggregates to LIST with a dedicated pivot operator. @default 10 */
    pivot_filter_threshold: bigint

    /** The maximum number of pivot columns in a pivot statement (default: 100000). @default 100000 */
    pivot_limit: bigint

    /** Force use of range joins with mixed predicates. @default false */
    prefer_range_joins: boolean

    /** Whether or not to preserve the identifier case, instead of always lowercasing all non-quoted identifiers. @default true */
    preserve_identifier_case: boolean

    /** Whether or not to preserve insertion order. If set to false the system is allowed to re-order any results that do not contain ORDER BY clauses. @default true */
    preserve_insertion_order: boolean

    /** The file to which profile output should be saved, or empty to print to the terminal. */
    profile_output?: string

    /** Sets the profiler history size. */
    profiler_history_size?: bigint

    /** The profiling mode (STANDARD or DETAILED). */
    profiling_mode?: string

    /** Sets the time (in milliseconds) how long a query needs to take before we start printing a progress bar. @default 2000 */
    progress_bar_time: bigint

    /** S3 Access Key ID. */
    s3_access_key_id?: string

    /** S3 Endpoint (empty for default endpoint). */
    s3_endpoint?: string

    /** S3 Region (default us-east-1). @default us-east-1 */
    s3_region: string

    /** S3 Access Key. */
    s3_secret_access_key?: string

    /** S3 Session Token. */
    s3_session_token?: string

    /** S3 Uploader max filesize (between 50GB and 5TB, default 800GB). @default 800GB */
    s3_uploader_max_filesize: string

    /** S3 Uploader max parts per file (between 1 and 10000, default 10000). @default 10000 */
    s3_uploader_max_parts_per_file: bigint

    /** S3 Uploader global thread limit (default 50). @default 50 */
    s3_uploader_thread_limit: bigint

    /** Disable Globs and Query Parameters on S3 URLs. @default false */
    s3_url_compatibility_mode: boolean

    /** S3 URL style (‘vhost’ (default) or ‘path’). @default vhost */
    s3_url_style: "vhost" | "path"

    /** S3 use SSL (default true). @default true */
    s3_use_ssl: boolean

    /** Sets the default search schema. Equivalent to setting search_path to a single value. */
    schema?: string

    /** Sets the default catalog search path as a comma-separated list of values. */
    search_path?: string

    /** Set the directory to which persistent secrets are stored. @default /home/runner/.duckdb/stored_secrets/11e1868051 */
    secret_directory: string

    /** Set the directory to which to write temp files. */
    temp_directory?: string

    /** The number of total threads used by the system. @default # Cores */
    threads: bigint

    /** The username to use. Ignored for legacy compatibility. */
    username?: string
}

import { BaseDataSourceOptions } from "../../data-source/BaseDataSourceOptions"
import { DuckDbDatabaseOptions } from "./DuckDbDatabaseOptions"


/**
 * Sqlite-specific connection options.
 */
export interface DuckDbConnectionOptions extends BaseDataSourceOptions {
    /**
     * Database type.
     */
    readonly type: "duckdb"

    /**
     * Storage type or path to the storage.
     */
    readonly database: string

    /**
     * The driver object
     * This defaults to require("duckdb")
     */
    readonly driver?: any

    dbOptions?: DuckDbDatabaseOptions
}

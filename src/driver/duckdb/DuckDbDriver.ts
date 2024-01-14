import mkdirp from "mkdirp"
import path from "path"
import { DriverPackageNotInstalledError } from "../../error/DriverPackageNotInstalledError"
import { PlatformTools } from "../../platform/PlatformTools"
import { DataSource } from "../../data-source/DataSource"
import { DuckDbConnectionOptions } from "./DuckDbConnectionOptions"
import { ColumnType } from "../types/ColumnTypes"
import { QueryRunner } from "../../query-runner/QueryRunner"
import { ReplicationMode } from "../types/ReplicationMode"
import { isAbsolute } from "../../util/PathUtils"
import { Driver, ReturningType } from "../Driver"
import * as duckdb from "duckdb"
import { DuckDbQueryRunner } from "./DuckDbQueryRunner"
import { ObjectLiteral } from "../../common/ObjectLiteral"
import { ColumnMetadata } from "../../metadata/ColumnMetadata"
import { EntityMetadata } from "../../metadata/EntityMetadata"
import { Table } from "../../schema-builder/table/Table"
import { TableColumn } from "../../schema-builder/table/TableColumn"
import { TableForeignKey } from "../../schema-builder/table/TableForeignKey"
import { View } from "../../schema-builder/view/View"
import { CteCapabilities } from "../types/CteCapabilities"
import { DataTypeDefaults } from "../types/DataTypeDefaults"
import { MappedColumnTypes } from "../types/MappedColumnTypes"
import { UpsertType } from "../types/UpsertType"
import { RdbmsSchemaBuilder } from "../../schema-builder/RdbmsSchemaBuilder"
import { DateUtils } from "../../util/DateUtils"
import { InstanceChecker } from "../../util/InstanceChecker"
import { ApplyValueTransformers } from "../../util/ApplyValueTransformers"
import { OrmUtils } from "../../util/OrmUtils"

type DatabasesMap = Record<
    string,
    {
        attachFilepathAbsolute: string
        attachFilepathRelative: string
        attachHandle: string
    }
>


export function isInMemoryDatabase(
    database: string
): database is ":memory:" {
    return database === ":memory:"
}

/**
 * Organizes communication with duckdb DBMS.
 */
export class DuckDbDriver implements Driver {
    // -------------------------------------------------------------------------
    // Public Properties
    // -------------------------------------------------------------------------
    
    /**
     * SQLite underlying library.
     */
    public readonly duckdb: typeof duckdb

    /**
     * DuckDb has a single QueryRunner because it works on a single database connection.
     */
    public queryRunner?: QueryRunner

    /**
     * Real database connection with duckdb database.
     * We are not using duckdb.Connection for now because there is no need for it.
     * Else we'd need to have a connection pool
     */
    public databaseConnection: duckdb.Database
   
    // -------------------------------------------------------------------------
    // Public Implemented Properties
    // -------------------------------------------------------------------------

    /**
     * Connection options.
     */
    options: DuckDbConnectionOptions
   
    isReplicated = false

    treeSupport = false // TODO check if this is true
    
    transactionSupport: "none" | "simple" | "nested" = "simple" // TODO check if this is true

    supportedDataTypes: ColumnType[] = [
        // ints
        "int",
        "tinyint",
        "smallint",
        "bigint",
        "int2",
        "int4",
        "int8",
        "integer",
        // bool
        "boolean",
        "bool",
        // floats
        "float",
        "float4",
        "real",
        "double",
        "numeric",
        "float8",
        "decimal",
        // varchar
        "char",
        "varchar",
        "text",
        "string",
        // date / time
        "date",
        "datetime",
        "timestamp",
        "time",
        "timestamp with time zone",
        "timestamptz",
        // others
        "uuid",
        "blob",
        "binary",
        "bytea",
        "varbinary"
    ]

    supportedUpsertTypes: UpsertType[] = [] // TODO

    dataTypeDefaults: DataTypeDefaults

    spatialTypes: ColumnType[] = []

    // TODO implement
    withLengthColumnTypes: ColumnType[] = [
        "char",
        "varchar",
        "text",
        "string",
        "blob",
        "nvarchar",
        "binary",
        "varbinary"
    ]
    
    // TODO implement
    withPrecisionColumnTypes: ColumnType[] = [
        "float",
        "float4",
        "real",
        "double",
        "numeric",
        "float8",
        "decimal"
    ]
    
    withScaleColumnTypes: ColumnType[] = [
        "float",
        "float4",
        "real",
        "double",
        "numeric",
        "float8",
        "decimal"
    ]

    mappedDataTypes: MappedColumnTypes = {
        createDate: "timestamp",
        createDateDefault: "now()",
        updateDate: "timestamp",
        updateDateDefault: "now()",
        deleteDate: "timestamp",
        deleteDateNullable: true,
        version: "int4",
        treeLevel: "int4",
        migrationId: "int4",
        migrationName: "varchar",
        migrationTimestamp: "int8",
        cacheId: "int4",
        cacheIdentifier: "varchar",
        cacheTime: "int8",
        cacheDuration: "int4",
        cacheQuery: "text",
        cacheResult: "text",
        metadataType: "varchar",
        metadataDatabase: "varchar",
        metadataSchema: "varchar",
        metadataTable: "varchar",
        metadataName: "varchar",
        metadataValue: "text",
    }

    parametersPrefix: string = "$"

    uuidGenerator = "gen_random_uuid()"

    maxAliasLength = 63 // same as postgres?

    // TODO check if this is true
    cteCapabilities: CteCapabilities = {
        enabled: false
    }

    // -------------------------------------------------------------------------
    // Protected Properties
    // -------------------------------------------------------------------------

    /**
     * Any attached databases (excepting default 'main')
     */
    attachedDatabases: DatabasesMap = {}
    
    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    constructor(public connection: DataSource) {
        this.options = connection.options as DuckDbConnectionOptions

        // load duckdb package
        try {
            this.duckdb = this.options.driver || PlatformTools.load("duckdb")
        } catch (e) {
            throw new DriverPackageNotInstalledError("DuckDb", "duckdb")
        }
    }

    // -------------------------------------------------------------------------
    // Public Methods
    // -------------------------------------------------------------------------

    /**
     * Closes connection with database.
     */
    async disconnect(): Promise<void> {
        return new Promise<void>((ok, fail) => {
            this.queryRunner = undefined
            this.databaseConnection.close((err: any) =>
                err ? fail(err) : ok(),
            )
        })
    }

    getAttachedDatabaseHandleByRelativePath(path: string): string | undefined {
        return this.attachedDatabases?.[path]?.attachHandle
    }

    /**
     * Creates a query runner used to execute database queries.
     */
    createQueryRunner(mode: ReplicationMode): QueryRunner {
        if (!this.queryRunner) this.queryRunner = new DuckDbQueryRunner(this)

        return this.queryRunner
    }

    
    /**
     * Creates a database type from a given column metadata.
     */
    normalizeType(column: {
        type?: ColumnType
        length?: number | string
        precision?: number | null
        scale?: number
    }): string {
        if (column.type === Number || column.type === "int") {
            return "integer"
        } else if (column.type === String) {
            return "varchar"
        } else if (column.type === Date) {
            return "datetime"
        } else if (column.type === Boolean) {
            return "boolean"
        } else if (column.type === "uuid") {
            return "varchar"
        } else if (column.type === "simple-array") {
            return "text"
        } else if (column.type === "simple-json") {
            return "text"
        } else if (column.type === "simple-enum") {
            return "varchar"
        } else {
            return (column.type as string) || ""
        }
    }

    async afterConnect(): Promise<void> {
        return this.attachDatabases()
    }

    /**
     * For SQLite, the database may be added in the decorator metadata. It will be a filepath to a database file.
     */
    buildTableName(
        tableName: string,
        _schema?: string,
        database?: string,
    ): string {
        if (!database) return tableName
        if (this.getAttachedDatabaseHandleByRelativePath(database))
            return `${this.getAttachedDatabaseHandleByRelativePath(
                database,
            )}.${tableName}`

        if (database === this.options.database) return tableName
        
        throw new Error("Not implemented for custom database.")
    }

    async connect(): Promise<void> {
        console.warn("DuckDbDriver connect")
        this.databaseConnection = await this.createDatabaseConnection()
        console.warn("DuckDbDriver connected")
    }

    createSchemaBuilder() {
        return new RdbmsSchemaBuilder(this.connection)
    }
    

    /**
     * Replaces parameters in the given sql with special escaping character
     * and an array of parameter names to be passed to a query.
     */
    escapeQueryWithParameters(
        sql: string,
        parameters: ObjectLiteral,
        nativeParameters: ObjectLiteral,
    ): [string, any[]] {
        const escapedParameters: any[] = Object.keys(nativeParameters).map(
            (key) => {
                // Mapping boolean values to their numeric representation
                if (typeof nativeParameters[key] === "boolean") {
                    return nativeParameters[key] === true ? 1 : 0
                }

                if (nativeParameters[key] instanceof Date) {
                    return DateUtils.mixedDateToUtcDatetimeString(
                        nativeParameters[key],
                    )
                }

                return nativeParameters[key]
            },
        )

        if (!parameters || !Object.keys(parameters).length)
            return [sql, escapedParameters]

        sql = sql.replace(
            /:(\.\.\.)?([A-Za-z0-9_.]+)/g,
            (full, isArray: string, key: string): string => {
                if (!parameters.hasOwnProperty(key)) {
                    return full
                }

                let value: any = parameters[key]

                if (isArray) {
                    return value
                        .map((v: any) => {
                            escapedParameters.push(v)
                            return this.createParameter(
                                key,
                                escapedParameters.length - 1,
                            )
                        })
                        .join(", ")
                }

                if (typeof value === "function") {
                    return value()
                } else if (typeof value === "number") {
                    return String(value)
                }

                // Sqlite does not have a boolean data type so we have to transform
                // it to 1 or 0
                if (typeof value === "boolean") {
                    escapedParameters.push(+value)
                    return this.createParameter(
                        key,
                        escapedParameters.length - 1,
                    )
                }

                if (value instanceof Date) {
                    escapedParameters.push(
                        DateUtils.mixedDateToUtcDatetimeString(value),
                    )
                    return this.createParameter(
                        key,
                        escapedParameters.length - 1,
                    )
                }

                escapedParameters.push(value)
                return this.createParameter(key, escapedParameters.length - 1)
            },
        ) // todo: make replace only in value statements, otherwise problems
        return [sql, escapedParameters]
    }

    escape(columnName: string): string {
        return '"' + columnName + '"'
    }

    
    /**
     * Parse a target table name or other types and return a normalized table definition.
     */
    parseTableName(
        target: EntityMetadata | Table | View | TableForeignKey | string,
    ): { database?: string; schema?: string; tableName: string } {
        const driverDatabase = this.options.database

        if (InstanceChecker.isTable(target) || InstanceChecker.isView(target)) {
            const parsed = this.parseTableName(
                target.schema
                    ? `"${target.schema}"."${target.name}"`
                    : target.name,
            )

            return {
                database: target.database || parsed.database || driverDatabase,
                schema: target.schema || parsed.schema || undefined,
                tableName: parsed.tableName,
            }
        }

        if (InstanceChecker.isTableForeignKey(target)) {
            const parsed = this.parseTableName(target.referencedTableName)

            return {
                database:
                    target.referencedDatabase ||
                    parsed.database ||
                    driverDatabase,
                schema:
                    target.referencedSchema || parsed.schema || undefined,
                tableName: parsed.tableName,
            }
        }

        if (InstanceChecker.isEntityMetadata(target)) {
            // EntityMetadata tableName is never a path

            return {
                database: target.database || driverDatabase,
                schema: target.schema || undefined,
                tableName: target.tableName,
            }
        }

        const parts = target.split(".")

        if (parts.length === 3) {
            return {
                database: parts[0] || driverDatabase,
                schema: parts[1] || undefined,
                tableName: parts[2],
            }
        } else if (parts.length === 2) {
            const database =
                this.getAttachedDatabasePathRelativeByHandle(parts[0]) ??
                driverDatabase
            return {
                database: database,
                schema: parts[0],
                tableName: parts[1],
            }
        } else {
            return {
                database: driverDatabase,
                schema: undefined,
                tableName: target,
            }
        }
    }

    getAttachedDatabasePathRelativeByHandle(
        handle: string,
    ): string | undefined {
        return Object.values(this.attachedDatabases).find(
            ({ attachHandle }) => handle === attachHandle,
        )?.attachFilepathRelative
    }


    /**
     * Prepares given value to a value to be persisted, based on its column type and metadata.
     */
    preparePersistentValue(value: any, columnMetadata: ColumnMetadata): any {
        if (columnMetadata.transformer)
            value = ApplyValueTransformers.transformTo(
                columnMetadata.transformer,
                value,
            )

        if (value === null || value === undefined) return value

        if (
            columnMetadata.type === Boolean ||
            columnMetadata.type === "boolean"
        ) {
            return value === true ? 1 : 0
        } else if (columnMetadata.type === "date") {
            return DateUtils.mixedDateToDateString(value)
        } else if (columnMetadata.type === "time") {
            return DateUtils.mixedDateToTimeString(value)
        } else if (
            columnMetadata.type === "datetime" ||
            columnMetadata.type === Date
        ) {
            // to string conversation needs because SQLite stores date as integer number, when date came as Object
            // TODO: think about `toUTC` conversion
            return DateUtils.mixedDateToUtcDatetimeString(value)
        } else if (
            columnMetadata.type === "json" ||
            columnMetadata.type === "simple-json"
        ) {
            return DateUtils.simpleJsonToString(value)
        } else if (columnMetadata.type === "simple-array") {
            return DateUtils.simpleArrayToString(value)
        } else if (columnMetadata.type === "simple-enum") {
            return DateUtils.simpleEnumToString(value)
        }

        return value
    }

    /**
     * Prepares given value to a value to be hydrated, based on its column type or metadata.
     */
    prepareHydratedValue(value: any, columnMetadata: ColumnMetadata): any {
        if (value === null || value === undefined)
            return columnMetadata.transformer
                ? ApplyValueTransformers.transformFrom(
                      columnMetadata.transformer,
                      value,
                  )
                : value

        if (
            columnMetadata.type === Boolean ||
            columnMetadata.type === "boolean"
        ) {
            value = value ? true : false
        } else if (
            columnMetadata.type === "datetime" ||
            columnMetadata.type === Date
        ) {
            /**
             * Fix date conversion issue
             *
             * If the format of the date string is "2018-03-14 02:33:33.906", Safari (and iOS WKWebView) will convert it to an invalid date object.
             * We need to modify the date string to "2018-03-14T02:33:33.906Z" and Safari will convert it correctly.
             *
             * ISO 8601
             * https://www.w3.org/TR/NOTE-datetime
             */
            if (value && typeof value === "string") {
                // There are various valid time string formats a sqlite time string might have:
                // https://www.sqlite.org/lang_datefunc.html
                // There are two separate fixes we may need to do:
                //   1) Add 'T' separator if space is used instead
                //   2) Add 'Z' UTC suffix if no timezone or offset specified

                if (/^\d\d\d\d-\d\d-\d\d \d\d:\d\d/.test(value)) {
                    value = value.replace(" ", "T")
                }
                if (
                    /^\d\d\d\d-\d\d-\d\dT\d\d:\d\d(:\d\d(\.\d\d\d)?)?$/.test(
                        value,
                    )
                ) {
                    value += "Z"
                }
            }

            value = DateUtils.normalizeHydratedDate(value)
        } else if (columnMetadata.type === "date") {
            value = DateUtils.mixedDateToDateString(value)
        } else if (columnMetadata.type === "time") {
            value = DateUtils.mixedTimeToString(value)
        } else if (
            columnMetadata.type === "json" ||
            columnMetadata.type === "simple-json"
        ) {
            value = DateUtils.stringToSimpleJson(value)
        } else if (columnMetadata.type === "simple-array") {
            value = DateUtils.stringToSimpleArray(value)
        } else if (columnMetadata.type === "simple-enum") {
            value = DateUtils.stringToSimpleEnum(value, columnMetadata)
        } else if (columnMetadata.type === Number) {
            // convert to number if number
            value = !isNaN(+value) ? parseInt(value) : value
        }

        if (columnMetadata.transformer)
            value = ApplyValueTransformers.transformFrom(
                columnMetadata.transformer,
                value,
            )

        return value
    }

    
    /**
     * Normalizes "default" value of the column.
     */
    normalizeDefault(columnMetadata: ColumnMetadata): string | undefined {
        const defaultValue = columnMetadata.default

        if (typeof defaultValue === "number") {
            return "" + defaultValue
        }

        if (typeof defaultValue === "boolean") {
            return defaultValue ? "1" : "0"
        }

        if (typeof defaultValue === "function") {
            return defaultValue()
        }

        if (typeof defaultValue === "string") {
            return `'${defaultValue}'`
        }

        if (defaultValue === null || defaultValue === undefined) {
            return undefined
        }

        return `${defaultValue}`
    }

    
    /**
     * Normalizes "isUnique" value of the column.
     */
    normalizeIsUnique(column: ColumnMetadata): boolean {
        return column.entityMetadata.uniques.some(
            (uq) => uq.columns.length === 1 && uq.columns[0] === column,
        )
    }

    /**
     * Calculates column length taking into account the default length values.
     */
    getColumnLength(column: ColumnMetadata): string {
        return column.length ? column.length.toString() : ""
    }

    /**
     * Normalizes "default" value of the column.
     */
    createFullType(column: TableColumn): string {
        let type = column.type
        if (column.enum) {
            return "varchar"
        }
        if (column.length) {
            type += "(" + column.length + ")"
        } else if (
            column.precision !== null &&
            column.precision !== undefined &&
            column.scale !== null &&
            column.scale !== undefined
        ) {
            type += "(" + column.precision + "," + column.scale + ")"
        } else if (
            column.precision !== null &&
            column.precision !== undefined
        ) {
            type += "(" + column.precision + ")"
        }

        if (column.isArray) type += " array"

        return type
    }

    async obtainMasterConnection(): Promise<duckdb.Database> {
        return this.databaseConnection
    }

    obtainSlaveConnection(): Promise<duckdb.Database> {
        return this.obtainMasterConnection()
    }

    createGeneratedMap(
        metadata: EntityMetadata,
        insertResult: any,
        entityIndex: number,
        entityNum: number,
    ) {
        const generatedMap = metadata.generatedColumns.reduce(
            (map, generatedColumn) => {
                let value: any
                if (
                    generatedColumn.generationStrategy === "increment" &&
                    insertResult
                ) {
                    // NOTE: When INSERT statement is successfully completed, the last inserted row ID is returned.
                    // see also: SqliteQueryRunner.query()
                    value = insertResult - entityNum + entityIndex + 1
                    // } else if (generatedColumn.generationStrategy === "uuid") {
                    //     value = insertValue[generatedColumn.databaseName];
                }

                if (!value) return map
                return OrmUtils.mergeDeep(
                    map,
                    generatedColumn.createValueMap(value),
                )
            },
            {} as ObjectLiteral,
        )

        return Object.keys(generatedMap).length > 0 ? generatedMap : undefined
    }

    findChangedColumns(
        tableColumns: TableColumn[],
        columnMetadatas: ColumnMetadata[],
    ): ColumnMetadata[] {
        return columnMetadatas.filter((columnMetadata) => {
            const tableColumn = tableColumns.find(
                (c) => c.name === columnMetadata.databaseName,
            )
            if (!tableColumn) return false // we don't need new columns, we only need exist and changed

            const isColumnChanged =
                tableColumn.name !== columnMetadata.databaseName ||
                tableColumn.type !== this.normalizeType(columnMetadata) ||
                tableColumn.length !== columnMetadata.length ||
                tableColumn.precision !== columnMetadata.precision ||
                tableColumn.scale !== columnMetadata.scale ||
                this.normalizeDefault(columnMetadata) !== tableColumn.default ||
                tableColumn.isPrimary !== columnMetadata.isPrimary ||
                tableColumn.isNullable !== columnMetadata.isNullable ||
                tableColumn.generatedType !== columnMetadata.generatedType ||
                tableColumn.asExpression !== columnMetadata.asExpression ||
                tableColumn.isUnique !==
                    this.normalizeIsUnique(columnMetadata) ||
                (tableColumn.enum &&
                    columnMetadata.enum &&
                    !OrmUtils.isArraysEqual(
                        tableColumn.enum,
                        columnMetadata.enum.map((val) => val + ""),
                    )) ||
                (columnMetadata.generationStrategy !== "uuid" &&
                    tableColumn.isGenerated !== columnMetadata.isGenerated)

            // DEBUG SECTION
            // if (isColumnChanged) {
            //     console.log("table:", columnMetadata.entityMetadata.tableName)
            //     console.log(
            //         "name:",
            //         tableColumn.name,
            //         columnMetadata.databaseName,
            //     )
            //     console.log(
            //         "type:",
            //         tableColumn.type,
            //         this.normalizeType(columnMetadata),
            //     )
            //     console.log(
            //         "length:",
            //         tableColumn.length,
            //         columnMetadata.length,
            //     )
            //     console.log(
            //         "precision:",
            //         tableColumn.precision,
            //         columnMetadata.precision,
            //     )
            //     console.log("scale:", tableColumn.scale, columnMetadata.scale)
            //     console.log(
            //         "default:",
            //         this.normalizeDefault(columnMetadata),
            //         columnMetadata.default,
            //     )
            //     console.log(
            //         "isPrimary:",
            //         tableColumn.isPrimary,
            //         columnMetadata.isPrimary,
            //     )
            //     console.log(
            //         "isNullable:",
            //         tableColumn.isNullable,
            //         columnMetadata.isNullable,
            //     )
            //     console.log(
            //         "generatedType:",
            //         tableColumn.generatedType,
            //         columnMetadata.generatedType,
            //     )
            //     console.log(
            //         "asExpression:",
            //         tableColumn.asExpression,
            //         columnMetadata.asExpression,
            //     )
            //     console.log(
            //         "isUnique:",
            //         tableColumn.isUnique,
            //         this.normalizeIsUnique(columnMetadata),
            //     )
            //     console.log(
            //         "enum:",
            //         tableColumn.enum &&
            //             columnMetadata.enum &&
            //             !OrmUtils.isArraysEqual(
            //                 tableColumn.enum,
            //                 columnMetadata.enum.map((val) => val + ""),
            //             ),
            //     )
            //     console.log(
            //         "isGenerated:",
            //         tableColumn.isGenerated,
            //         columnMetadata.isGenerated,
            //     )
            // }

            return isColumnChanged
        })
    }

    isReturningSqlSupported(returningType: ReturningType): boolean {
        return returningType === "insert"
    }
    isUUIDGenerationSupported(): boolean {
        return true
    }

    /**
     * can probably be implemented with https://duckdb.org/docs/extensions/full_text_search
     */
    isFullTextColumnTypeSupported(): boolean {
        return false
    }

    createParameter(parameterName: string, index: number): string {
        // return "$" + (index + 1);
        return "?"
        // return "$" + parameterName;
    }

    // -------------------------------------------------------------------------
    // Protected Methods
    // -------------------------------------------------------------------------

    /**
     * Creates connection with the database.
     */
    protected async createDatabaseConnection(): Promise<duckdb.Database> {
        if (!isInMemoryDatabase(this.options.database)) {
            await this.createDatabaseDirectory(this.options.database)
        }

        return await new Promise((ok, fail) => {
            const db = new this.duckdb.Database(this.options.database, this.options.dbOptions as unknown as Record<string, string> ?? {}, (err: duckdb.DuckDbError) => {
                if (err) return fail(err)
                ok(db)
            })
        })

    }

    /**
     * Auto creates database directory if it does not exist.
     */
    protected async createDatabaseDirectory(fullPath: string): Promise<void> {
        await mkdirp(path.dirname(fullPath))
    }

    /**
     * Performs the attaching of the database files. The attachedDatabase should have been populated during calls to #buildTableName
     * during EntityMetadata production (see EntityMetadata#buildTablePath)
     *
     * https://sqlite.org/lang_attach.html
     */
    protected async attachDatabases() {
        // @todo - possibly check number of databases (but unqueriable at runtime sadly) - https://www.sqlite.org/limits.html#max_attached
        for await (const {
            attachHandle,
            attachFilepathAbsolute,
        } of Object.values(this.attachedDatabases)) {
            await this.createDatabaseDirectory(attachFilepathAbsolute)
            await this.connection.query(
                `ATTACH "${attachFilepathAbsolute}" AS "${attachHandle}"`,
            )
        }
    }

    protected getMainDatabasePath(): string {
        const optionsDb = this.options.database
        return path.dirname(
            isAbsolute(optionsDb)
                ? optionsDb
                : path.join(process.cwd(), optionsDb),
        )
    }
}

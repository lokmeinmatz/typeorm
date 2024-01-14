import { QueryRunnerAlreadyReleasedError } from "../../error/QueryRunnerAlreadyReleasedError"
import { QueryFailedError } from "../../error/QueryFailedError"
import { Broadcaster } from "../../subscriber/Broadcaster"
import { ConnectionIsNotSetError } from "../../error/ConnectionIsNotSetError"
import { QueryResult } from "../../query-runner/QueryResult"
import { BroadcasterResult } from "../../subscriber/BroadcasterResult"
import { BaseQueryRunner } from "../../query-runner/BaseQueryRunner"
import { QueryRunner } from "../../query-runner/QueryRunner"
import { DuckDbDriver } from "./DuckDbDriver"
import { ReadStream } from "fs"
import { Table } from "../../schema-builder/table/Table"
import { TableCheck } from "../../schema-builder/table/TableCheck"
import { TableColumn } from "../../schema-builder/table/TableColumn"
import { TableExclusion } from "../../schema-builder/table/TableExclusion"
import { TableForeignKey } from "../../schema-builder/table/TableForeignKey"
import { TableIndex } from "../../schema-builder/table/TableIndex"
import { TableUnique } from "../../schema-builder/table/TableUnique"
import { View } from "../../schema-builder/view/View"
import { ObjectLiteral } from "../../common/ObjectLiteral"
import type * as duckdb from "duckdb"
import { InstanceChecker } from "../../util/InstanceChecker"
import { MetadataTableType } from "../types/MetadataTableType"
import { OrmUtils } from "../../util/OrmUtils"
import { TableIndexOptions } from "../../schema-builder/options/TableIndexOptions"
import { Query } from "../Query"

/**
 * Runs queries on a single duckdb database connection.
 *
 * Does not support compose primary keys with autoincrement field.
 * todo: need to throw exception for this case.
 */
export class DuckDbQueryRunner extends BaseQueryRunner implements QueryRunner {
    /**
     * Database driver used by connection.
     */
    driver: DuckDbDriver

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    constructor(driver: DuckDbDriver) {
        super()
        this.driver = driver
        this.connection = driver.connection
        this.broadcaster = new Broadcaster(this)
    }

    // -------------------------------------------------------------------------
    // Public Methods
    // -------------------------------------------------------------------------

    /**
     * Loads all tables (with given names) from the database and creates a Table from them.
     */
    protected async loadTables(tableNames?: string[]): Promise<Table[]> {
        // if no tables given then no need to proceed
        if (tableNames && tableNames.length === 0) {
            return []
        }

        let dbTables: { database?: string; name: string; sql: string }[] = []
        let dbIndicesDef: ObjectLiteral[]

        if (!tableNames) {
            // duckdb defined sqlite_master for compability: https://duckdb.org/docs/guides/meta/list_tables#see-also
            const tablesSql = `SELECT * FROM "sqlite_master" WHERE "type" = 'table'`
            dbTables.push(...(await this.query(tablesSql)))

            const tableNamesString = dbTables
                .map(({ name }) => `'${name}'`)
                .join(", ")
            dbIndicesDef = await this.query(
                `SELECT * FROM "sqlite_master" WHERE "type" = 'index' AND "tbl_name" IN (${tableNamesString})`,
            )
        } else {
            const tableNamesWithoutDot = tableNames
                .filter((tableName) => {
                    return tableName.split(".").length === 1
                })
                .map((tableName) => `'${tableName}'`)

            const tableNamesWithDot = tableNames.filter((tableName) => {
                return tableName.split(".").length > 1
            })

            const queryPromises = (type: "table" | "index") => {
                const promises = [
                    ...tableNamesWithDot.map((tableName) =>
                        this.loadTableRecords(tableName, type),
                    ),
                ]

                if (tableNamesWithoutDot.length) {
                    promises.push(
                        this.query(
                            `SELECT * FROM "sqlite_master" WHERE "type" = '${type}' AND "${
                                type === "table" ? "name" : "tbl_name"
                            }" IN (${tableNamesWithoutDot})`,
                        ),
                    )
                }

                return promises
            }
            dbTables = (await Promise.all(queryPromises("table")))
                .reduce((acc, res) => [...acc, ...res], [])
                .filter(Boolean)
            dbIndicesDef = (await Promise.all(queryPromises("index")))
                .reduce((acc, res) => [...acc, ...res], [])
                .filter(Boolean)
        }

        // if tables were not found in the db, no need to proceed
        if (dbTables.length === 0) {
            return []
        }

        // create table schemas for loaded tables
        return Promise.all(
            dbTables.map(async (dbTable) => {
                const tablePath =
                    dbTable["database"] &&
                    this.driver.getAttachedDatabaseHandleByRelativePath(
                        dbTable["database"],
                    )
                        ? `${this.driver.getAttachedDatabaseHandleByRelativePath(
                              dbTable["database"],
                          )}.${dbTable["name"]}`
                        : dbTable["name"]

                const sql = dbTable["sql"]

                const withoutRowid = sql.includes("WITHOUT ROWID")
                const table = new Table({ name: tablePath, withoutRowid })

                // load columns and indices
                const [dbColumns, dbIndices, dbForeignKeys]: ObjectLiteral[][] =
                    await Promise.all([
                        this.loadPragmaRecords(tablePath, `table_xinfo`),
                        this.loadPragmaRecords(tablePath, `index_list`),
                        this.loadPragmaRecords(tablePath, `foreign_key_list`),
                    ])

                // find column name with auto increment
                let autoIncrementColumnName: string | undefined = undefined
                const tableSql: string = dbTable["sql"]
                const autoIncrementIndex = tableSql
                    .toUpperCase()
                    .indexOf("AUTOINCREMENT")
                if (autoIncrementIndex !== -1) {
                    autoIncrementColumnName = tableSql.substr(
                        0,
                        autoIncrementIndex,
                    )
                    const comma = autoIncrementColumnName.lastIndexOf(",")
                    const bracket = autoIncrementColumnName.lastIndexOf("(")
                    if (comma !== -1) {
                        autoIncrementColumnName =
                            autoIncrementColumnName.substr(comma)
                        autoIncrementColumnName =
                            autoIncrementColumnName.substr(
                                0,
                                autoIncrementColumnName.lastIndexOf('"'),
                            )
                        autoIncrementColumnName =
                            autoIncrementColumnName.substr(
                                autoIncrementColumnName.indexOf('"') + 1,
                            )
                    } else if (bracket !== -1) {
                        autoIncrementColumnName =
                            autoIncrementColumnName.substr(bracket)
                        autoIncrementColumnName =
                            autoIncrementColumnName.substr(
                                0,
                                autoIncrementColumnName.lastIndexOf('"'),
                            )
                        autoIncrementColumnName =
                            autoIncrementColumnName.substr(
                                autoIncrementColumnName.indexOf('"') + 1,
                            )
                    }
                }

                // create columns from the loaded columns
                table.columns = await Promise.all(
                    dbColumns.map(async (dbColumn) => {
                        const tableColumn = new TableColumn()
                        tableColumn.name = dbColumn["name"]
                        tableColumn.type = dbColumn["type"].toLowerCase()
                        tableColumn.default =
                            dbColumn["dflt_value"] !== null &&
                            dbColumn["dflt_value"] !== undefined
                                ? dbColumn["dflt_value"]
                                : undefined
                        tableColumn.isNullable = dbColumn["notnull"] === 0
                        // primary keys are numbered starting with 1, columns that aren't primary keys are marked with 0
                        tableColumn.isPrimary = dbColumn["pk"] > 0
                        tableColumn.comment = "" // DuckDB does not support column comments
                        tableColumn.isGenerated =
                            autoIncrementColumnName === dbColumn["name"]
                        if (tableColumn.isGenerated) {
                            tableColumn.generationStrategy = "increment"
                        }

                        if (
                            dbColumn["hidden"] === 2 ||
                            dbColumn["hidden"] === 3
                        ) {
                            tableColumn.generatedType =
                                dbColumn["hidden"] === 2 ? "VIRTUAL" : "STORED"

                            const asExpressionQuery =
                                this.selectTypeormMetadataSql({
                                    table: table.name,
                                    type: MetadataTableType.GENERATED_COLUMN,
                                    name: tableColumn.name,
                                })

                            const results = await this.query(
                                asExpressionQuery.query,
                                asExpressionQuery.parameters,
                            )
                            if (results[0] && results[0].value) {
                                tableColumn.asExpression = results[0].value
                            } else {
                                tableColumn.asExpression = ""
                            }
                        }

                        if (tableColumn.type === "varchar") {
                            tableColumn.enum = OrmUtils.parseSqlCheckExpression(
                                sql,
                                tableColumn.name,
                            )
                        }

                        // parse datatype and attempt to retrieve length, precision and scale
                        const pos = tableColumn.type.indexOf("(")
                        if (pos !== -1) {
                            const fullType = tableColumn.type
                            const dataType = fullType.substr(0, pos)
                            if (
                                !!this.driver.withLengthColumnTypes.find(
                                    (col) => col === dataType,
                                )
                            ) {
                                const len = parseInt(
                                    fullType.substring(
                                        pos + 1,
                                        fullType.length - 1,
                                    ),
                                )
                                if (len) {
                                    tableColumn.length = len.toString()
                                    tableColumn.type = dataType // remove the length part from the datatype
                                }
                            }
                            if (
                                !!this.driver.withPrecisionColumnTypes.find(
                                    (col) => col === dataType,
                                )
                            ) {
                                const re = new RegExp(
                                    `^${dataType}\\((\\d+),?\\s?(\\d+)?\\)`,
                                )
                                const matches = fullType.match(re)
                                if (matches && matches[1]) {
                                    tableColumn.precision = +matches[1]
                                }
                                if (
                                    !!this.driver.withScaleColumnTypes.find(
                                        (col) => col === dataType,
                                    )
                                ) {
                                    if (matches && matches[2]) {
                                        tableColumn.scale = +matches[2]
                                    }
                                }
                                tableColumn.type = dataType // remove the precision/scale part from the datatype
                            }
                        }

                        return tableColumn
                    }),
                )

                // find foreign key constraints from CREATE TABLE sql
                let fkResult
                const fkMappings: {
                    name: string
                    columns: string[]
                    referencedTableName: string
                }[] = []
                const fkRegex =
                    /CONSTRAINT "([^"]*)" FOREIGN KEY ?\((.*?)\) REFERENCES "([^"]*)"/g
                while ((fkResult = fkRegex.exec(sql)) !== null) {
                    fkMappings.push({
                        name: fkResult[1],
                        columns: fkResult[2]
                            .substr(1, fkResult[2].length - 2)
                            .split(`", "`),
                        referencedTableName: fkResult[3],
                    })
                }

                // build foreign keys
                const tableForeignKeyConstraints = OrmUtils.uniq(
                    dbForeignKeys,
                    (dbForeignKey) => dbForeignKey["id"],
                )

                table.foreignKeys = tableForeignKeyConstraints.map(
                    (foreignKey) => {
                        const ownForeignKeys = dbForeignKeys.filter(
                            (dbForeignKey) =>
                                dbForeignKey["id"] === foreignKey["id"] &&
                                dbForeignKey["table"] === foreignKey["table"],
                        )
                        const columnNames = ownForeignKeys.map(
                            (dbForeignKey) => dbForeignKey["from"],
                        )
                        const referencedColumnNames = ownForeignKeys.map(
                            (dbForeignKey) => dbForeignKey["to"],
                        )

                        // find related foreign key mapping
                        const fkMapping = fkMappings.find(
                            (it) =>
                                it.referencedTableName ===
                                    foreignKey["table"] &&
                                it.columns.every(
                                    (column) =>
                                        columnNames.indexOf(column) !== -1,
                                ),
                        )

                        return new TableForeignKey({
                            name: fkMapping?.name,
                            columnNames: columnNames,
                            referencedTableName: foreignKey["table"],
                            referencedColumnNames: referencedColumnNames,
                            onDelete: foreignKey["on_delete"],
                            onUpdate: foreignKey["on_update"],
                        })
                    },
                )

                // find unique constraints from CREATE TABLE sql
                let uniqueRegexResult
                const uniqueMappings: { name: string; columns: string[] }[] = []
                const uniqueRegex = /CONSTRAINT "([^"]*)" UNIQUE ?\((.*?)\)/g
                while ((uniqueRegexResult = uniqueRegex.exec(sql)) !== null) {
                    uniqueMappings.push({
                        name: uniqueRegexResult[1],
                        columns: uniqueRegexResult[2]
                            .substr(1, uniqueRegexResult[2].length - 2)
                            .split(`", "`),
                    })
                }

                // build unique constraints
                const tableUniquePromises = dbIndices
                    .filter((dbIndex) => dbIndex["origin"] === "u")
                    .map((dbIndex) => dbIndex["name"])
                    .filter(
                        (value, index, self) => self.indexOf(value) === index,
                    )
                    .map(async (dbIndexName) => {
                        const dbIndex = dbIndices.find(
                            (dbIndex) => dbIndex["name"] === dbIndexName,
                        )
                        const indexInfos: ObjectLiteral[] = await this.query(
                            `PRAGMA index_info("${dbIndex!["name"]}")`,
                        )
                        const indexColumns = indexInfos
                            .sort(
                                (indexInfo1, indexInfo2) =>
                                    parseInt(indexInfo1["seqno"]) -
                                    parseInt(indexInfo2["seqno"]),
                            )
                            .map((indexInfo) => indexInfo["name"])
                        if (indexColumns.length === 1) {
                            const column = table.columns.find((column) => {
                                return !!indexColumns.find(
                                    (indexColumn) =>
                                        indexColumn === column.name,
                                )
                            })
                            if (column) column.isUnique = true
                        }

                        // find existent mapping by a column names
                        const foundMapping = uniqueMappings.find((mapping) => {
                            return mapping!.columns.every(
                                (column) => indexColumns.indexOf(column) !== -1,
                            )
                        })

                        return new TableUnique({
                            name: foundMapping
                                ? foundMapping.name
                                : this.connection.namingStrategy.uniqueConstraintName(
                                      table,
                                      indexColumns,
                                  ),
                            columnNames: indexColumns,
                        })
                    })
                table.uniques = (await Promise.all(
                    tableUniquePromises,
                )) as TableUnique[]

                // build checks
                let result
                const regexp =
                    /CONSTRAINT "([^"]*)" CHECK ?(\(.*?\))([,]|[)]$)/g
                while ((result = regexp.exec(sql)) !== null) {
                    table.checks.push(
                        new TableCheck({
                            name: result[1],
                            expression: result[2],
                        }),
                    )
                }

                // build indices
                const indicesPromises = dbIndices
                    .filter((dbIndex) => dbIndex["origin"] === "c")
                    .map((dbIndex) => dbIndex["name"])
                    .filter(
                        (value, index, self) => self.indexOf(value) === index,
                    ) // unqiue
                    .map(async (dbIndexName) => {
                        const indexDef = dbIndicesDef.find(
                            (dbIndexDef) => dbIndexDef["name"] === dbIndexName,
                        )
                        const condition = /WHERE (.*)/.exec(indexDef!["sql"])
                        const dbIndex = dbIndices.find(
                            (dbIndex) => dbIndex["name"] === dbIndexName,
                        )
                        const indexInfos: ObjectLiteral[] = await this.query(
                            `PRAGMA index_info("${dbIndex!["name"]}")`,
                        )
                        const indexColumns = indexInfos
                            .sort(
                                (indexInfo1, indexInfo2) =>
                                    parseInt(indexInfo1["seqno"]) -
                                    parseInt(indexInfo2["seqno"]),
                            )
                            .map((indexInfo) => indexInfo["name"])
                        const dbIndexPath = `${
                            dbTable["database"] ? `${dbTable["database"]}.` : ""
                        }${dbIndex!["name"]}`

                        const isUnique =
                            dbIndex!["unique"] === "1" ||
                            dbIndex!["unique"] === 1
                        return new TableIndex(<TableIndexOptions>{
                            table: table,
                            name: dbIndexPath,
                            columnNames: indexColumns,
                            isUnique: isUnique,
                            where: condition ? condition[1] : undefined,
                        })
                    })
                const indices = await Promise.all(indicesPromises)
                table.indices = indices.filter(
                    (index) => !!index,
                ) as TableIndex[]

                return table
            }),
        )
    }

    protected async loadTableRecords(
        tablePath: string,
        tableOrIndex: "table" | "index",
    ) {
        let database: string | undefined = undefined
        const [schema, tableName] = this.splitTablePath(tablePath)
        if (
            schema &&
            this.driver.getAttachedDatabasePathRelativeByHandle(schema)
        ) {
            database =
                this.driver.getAttachedDatabasePathRelativeByHandle(schema)
        }
        return this.query(
            `SELECT ${database ? `'${database}'` : null} as database, ${
                schema ? `'${schema}'` : null
            } as schema, * FROM ${
                schema ? `"${schema}".` : ""
            }${this.escapePath(
                `sqlite_master`,
            )} WHERE "type" = '${tableOrIndex}' AND "${
                tableOrIndex === "table" ? "name" : "tbl_name"
            }" IN ('${tableName}')`,
        )
    }

    protected async loadPragmaRecords(tablePath: string, pragma: string) {
        const [, tableName] = this.splitTablePath(tablePath)
        return this.query(`PRAGMA ${pragma}("${tableName}")`)
    }

    protected splitTablePath(tablePath: string): [string | undefined, string] {
        return (
            tablePath.indexOf(".") !== -1
                ? tablePath.split(".")
                : [undefined, tablePath]
        ) as [string | undefined, string]
    }

    protected escapePath(
        target: Table | View | string,
        disableEscape?: boolean,
    ): string {
        const tableName =
            InstanceChecker.isTable(target) || InstanceChecker.isView(target)
                ? target.name
                : target
        return tableName
            .replace(/^\.+|\.+$/g, "")
            .split(".")
            .map((i) => (disableEscape ? i : `"${i}"`))
            .join(".")
    }

    protected async loadViews(viewNames?: string[]): Promise<View[]> {
        const hasTable = await this.hasTable(this.getTypeormMetadataTableName())
        if (!hasTable) {
            return []
        }

        if (!viewNames) {
            viewNames = []
        }

        const viewNamesString = viewNames
            .map((name) => "'" + name + "'")
            .join(", ")
        let query = `SELECT "t".* FROM "${this.getTypeormMetadataTableName()}" "t" INNER JOIN "sqlite_master" s ON "s"."name" = "t"."name" AND "s"."type" = 'view' WHERE "t"."type" = '${
            MetadataTableType.VIEW
        }'`
        if (viewNamesString.length > 0)
            query += ` AND "t"."name" IN (${viewNamesString})`
        const dbViews = await this.query(query)
        return dbViews.map((dbView: any) => {
            const view = new View()
            view.name = dbView["name"]
            view.expression = dbView["value"]
            return view
        })
    }

    connect(): Promise<duckdb.Database> {
        return Promise.resolve(this.driver.databaseConnection)
    }

    /**
     * Releases used database connection.
     * We just clear loaded tables and sql in memory, because duckdb do not support multiple connections thus query runners. // TODO multi connection support?
     */
    release(): Promise<void> {
        this.loadedTables = []
        this.clearSqlMemory()
        return Promise.resolve()
    }

    async clearDatabase(database?: string): Promise<void> {
        let dbPath: string | undefined = undefined
        if (
            database &&
            this.driver.getAttachedDatabaseHandleByRelativePath(database)
        ) {
            dbPath =
                this.driver.getAttachedDatabaseHandleByRelativePath(database)
        }

        // await this.query(`PRAGMA foreign_keys = OFF`)

        const isAnotherTransactionActive = this.isTransactionActive
        if (!isAnotherTransactionActive) await this.startTransaction()
        try {
            const selectViewDropsQuery = dbPath
                ? `SELECT 'DROP VIEW "${dbPath}"."' || name || '";' as query FROM "${dbPath}"."sqlite_master" WHERE "type" = 'view'`
                : `SELECT 'DROP VIEW "' || name || '";' as query FROM "sqlite_master" WHERE "type" = 'view'`
            const dropViewQueries: ObjectLiteral[] = await this.query(
                selectViewDropsQuery,
            )
            await Promise.all(
                dropViewQueries.map((q) => this.query(q["query"])),
            )

            const selectTableDropsQuery = dbPath
                ? `SELECT 'DROP TABLE "${dbPath}"."' || name || '";' as query FROM "${dbPath}"."sqlite_master" WHERE "type" = 'table' AND "name" != 'sqlite_sequence'`
                : `SELECT 'DROP TABLE "' || name || '";' as query FROM "sqlite_master" WHERE "type" = 'table' AND "name" != 'sqlite_sequence'`
            const dropTableQueries: ObjectLiteral[] = await this.query(
                selectTableDropsQuery,
            )
            await Promise.all(
                dropTableQueries.map((q) => this.query(q["query"])),
            )

            if (!isAnotherTransactionActive) await this.commitTransaction()
        } catch (error) {
            try {
                // we throw original error even if rollback thrown an error
                if (!isAnotherTransactionActive)
                    await this.rollbackTransaction()
            } catch (rollbackError) {}
            throw error
        } finally {
            // await this.query(`PRAGMA foreign_keys = ON`)
        }
    }

    startTransaction(): Promise<void> {
        return this.query("BEGIN TRANSACTION")
    }
    commitTransaction(): Promise<void> {
        return this.query("COMMIT")
    }
    rollbackTransaction(): Promise<void> {
        return this.query("ROLLBACK")
    }
    stream(
        query: string,
        parameters?: any[],
        onEnd?: Function,
        onError?: Function,
    ): Promise<ReadStream> {
        throw new Error("Method not implemented.")
    }
    getDatabases(): Promise<string[]> {
        throw new Error("Method not implemented.")
    }
    getSchemas(database?: string): Promise<string[]> {
        throw new Error("Method not implemented.")
    }
    hasDatabase(database: string): Promise<boolean> {
        throw new Error("Method not implemented.")
    }
    getCurrentDatabase(): Promise<string> {
        throw new Error("Method not implemented.")
    }
    hasSchema(schema: string): Promise<boolean> {
        throw new Error("Method not implemented.")
    }
    getCurrentSchema(): Promise<string> {
        throw new Error("Method not implemented.")
    }

    public async hasTable(tableOrName: Table | string): Promise<boolean> {
        const tableName = InstanceChecker.isTable(tableOrName)
            ? tableOrName.name
            : tableOrName
        const sql = `SELECT * FROM information_schema.tables WHERE tables.table_name = ?`
        const result = await this.query(sql, [tableName])
        return result.length ? true : false
    }

    hasColumn(table: string | Table, columnName: string): Promise<boolean> {
        throw new Error("Method not implemented.")
    }
    createDatabase(database: string, ifNotExist?: boolean): Promise<void> {
        throw new Error("Method not implemented.")
    }
    dropDatabase(database: string, ifExist?: boolean): Promise<void> {
        throw new Error("Method not implemented.")
    }
    createSchema(schemaPath: string, ifNotExist?: boolean): Promise<void> {
        throw new Error("Method not implemented.")
    }
    dropSchema(
        schemaPath: string,
        ifExist?: boolean,
        isCascade?: boolean,
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }

    /**
     * Creates a new table.
     */
    public async createTable(
        table: Table,
        ifNotExist: boolean = false,
        createForeignKeys: boolean = true,
        createIndices: boolean = true,
    ): Promise<void> {
        const upQueries: Query[] = []
        const downQueries: Query[] = []

        if (ifNotExist) {
            const isTableExist = await this.hasTable(table)
            if (isTableExist) return Promise.resolve()
        }

        upQueries.push(this.createTableSql(table, createForeignKeys))
        downQueries.push(this.dropTableSql(table))

        if (createIndices) {
            table.indices.forEach((index) => {
                // new index may be passed without name. In this case we generate index name manually.
                if (!index.name)
                    index.name = this.connection.namingStrategy.indexName(
                        table,
                        index.columnNames,
                        index.where,
                    )
                upQueries.push(this.createIndexSql(table, index))
                downQueries.push(this.dropIndexSql(table, index))
            })
        }

        // if table have column with generated type, we must add the expression to the metadata table
        const generatedColumns = table.columns.filter(
            (column) => column.generatedType && column.asExpression,
        )

        for (const column of generatedColumns) {
            const insertQuery = this.insertTypeormMetadataSql({
                table: table.name,
                type: MetadataTableType.GENERATED_COLUMN,
                name: column.name,
                value: column.asExpression,
            })

            const deleteQuery = this.deleteTypeormMetadataSql({
                table: table.name,
                type: MetadataTableType.GENERATED_COLUMN,
                name: column.name,
            })

            upQueries.push(insertQuery)
            downQueries.push(deleteQuery)
        }

        await this.executeQueries(upQueries, downQueries)
    }

    /**
     * Builds create index sql.
     */
    protected createIndexSql(table: Table, index: TableIndex): Query {
        const columns = index.columnNames
            .map((columnName) => `"${columnName}"`)
            .join(", ")
        const [database, tableName] = this.splitTablePath(table.name)
        return new Query(
            `CREATE ${index.isUnique ? "UNIQUE " : ""}INDEX ${
                database ? `"${database}".` : ""
            }${this.escapePath(index.name!)} ON "${tableName}" (${columns}) ${
                index.where ? "WHERE " + index.where : ""
            }`,
        )
    }

    /**
     * Builds drop index sql.
     */
    protected dropIndexSql(
        table: Table | View,
        indexOrName: TableIndex | string,
    ): Query {
        let indexName = InstanceChecker.isTableIndex(indexOrName)
            ? indexOrName.name
            : indexOrName
        const concurrent = InstanceChecker.isTableIndex(indexOrName)
            ? indexOrName.isConcurrent
            : false
        const { schema } = this.driver.parseTableName(table)
        return schema
            ? new Query(
                  `DROP INDEX ${
                      concurrent ? "CONCURRENTLY" : ""
                  }"${schema}"."${indexName}"`,
              )
            : new Query(
                  `DROP INDEX ${
                      concurrent ? "CONCURRENTLY" : ""
                  }"${indexName}"`,
              )
    }

    /**
     * Builds create table sql.
     */
    protected createTableSql(table: Table, createForeignKeys?: boolean): Query {
        const columnDefinitions = table.columns
            .map((column) => this.buildCreateColumnSql(table, column))
            .join(", ")
        let sql = `CREATE TABLE ${this.escapePath(table)} (${columnDefinitions}`

        table.columns
            .filter((column) => column.isUnique)
            .forEach((column) => {
                const isUniqueExist = table.uniques.some(
                    (unique) =>
                        unique.columnNames.length === 1 &&
                        unique.columnNames[0] === column.name,
                )
                if (!isUniqueExist)
                    table.uniques.push(
                        new TableUnique({
                            name: this.connection.namingStrategy.uniqueConstraintName(
                                table,
                                [column.name],
                            ),
                            columnNames: [column.name],
                        }),
                    )
            })

        if (table.uniques.length > 0) {
            const uniquesSql = table.uniques
                .map((unique) => {
                    const uniqueName = unique.name
                        ? unique.name
                        : this.connection.namingStrategy.uniqueConstraintName(
                              table,
                              unique.columnNames,
                          )
                    const columnNames = unique.columnNames
                        .map((columnName) => `"${columnName}"`)
                        .join(", ")
                    let constraint = `CONSTRAINT "${uniqueName}" UNIQUE (${columnNames})`
                    if (unique.deferrable)
                        constraint += ` DEFERRABLE ${unique.deferrable}`
                    return constraint
                })
                .join(", ")

            sql += `, ${uniquesSql}`
        }

        if (table.checks.length > 0) {
            const checksSql = table.checks
                .map((check) => {
                    const checkName = check.name
                        ? check.name
                        : this.connection.namingStrategy.checkConstraintName(
                              table,
                              check.expression!,
                          )
                    return `CONSTRAINT "${checkName}" CHECK (${check.expression})`
                })
                .join(", ")

            sql += `, ${checksSql}`
        }

        if (table.exclusions.length > 0) {
            const exclusionsSql = table.exclusions
                .map((exclusion) => {
                    const exclusionName = exclusion.name
                        ? exclusion.name
                        : this.connection.namingStrategy.exclusionConstraintName(
                              table,
                              exclusion.expression!,
                          )
                    return `CONSTRAINT "${exclusionName}" EXCLUDE ${exclusion.expression}`
                })
                .join(", ")

            sql += `, ${exclusionsSql}`
        }

        if (table.foreignKeys.length > 0 && createForeignKeys) {
            const foreignKeysSql = table.foreignKeys
                .map((fk) => {
                    const columnNames = fk.columnNames
                        .map((columnName) => `"${columnName}"`)
                        .join(", ")
                    if (!fk.name)
                        fk.name = this.connection.namingStrategy.foreignKeyName(
                            table,
                            fk.columnNames,
                            this.getTablePath(fk),
                            fk.referencedColumnNames,
                        )

                    const referencedColumnNames = fk.referencedColumnNames
                        .map((columnName) => `"${columnName}"`)
                        .join(", ")

                    let constraint = `CONSTRAINT "${
                        fk.name
                    }" FOREIGN KEY (${columnNames}) REFERENCES ${this.escapePath(
                        this.getTablePath(fk),
                    )} (${referencedColumnNames})`
                    if (fk.onDelete) constraint += ` ON DELETE ${fk.onDelete}`
                    if (fk.onUpdate) constraint += ` ON UPDATE ${fk.onUpdate}`
                    if (fk.deferrable)
                        constraint += ` DEFERRABLE ${fk.deferrable}`

                    return constraint
                })
                .join(", ")

            sql += `, ${foreignKeysSql}`
        }

        const primaryColumns = table.columns.filter(
            (column) => column.isPrimary,
        )
        if (primaryColumns.length > 0) {
            const primaryKeyName = primaryColumns[0].primaryKeyConstraintName
                ? primaryColumns[0].primaryKeyConstraintName
                : this.connection.namingStrategy.primaryKeyName(
                      table,
                      primaryColumns.map((column) => column.name),
                  )

            const columnNames = primaryColumns
                .map((column) => `"${column.name}"`)
                .join(", ")
            sql += `, CONSTRAINT "${primaryKeyName}" PRIMARY KEY (${columnNames})`
        }

        sql += `)`
        return new Query(sql)
    }

    /**
     * Builds drop table sql.
     */
    protected dropTableSql(
        tableOrName: Table | string,
        ifExist?: boolean,
    ): Query {
        const tableName = InstanceChecker.isTable(tableOrName)
            ? tableOrName.name
            : tableOrName
        const query = ifExist
            ? `DROP TABLE IF EXISTS ${this.escapePath(tableName)}`
            : `DROP TABLE ${this.escapePath(tableName)}`
        return new Query(query)
    }

    protected createViewSql(view: View): Query {
        if (typeof view.expression === "string") {
            return new Query(`CREATE VIEW "${view.name}" AS ${view.expression}`)
        } else {
            return new Query(
                `CREATE VIEW "${view.name}" AS ${view
                    .expression(this.connection)
                    .getQuery()}`,
            )
        }
    }

    /**
     * Builds a query for create column.
     */
    protected buildCreateColumnSql(table: Table, column: TableColumn) {
        let c = '"' + column.name + '"'
        if (
            column.isGenerated === true &&
            column.generationStrategy !== "uuid"
        ) {
            if (column.generationStrategy === "identity") {
                throw new Error(
                    `Generation strategy "identity" is not supported by DuckDB. (table "${table.name}", column "${column.name}")`,
                )
            } else {
                // classic SERIAL primary column
                if (
                    column.type === "integer" ||
                    column.type === "int" ||
                    column.type === "int4"
                )
                    c += " SERIAL"
                if (column.type === "smallint" || column.type === "int2")
                    c += " SMALLSERIAL"
                if (column.type === "bigint" || column.type === "int8")
                    c += " BIGSERIAL"
            }
        }
        if (column.type === "enum" || column.type === "simple-enum") {
            c += " " + this.buildEnumName(table, column)
            if (column.isArray) c += " array"
        } else if (!column.isGenerated || column.type === "uuid") {
            c += " " + this.connection.driver.createFullType(column)
        }

        // Postgres only supports the stored generated column type
        if (column.generatedType === "STORED" && column.asExpression) {
            c += ` GENERATED ALWAYS AS (${column.asExpression}) STORED`
        }

        if (column.charset) c += ' CHARACTER SET "' + column.charset + '"'
        if (column.collation) c += ' COLLATE "' + column.collation + '"'
        if (column.isNullable !== true) c += " NOT NULL"
        if (column.default !== undefined && column.default !== null)
            c += " DEFAULT " + column.default
        if (
            column.isGenerated &&
            column.generationStrategy === "uuid" &&
            !column.default
        )
            c += ` DEFAULT ${this.driver.uuidGenerator}`

        return c
    }

    /**
     * Builds sequence name from given table and column.
     */
    protected buildSequenceName(
        table: Table,
        columnOrName: TableColumn | string,
    ): string {
        const { tableName } = this.driver.parseTableName(table)

        const columnName = InstanceChecker.isTableColumn(columnOrName)
            ? columnOrName.name
            : columnOrName

        let seqName = `${tableName}_${columnName}_seq`

        if (seqName.length > this.connection.driver.maxAliasLength!) {
            // note doesn't yet handle corner cases where .length differs from number of UTF-8 bytes
            seqName = `${tableName.substring(0, 29)}_${columnName.substring(
                0,
                Math.max(29, 63 - table.name.length - 5),
            )}_seq`
        }

        return seqName
    }

    protected buildSequencePath(
        table: Table,
        columnOrName: TableColumn | string,
    ): string {
        const { schema } = this.driver.parseTableName(table)

        return schema
            ? `${schema}.${this.buildSequenceName(table, columnOrName)}`
            : this.buildSequenceName(table, columnOrName)
    }

    generatedIncrementSequenceName(
        tableName: string,
        columnName: string,
    ): string {
        return `seq_${tableName}_${columnName}`
    }

    /**
     * Builds create ENUM type sql.
     */
    protected createEnumTypeSql(
        table: Table,
        column: TableColumn,
        enumName?: string,
    ): Query {
        if (!enumName) enumName = this.buildEnumName(table, column)
        const enumValues = column
            .enum!.map((value) => `'${value.replace("'", "''")}'`)
            .join(", ")
        return new Query(`CREATE TYPE ${enumName} AS ENUM(${enumValues})`)
    }

    /**
     * Builds ENUM type name from given table and column.
     */
    protected buildEnumName(
        table: Table,
        column: TableColumn,
        withSchema: boolean = true,
        disableEscape?: boolean,
        toOld?: boolean,
    ): string {
        const { schema, tableName } = this.driver.parseTableName(table)
        let enumName = column.enumName
            ? column.enumName
            : `${tableName}_${column.name.toLowerCase()}_enum`
        if (schema && withSchema) enumName = `${schema}.${enumName}`
        if (toOld) enumName = enumName + "_old"
        return enumName
            .split(".")
            .map((i) => {
                return disableEscape ? i : `"${i}"`
            })
            .join(".")
    }

    /**
     * Drops the table.
     */
    async dropTable(
        target: Table | string,
        ifExist?: boolean,
        dropForeignKeys: boolean = true,
        dropIndices: boolean = true,
    ): Promise<void> {
        // It needs because if table does not exist and dropForeignKeys or dropIndices is true, we don't need
        // to perform drop queries for foreign keys and indices.
        if (ifExist) {
            const isTableExist = await this.hasTable(target)
            if (!isTableExist) return Promise.resolve()
        }

        // if dropTable called with dropForeignKeys = true, we must create foreign keys in down query.
        const createForeignKeys: boolean = dropForeignKeys
        const tablePath = this.getTablePath(target)
        const table = await this.getCachedTable(tablePath)
        const upQueries: Query[] = []
        const downQueries: Query[] = []

        if (dropIndices) {
            table.indices.forEach((index) => {
                upQueries.push(this.dropIndexSql(table, index))
                downQueries.push(this.createIndexSql(table, index))
            })
        }

        if (dropForeignKeys)
            table.foreignKeys.forEach((foreignKey) =>
                upQueries.push(this.dropForeignKeySql(table, foreignKey)),
            )

        upQueries.push(this.dropTableSql(table))
        downQueries.push(this.createTableSql(table, createForeignKeys))

        // if table had columns with generated type, we must remove the expression from the metadata table
        const generatedColumns = table.columns.filter(
            (column) => column.generatedType && column.asExpression,
        )
        for (const column of generatedColumns) {
            const tableNameWithSchema = (
                await this.getTableNameWithSchema(table.name)
            ).split(".")
            const tableName = tableNameWithSchema[1]
            const schema = tableNameWithSchema[0]

            const deleteQuery = this.deleteTypeormMetadataSql({
                database: this.driver.options.database,
                schema,
                table: tableName,
                type: MetadataTableType.GENERATED_COLUMN,
                name: column.name,
            })

            const insertQuery = this.insertTypeormMetadataSql({
                database: this.driver.options.database,
                schema,
                table: tableName,
                type: MetadataTableType.GENERATED_COLUMN,
                name: column.name,
                value: column.asExpression,
            })

            upQueries.push(deleteQuery)
            downQueries.push(insertQuery)
        }

        await this.executeQueries(upQueries, downQueries)
    }

    /**
     * Creates a new view.
     */
    async createView(
        view: View,
        syncWithMetadata: boolean = false,
    ): Promise<void> {
        throw new Error("Method not implemented.")
        // should be implemented like postgres
    }

    /**
     * Drops the view.
     */
    async dropView(target: View | string): Promise<void> {
        throw new Error("Method not implemented.")
        // should be implemented like postgres
    }


    /**
     * Get the table name with table schema
     * Note: Without ' or "
     */
    protected async getTableNameWithSchema(target: Table | string) {
        const tableName = InstanceChecker.isTable(target) ? target.name : target
        if (tableName.indexOf(".") === -1) {
            const schemaResult = await this.query(`SELECT current_schema()`)
            const schema = schemaResult[0]["current_schema"]
            return `${schema}.${tableName}`
        } else {
            return `${tableName.split(".")[0]}.${tableName.split(".")[1]}`
        }
    }

    /**
     * Renames the given table.
     */
    async renameTable(
        oldTableOrName: Table | string,
        newTableName: string,
    ): Promise<void> {
        const upQueries: Query[] = []
        const downQueries: Query[] = []
        const oldTable = InstanceChecker.isTable(oldTableOrName)
            ? oldTableOrName
            : await this.getCachedTable(oldTableOrName)
        const newTable = oldTable.clone()

        const { schema: schemaName, tableName: oldTableName } =
            this.driver.parseTableName(oldTable)

        newTable.name = schemaName
            ? `${schemaName}.${newTableName}`
            : newTableName

        upQueries.push(
            new Query(
                `ALTER TABLE ${this.escapePath(
                    oldTable,
                )} RENAME TO "${newTableName}"`,
            ),
        )
        downQueries.push(
            new Query(
                `ALTER TABLE ${this.escapePath(
                    newTable,
                )} RENAME TO "${oldTableName}"`,
            ),
        )

        // rename column primary key constraint if it has default constraint name
        if (
            newTable.primaryColumns.length > 0 &&
            !newTable.primaryColumns[0].primaryKeyConstraintName
        ) {
            const columnNames = newTable.primaryColumns.map(
                (column) => column.name,
            )

            const oldPkName = this.connection.namingStrategy.primaryKeyName(
                oldTable,
                columnNames,
            )

            const newPkName = this.connection.namingStrategy.primaryKeyName(
                newTable,
                columnNames,
            )

            upQueries.push(
                new Query(
                    `ALTER TABLE ${this.escapePath(
                        newTable,
                    )} RENAME CONSTRAINT "${oldPkName}" TO "${newPkName}"`,
                ),
            )
            downQueries.push(
                new Query(
                    `ALTER TABLE ${this.escapePath(
                        newTable,
                    )} RENAME CONSTRAINT "${newPkName}" TO "${oldPkName}"`,
                ),
            )
        }

        // rename sequences
        newTable.columns.map((col) => {
            if (col.isGenerated && col.generationStrategy === "increment") {
                const sequencePath = this.buildSequencePath(oldTable, col.name)
                const sequenceName = this.buildSequenceName(oldTable, col.name)

                const newSequencePath = this.buildSequencePath(
                    newTable,
                    col.name,
                )
                const newSequenceName = this.buildSequenceName(
                    newTable,
                    col.name,
                )

                const up = `ALTER SEQUENCE ${this.escapePath(
                    sequencePath,
                )} RENAME TO "${newSequenceName}"`
                const down = `ALTER SEQUENCE ${this.escapePath(
                    newSequencePath,
                )} RENAME TO "${sequenceName}"`

                upQueries.push(new Query(up))
                downQueries.push(new Query(down))
            }
        })

        // rename unique constraints
        newTable.uniques.forEach((unique) => {
            const oldUniqueName =
                this.connection.namingStrategy.uniqueConstraintName(
                    oldTable,
                    unique.columnNames,
                )

            // Skip renaming if Unique has user defined constraint name
            if (unique.name !== oldUniqueName) return

            // build new constraint name
            const newUniqueName =
                this.connection.namingStrategy.uniqueConstraintName(
                    newTable,
                    unique.columnNames,
                )

            // build queries
            upQueries.push(
                new Query(
                    `ALTER TABLE ${this.escapePath(
                        newTable,
                    )} RENAME CONSTRAINT "${
                        unique.name
                    }" TO "${newUniqueName}"`,
                ),
            )
            downQueries.push(
                new Query(
                    `ALTER TABLE ${this.escapePath(
                        newTable,
                    )} RENAME CONSTRAINT "${newUniqueName}" TO "${
                        unique.name
                    }"`,
                ),
            )

            // replace constraint name
            unique.name = newUniqueName
        })

        // rename index constraints
        newTable.indices.forEach((index) => {
            const oldIndexName = this.connection.namingStrategy.indexName(
                oldTable,
                index.columnNames,
                index.where,
            )

            // Skip renaming if Index has user defined constraint name
            if (index.name !== oldIndexName) return

            // build new constraint name
            const { schema } = this.driver.parseTableName(newTable)
            const newIndexName = this.connection.namingStrategy.indexName(
                newTable,
                index.columnNames,
                index.where,
            )

            // build queries
            const up = schema
                ? `ALTER INDEX "${schema}"."${index.name}" RENAME TO "${newIndexName}"`
                : `ALTER INDEX "${index.name}" RENAME TO "${newIndexName}"`
            const down = schema
                ? `ALTER INDEX "${schema}"."${newIndexName}" RENAME TO "${index.name}"`
                : `ALTER INDEX "${newIndexName}" RENAME TO "${index.name}"`
            upQueries.push(new Query(up))
            downQueries.push(new Query(down))

            // replace constraint name
            index.name = newIndexName
        })

        // rename foreign key constraints
        newTable.foreignKeys.forEach((foreignKey) => {
            const oldForeignKeyName =
                this.connection.namingStrategy.foreignKeyName(
                    oldTable,
                    foreignKey.columnNames,
                    this.getTablePath(foreignKey),
                    foreignKey.referencedColumnNames,
                )

            // Skip renaming if foreign key has user defined constraint name
            if (foreignKey.name !== oldForeignKeyName) return

            // build new constraint name
            const newForeignKeyName =
                this.connection.namingStrategy.foreignKeyName(
                    newTable,
                    foreignKey.columnNames,
                    this.getTablePath(foreignKey),
                    foreignKey.referencedColumnNames,
                )

            // build queries
            upQueries.push(
                new Query(
                    `ALTER TABLE ${this.escapePath(
                        newTable,
                    )} RENAME CONSTRAINT "${
                        foreignKey.name
                    }" TO "${newForeignKeyName}"`,
                ),
            )
            downQueries.push(
                new Query(
                    `ALTER TABLE ${this.escapePath(
                        newTable,
                    )} RENAME CONSTRAINT "${newForeignKeyName}" TO "${
                        foreignKey.name
                    }"`,
                ),
            )

            // replace constraint name
            foreignKey.name = newForeignKeyName
        })

        // rename ENUM types
        const enumColumns = newTable.columns.filter(
            (column) => column.type === "enum" || column.type === "simple-enum",
        )
        for (let column of enumColumns) {
            // skip renaming for user-defined enum name
            if (column.enumName) continue

            const oldEnumType = await this.getUserDefinedTypeName(
                oldTable,
                column,
            )
            upQueries.push(
                new Query(
                    `ALTER TYPE "${oldEnumType.schema}"."${
                        oldEnumType.name
                    }" RENAME TO ${this.buildEnumName(
                        newTable,
                        column,
                        false,
                    )}`,
                ),
            )
            downQueries.push(
                new Query(
                    `ALTER TYPE ${this.buildEnumName(
                        newTable,
                        column,
                    )} RENAME TO "${oldEnumType.name}"`,
                ),
            )
        }
        await this.executeQueries(upQueries, downQueries)
    }

        /**
     * Builds drop foreign key sql.
     */
        protected dropForeignKeySql(
            table: Table,
            foreignKeyOrName: TableForeignKey | string,
        ): Query {
            const foreignKeyName = InstanceChecker.isTableForeignKey(
                foreignKeyOrName,
            )
                ? foreignKeyOrName.name
                : foreignKeyOrName
            return new Query(
                `ALTER TABLE ${this.escapePath(
                    table,
                )} DROP CONSTRAINT "${foreignKeyName}"`,
            )
        }

    changeTableComment(
        tableOrName: string | Table,
        comment?: string,
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }
    addColumn(table: string | Table, column: TableColumn): Promise<void> {
        throw new Error("Method not implemented.")
    }
    addColumns(table: string | Table, columns: TableColumn[]): Promise<void> {
        throw new Error("Method not implemented.")
    }
    renameColumn(
        table: string | Table,
        oldColumnOrName: string | TableColumn,
        newColumnOrName: string | TableColumn,
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }
    changeColumn(
        table: string | Table,
        oldColumn: string | TableColumn,
        newColumn: TableColumn,
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }
    changeColumns(
        table: string | Table,
        changedColumns: { oldColumn: TableColumn; newColumn: TableColumn }[],
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }
    dropColumn(
        table: string | Table,
        column: string | TableColumn,
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }
    dropColumns(
        table: string | Table,
        columns: string[] | TableColumn[],
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }
    createPrimaryKey(
        table: string | Table,
        columnNames: string[],
        constraintName?: string,
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }
    updatePrimaryKeys(
        table: string | Table,
        columns: TableColumn[],
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }
    dropPrimaryKey(
        table: string | Table,
        constraintName?: string,
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }
    createUniqueConstraint(
        table: string | Table,
        uniqueConstraint: TableUnique,
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }
    createUniqueConstraints(
        table: string | Table,
        uniqueConstraints: TableUnique[],
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }
    dropUniqueConstraint(
        table: string | Table,
        uniqueOrName: string | TableUnique,
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }
    dropUniqueConstraints(
        table: string | Table,
        uniqueConstraints: TableUnique[],
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }
    createCheckConstraint(
        table: string | Table,
        checkConstraint: TableCheck,
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }
    createCheckConstraints(
        table: string | Table,
        checkConstraints: TableCheck[],
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }
    dropCheckConstraint(
        table: string | Table,
        checkOrName: string | TableCheck,
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }
    dropCheckConstraints(
        table: string | Table,
        checkConstraints: TableCheck[],
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }
    createExclusionConstraint(
        table: string | Table,
        exclusionConstraint: TableExclusion,
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }
    createExclusionConstraints(
        table: string | Table,
        exclusionConstraints: TableExclusion[],
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }
    dropExclusionConstraint(
        table: string | Table,
        exclusionOrName: string | TableExclusion,
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }
    dropExclusionConstraints(
        table: string | Table,
        exclusionConstraints: TableExclusion[],
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }
    createForeignKey(
        table: string | Table,
        foreignKey: TableForeignKey,
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }
    createForeignKeys(
        table: string | Table,
        foreignKeys: TableForeignKey[],
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }
    dropForeignKey(
        table: string | Table,
        foreignKeyOrName: string | TableForeignKey,
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }
    dropForeignKeys(
        table: string | Table,
        foreignKeys: TableForeignKey[],
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }
    createIndex(table: string | Table, index: TableIndex): Promise<void> {
        throw new Error("Method not implemented.")
    }
    createIndices(table: string | Table, indices: TableIndex[]): Promise<void> {
        throw new Error("Method not implemented.")
    }
    dropIndex(
        table: string | Table,
        index: string | TableIndex,
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }
    dropIndices(table: string | Table, indices: TableIndex[]): Promise<void> {
        throw new Error("Method not implemented.")
    }
    clearTable(tableName: string): Promise<void> {
        throw new Error("Method not implemented.")
    }

    protected async getUserDefinedTypeName(table: Table, column: TableColumn) {
        let { schema, tableName: name } = this.driver.parseTableName(table)

        if (!schema) {
            schema = await this.getCurrentSchema()
        }

        const result = await this.query(
            `SELECT "udt_schema", "udt_name" ` +
                `FROM "information_schema"."columns" WHERE "table_schema" = '${schema}' AND "table_name" = '${name}' AND "column_name"='${column.name}'`,
        )

        // docs: https://www.postgresql.org/docs/current/xtypes.html
        // When you define a new base type, PostgreSQL automatically provides support for arrays of that type.
        // The array type typically has the same name as the base type with the underscore character (_) prepended.
        // ----
        // so, we must remove this underscore character from enum type name
        let udtName = result[0]["udt_name"]
        if (udtName.indexOf("_") === 0) {
            udtName = udtName.substr(1, udtName.length)
        }
        return {
            schema: result[0]["udt_schema"],
            name: udtName,
        }
    }

    /**
     * Called before migrations are run.
     */
    async beforeMigration(): Promise<void> {
        // await this.query(`PRAGMA foreign_keys = OFF`)
    }

    /**
     * Called after migrations are run.
     */
    async afterMigration(): Promise<void> {
        // await this.query(`PRAGMA foreign_keys = ON`)
    }

    /**
     * Executes a given SQL query.
     */
    query(
        query: string,
        parameters?: any[],
        useStructuredResult = false,
    ): Promise<any> {
        if (this.isReleased) throw new QueryRunnerAlreadyReleasedError()

        console.log("query", query, parameters)
        const connection = this.driver.connection
        const maxQueryExecutionTime = this.driver.options.maxQueryExecutionTime
        const broadcasterResult = new BroadcasterResult()
        const broadcaster = this.broadcaster

        broadcaster.broadcastBeforeQueryEvent(
            broadcasterResult,
            query,
            parameters,
        )

        if (!connection.isInitialized) {
            throw new ConnectionIsNotSetError("duckdb")
        }

        return new Promise(async (ok, fail) => {
            try {
                const databaseConnection = await this.connect()
                this.driver.connection.logger.logQuery(query, parameters, this)
                const queryStartTime = +new Date()
                const isInsertQuery = query.startsWith("INSERT ")
                const isDeleteQuery = query.startsWith("DELETE ")
                const isUpdateQuery = query.startsWith("UPDATE ")

                const execute = async () => {
                    if (isInsertQuery || isDeleteQuery || isUpdateQuery) {
                        await databaseConnection.run(
                            query,
                            ...(parameters ?? []),
                            handler,
                        )
                    } else {
                        await databaseConnection.all(
                            query,
                            ...(parameters ?? []),
                            handler,
                        )
                    }
                }

                const self = this
                const handler = function (this: any, err: any, rows: any) {
                    // log slow queries if maxQueryExecution time is set
                    const queryEndTime = +new Date()
                    const queryExecutionTime = queryEndTime - queryStartTime
                    if (
                        maxQueryExecutionTime &&
                        queryExecutionTime > maxQueryExecutionTime
                    )
                        connection.logger.logQuerySlow(
                            queryExecutionTime,
                            query,
                            parameters,
                            self,
                        )

                    if (err) {
                        connection.logger.logQueryError(
                            err,
                            query,
                            parameters,
                            self,
                        )
                        broadcaster.broadcastAfterQueryEvent(
                            broadcasterResult,
                            query,
                            parameters,
                            false,
                            undefined,
                            undefined,
                            err,
                        )

                        return fail(
                            new QueryFailedError(query, parameters, err),
                        )
                    } else {
                        const result = new QueryResult()

                        if (isInsertQuery) {
                            result.raw = this["lastID"]
                        } else {
                            result.raw = rows
                        }

                        broadcaster.broadcastAfterQueryEvent(
                            broadcasterResult,
                            query,
                            parameters,
                            true,
                            queryExecutionTime,
                            result.raw,
                            undefined,
                        )

                        if (Array.isArray(rows)) {
                            result.records = rows
                        }

                        result.affected = this["changes"]

                        if (useStructuredResult) {
                            ok(result)
                        } else {
                            ok(result.raw)
                        }
                    }
                }

                await execute()
            } catch (err) {
                fail(err)
            } finally {
                await broadcasterResult.wait()
            }
        })
    }
}

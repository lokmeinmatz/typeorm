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
import { IsolationLevel } from "../types/IsolationLevel"
import { ObjectLiteral } from "../../common/ObjectLiteral"
import type * as duckdb from "duckdb"
import { InstanceChecker } from "../../util/InstanceChecker"
import { MetadataTableType } from "../types/MetadataTableType"
import { OrmUtils } from "../../util/OrmUtils"
import { TableIndexOptions } from "../../schema-builder/options/TableIndexOptions"

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

    protected databaseConnection: duckdb.Database

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
                        tableColumn.comment = "" // SQLite does not support column comments
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

    async connect(): Promise<duckdb.Database> {
        return this.databaseConnection
    }

    /**
     * Releases used database connection.
     * We just clear loaded tables and sql in memory, because sqlite do not support multiple connections thus query runners.
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

        await this.query(`PRAGMA foreign_keys = OFF`)

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
            await this.query(`PRAGMA foreign_keys = ON`)
        }
    }
    
    startTransaction(isolationLevel?: IsolationLevel): Promise<void> {
        throw new Error("Method not implemented.")
    }
    commitTransaction(): Promise<void> {
        throw new Error("Method not implemented.")
    }
    rollbackTransaction(): Promise<void> {
        throw new Error("Method not implemented.")
    }
    stream(query: string, parameters?: any[], onEnd?: Function, onError?: Function): Promise<ReadStream> {
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
    hasTable(table: string | Table): Promise<boolean> {
        throw new Error("Method not implemented.")
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
    dropSchema(schemaPath: string, ifExist?: boolean, isCascade?: boolean): Promise<void> {
        throw new Error("Method not implemented.")
    }
    createTable(table: Table, ifNotExist?: boolean, createForeignKeys?: boolean, createIndices?: boolean): Promise<void> {
        throw new Error("Method not implemented.")
    }
    dropTable(table: string | Table, ifExist?: boolean, dropForeignKeys?: boolean, dropIndices?: boolean): Promise<void> {
        throw new Error("Method not implemented.")
    }
    createView(view: View, syncWithMetadata?: boolean, oldView?: View): Promise<void> {
        throw new Error("Method not implemented.")
    }
    dropView(view: string | View): Promise<void> {
        throw new Error("Method not implemented.")
    }
    renameTable(oldTableOrName: string | Table, newTableName: string): Promise<void> {
        throw new Error("Method not implemented.")
    }
    changeTableComment(tableOrName: string | Table, comment?: string): Promise<void> {
        throw new Error("Method not implemented.")
    }
    addColumn(table: string | Table, column: TableColumn): Promise<void> {
        throw new Error("Method not implemented.")
    }
    addColumns(table: string | Table, columns: TableColumn[]): Promise<void> {
        throw new Error("Method not implemented.")
    }
    renameColumn(table: string | Table, oldColumnOrName: string | TableColumn, newColumnOrName: string | TableColumn): Promise<void> {
        throw new Error("Method not implemented.")
    }
    changeColumn(table: string | Table, oldColumn: string | TableColumn, newColumn: TableColumn): Promise<void> {
        throw new Error("Method not implemented.")
    }
    changeColumns(table: string | Table, changedColumns: { oldColumn: TableColumn; newColumn: TableColumn }[]): Promise<void> {
        throw new Error("Method not implemented.")
    }
    dropColumn(table: string | Table, column: string | TableColumn): Promise<void> {
        throw new Error("Method not implemented.")
    }
    dropColumns(table: string | Table, columns: string[] | TableColumn[]): Promise<void> {
        throw new Error("Method not implemented.")
    }
    createPrimaryKey(table: string | Table, columnNames: string[], constraintName?: string): Promise<void> {
        throw new Error("Method not implemented.")
    }
    updatePrimaryKeys(table: string | Table, columns: TableColumn[]): Promise<void> {
        throw new Error("Method not implemented.")
    }
    dropPrimaryKey(table: string | Table, constraintName?: string): Promise<void> {
        throw new Error("Method not implemented.")
    }
    createUniqueConstraint(table: string | Table, uniqueConstraint: TableUnique): Promise<void> {
        throw new Error("Method not implemented.")
    }
    createUniqueConstraints(table: string | Table, uniqueConstraints: TableUnique[]): Promise<void> {
        throw new Error("Method not implemented.")
    }
    dropUniqueConstraint(table: string | Table, uniqueOrName: string | TableUnique): Promise<void> {
        throw new Error("Method not implemented.")
    }
    dropUniqueConstraints(table: string | Table, uniqueConstraints: TableUnique[]): Promise<void> {
        throw new Error("Method not implemented.")
    }
    createCheckConstraint(table: string | Table, checkConstraint: TableCheck): Promise<void> {
        throw new Error("Method not implemented.")
    }
    createCheckConstraints(table: string | Table, checkConstraints: TableCheck[]): Promise<void> {
        throw new Error("Method not implemented.")
    }
    dropCheckConstraint(table: string | Table, checkOrName: string | TableCheck): Promise<void> {
        throw new Error("Method not implemented.")
    }
    dropCheckConstraints(table: string | Table, checkConstraints: TableCheck[]): Promise<void> {
        throw new Error("Method not implemented.")
    }
    createExclusionConstraint(table: string | Table, exclusionConstraint: TableExclusion): Promise<void> {
        throw new Error("Method not implemented.")
    }
    createExclusionConstraints(table: string | Table, exclusionConstraints: TableExclusion[]): Promise<void> {
        throw new Error("Method not implemented.")
    }
    dropExclusionConstraint(table: string | Table, exclusionOrName: string | TableExclusion): Promise<void> {
        throw new Error("Method not implemented.")
    }
    dropExclusionConstraints(table: string | Table, exclusionConstraints: TableExclusion[]): Promise<void> {
        throw new Error("Method not implemented.")
    }
    createForeignKey(table: string | Table, foreignKey: TableForeignKey): Promise<void> {
        throw new Error("Method not implemented.")
    }
    createForeignKeys(table: string | Table, foreignKeys: TableForeignKey[]): Promise<void> {
        throw new Error("Method not implemented.")
    }
    dropForeignKey(table: string | Table, foreignKeyOrName: string | TableForeignKey): Promise<void> {
        throw new Error("Method not implemented.")
    }
    dropForeignKeys(table: string | Table, foreignKeys: TableForeignKey[]): Promise<void> {
        throw new Error("Method not implemented.")
    }
    createIndex(table: string | Table, index: TableIndex): Promise<void> {
        throw new Error("Method not implemented.")
    }
    createIndices(table: string | Table, indices: TableIndex[]): Promise<void> {
        throw new Error("Method not implemented.")
    }
    dropIndex(table: string | Table, index: string | TableIndex): Promise<void> {
        throw new Error("Method not implemented.")
    }
    dropIndices(table: string | Table, indices: TableIndex[]): Promise<void> {
        throw new Error("Method not implemented.")
    }
    clearTable(tableName: string): Promise<void> {
        throw new Error("Method not implemented.")
    }

    /**
     * Called before migrations are run.
     */
    async beforeMigration(): Promise<void> {
        await this.query(`PRAGMA foreign_keys = OFF`)
    }

    /**
     * Called after migrations are run.
     */
    async afterMigration(): Promise<void> {
        await this.query(`PRAGMA foreign_keys = ON`)
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
            throw new ConnectionIsNotSetError("sqlite")
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
                        await databaseConnection.run(query, parameters, handler)
                    } else {
                        await databaseConnection.all(query, parameters, handler)
                    }
                }

                const self = this;
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

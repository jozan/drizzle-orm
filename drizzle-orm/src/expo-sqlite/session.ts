import { entityKind } from "~/entity";
import type { Logger } from "~/logger";
import { NoopLogger } from "~/logger";
import {
  type RelationalSchemaConfig,
  type TablesRelationalConfig,
} from "~/relations";
import { type Query, sql } from "~/sql";
import { SQLiteTransaction } from "~/sqlite-core";
import type { SQLiteAsyncDialect } from "~/sqlite-core/dialect";
import type { SelectedFieldsOrdered } from "~/sqlite-core/query-builders/select.types";
import {
  type PreparedQueryConfig as PreparedQueryConfigBase,
  type SQLiteTransactionConfig,
} from "~/sqlite-core/session";
import {
  PreparedQuery as PreparedQueryBase,
  SQLiteSession,
} from "~/sqlite-core/session";
import { mapResultRow } from "~/utils";

import type * as ExpoSQLite from "expo-sqlite";

export interface SQLiteExpoSessionOptions {
  logger?: Logger;
}

type PreparedQueryConfig = Omit<PreparedQueryConfigBase, "statement" | "run">;

export class SQLiteExpoSession<
  TFullSchema extends Record<string, unknown>,
  TSchema extends TablesRelationalConfig
> extends SQLiteSession<"async", ExpoSQLite.ResultSet, TFullSchema, TSchema> {
  static readonly [entityKind]: string = "SQLiteExpoSession";

  private logger: Logger;

  constructor(
    private client: ExpoSQLite.SQLiteDatabase,
    dialect: SQLiteAsyncDialect,
    private schema: RelationalSchemaConfig<TSchema> | undefined,
    private options: SQLiteExpoSessionOptions = {}
  ) {
    super(dialect);
    this.logger = options.logger ?? new NoopLogger();
  }

  prepareQuery(
    query: Query,
    fields?: SelectedFieldsOrdered,
    customResultMapper?: (rows: unknown[][]) => unknown
  ): PreparedQuery {
    // TODO: maybe this is not needed at all since expo-sqlite has native prepared statements
    //       that cannot be created on the fly as in other SQLite bindings
    return new PreparedQuery(
      this.client,
      query.sql,
      query.params,
      this.logger,
      fields,
      customResultMapper
    );
  }

  override async transaction<T>(
    transaction: (tx: ExpoTransaction<TFullSchema, TSchema>) => T | Promise<T>,
    config?: SQLiteTransactionConfig
  ): Promise<T> {
    const tx = new ExpoTransaction("async", this.dialect, this, this.schema);
    await this.run(
      sql.raw(`begin${config?.behavior ? " " + config.behavior : ""}`)
    );
    try {
      const result = await transaction(tx);
      await this.run(sql`commit`);
      return result;
    } catch (err) {
      await this.run(sql`rollback`);
      throw err;
    }
  }
}

export class ExpoTransaction<
  TFullSchema extends Record<string, unknown>,
  TSchema extends TablesRelationalConfig
> extends SQLiteTransaction<
  "async",
  ExpoSQLite.ResultSet,
  TFullSchema,
  TSchema
> {
  static readonly [entityKind]: string = "ExpoTransaction";

  override async transaction<T>(
    transaction: (tx: ExpoTransaction<TFullSchema, TSchema>) => Promise<T>
  ): Promise<T> {
    const savepointName = `sp${this.nestedIndex}`;
    const tx = new ExpoTransaction(
      "async",
      this.dialect,
      this.session,
      this.schema,
      this.nestedIndex + 1
    );
    await this.session.run(sql.raw(`savepoint ${savepointName}`));
    try {
      const result = await transaction(tx);
      await this.session.run(sql.raw(`release savepoint ${savepointName}`));
      return result;
    } catch (err) {
      await this.session.run(sql.raw(`rollback to savepoint ${savepointName}`));
      throw err;
    }
  }
}

export class PreparedQuery<
  T extends PreparedQueryConfig = PreparedQueryConfig
> extends PreparedQueryBase<{
  type: "async";
  run: ExpoSQLite.ResultSet;
  all: T["all"];
  get: T["get"];
  values: T["values"];
}> {
  static readonly [entityKind]: string = "ExpoPreparedQuery";

  constructor(
    private client: ExpoSQLite.SQLiteDatabase,
    private queryString: string,
    private params: unknown[],
    private logger: Logger,
    private fields: SelectedFieldsOrdered | undefined,
    private customResultMapper?: (rows: unknown[][]) => unknown
  ) {
    super();
  }

  // TODO: implement using expo-sqlite
  async run(
    placeholderValues?: Record<string, unknown>
  ): Promise<ExpoSQLite.ResultSet> {
    // TODO: placeholder values
    this.logger.logQuery(this.queryString, this.params);
    // TODO: expose readonly flag
    const readOnly = false;
    const result = await this.client.execAsync(
      [{ sql: this.queryString, args: this.params }],
      readOnly
    );
    // TODO: handle case where result is empty
    return result[0] as ExpoSQLite.ResultSet;
  }

  // TODO: implement using expo-sqlite
  async all(placeholderValues?: Record<string, unknown>): Promise<T["all"]> {
    const {
      fields,
      joinsNotNullableMap,
      queryString,
      params,
      logger,
      client,
      customResultMapper,
    } = this;
    if (!fields && !customResultMapper) {
      // TODO: placeholder values
      logger.logQuery(queryString, params);
      return client
        .execAsync([{ sql: queryString, args: params }], false)
        .then((result) => {
          const { rows } = result[0] as ExpoSQLite.ResultSet;
          return rows;
        });
    }

    const rows = await this.values(placeholderValues);

    if (customResultMapper) {
      return customResultMapper(rows) as T["all"];
    }

    return rows.map((row) => mapResultRow(fields!, row, joinsNotNullableMap));
  }

  // TODO: implement using expo-sqlite
  async get(placeholderValues?: Record<string, unknown>): Promise<T["get"]> {
    const {
      fields,
      joinsNotNullableMap,
      queryString,
      params,
      logger,
      client,
      customResultMapper,
    } = this;
    if (!fields && !customResultMapper) {
      // TODO: placeholder values
      logger.logQuery(queryString, params);
      return client
        .execAsync([{ sql: queryString, args: params }], false)
        .then((result) => {
          const { rows } = result[0] as ExpoSQLite.ResultSet;
          return rows[0];
        });
    }

    const rows = await this.values(placeholderValues);

    if (!rows[0]) {
      return undefined;
    }

    if (customResultMapper) {
      return customResultMapper(rows) as T["all"];
    }

    return mapResultRow(fields!, rows[0], joinsNotNullableMap);
  }

  // TODO: implement using expo-sqlite
  values<T extends any[] = unknown[]>(
    placeholderValues?: Record<string, unknown>
  ): Promise<T[]> {
    // TODO: placeholder values
    this.logger.logQuery(this.queryString, this.params);
    return this.client
      .execAsync([{ sql: this.queryString, args: this.params }], false)
      .then((result) => {
        return result[0] as unknown as T;
      });
  }
}

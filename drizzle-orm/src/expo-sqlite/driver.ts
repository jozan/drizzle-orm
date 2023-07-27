import { DefaultLogger } from "~/logger";
import {
  createTableRelationsHelpers,
  extractTablesRelationalConfig,
  type RelationalSchemaConfig,
  type TablesRelationalConfig,
} from "~/relations";
import { BaseSQLiteDatabase } from "~/sqlite-core/db";
import { SQLiteAsyncDialect } from "~/sqlite-core/dialect";
import { type DrizzleConfig } from "~/utils";
// TODO: use ExpoSQLiteSession
import { SQLiteD1Session } from "./session";

// TODO: this is pending improvements on the expo-sqlite bindings
//       it's missing prepared statements all together so there is
//       no much point having expo support as it would be slow by default
//       and no much can be done. however, expo team has expressed their
//       interest in improving the bindings so we'll wait for that.

import type * as ExpoSQLite from "expo-sqlite";

export type DrizzleExpoSQLiteDatabase<
  TSchema extends Record<string, unknown> = Record<string, never>
> = BaseSQLiteDatabase<"async", ExpoSQLite.ResultSet, TSchema>;

export function drizzle<
  TSchema extends Record<string, unknown> = Record<string, never>
>(
  client: ExpoSQLite.SQLiteDatabase,
  config: DrizzleConfig<TSchema> = {}
): DrizzleExpoSQLiteDatabase<TSchema> {
  const dialect = new SQLiteAsyncDialect();
  let logger;
  if (config.logger === true) {
    logger = new DefaultLogger();
  } else if (config.logger !== false) {
    logger = config.logger;
  }

  let schema: RelationalSchemaConfig<TablesRelationalConfig> | undefined;
  if (config.schema) {
    const tablesConfig = extractTablesRelationalConfig(
      config.schema,
      createTableRelationsHelpers
    );
    schema = {
      fullSchema: config.schema,
      schema: tablesConfig.tables,
      tableNamesMap: tablesConfig.tableNamesMap,
    };
  }

  const session = new SQLiteD1Session(client, dialect, schema, { logger });
  return new BaseSQLiteDatabase(
    "async",
    dialect,
    session,
    schema
  ) as DrizzleExpoSQLiteDatabase<TSchema>;
}

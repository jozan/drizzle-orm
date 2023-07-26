import type { MigrationConfig } from '~/migrator';
import { readMigrationFiles } from '~/migrator';
// TODO: change driver type to ExpoSQLiteDriver
import type { DrizzleD1Database } from './driver';

export async function migrate<TSchema extends Record<string, unknown>>(
	db: DrizzleD1Database<TSchema>,
	config: string | MigrationConfig,
) {
    // TODO: read migrations from a single file
	const migrations = readMigrationFiles(config);
	await db.dialect.migrate(migrations, db.session);
}

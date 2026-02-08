// filepath: checkpoint-sdb/src/migrations.ts
import { createTable, dropTable, addField, dropField } from 'surrealdb-lib'; // Hypothetical library for SurrealDB operations

export async function setupDatabase() {
  await createTable('checkpoints');
  await addField('checkpoints', 'id', 'string');
  await addField('checkpoints', 'data', 'json');
  await addField('checkpoints', 'createdAt', 'datetime');
}

export async function updateDatabase() {
  await addField('checkpoints', 'updatedAt', 'datetime');
}

export async function teardownDatabase() {
  await dropField('checkpoints', 'updatedAt');
  await dropTable('checkpoints');
}
// filepath: checkpoint-sdb/src/surrealdb.ts
import { createClient } from 'surrealdb.js';

const SURREALDB_URL = process.env.SURREALDB_URL || 'http://localhost:8000';
const SURREALDB_NAMESPACE = process.env.SURREALDB_NAMESPACE || 'test';
const SURREALDB_DATABASE = process.env.SURREALDB_DATABASE || 'test';

export const connectToSurrealDB = async () => {
  const client = createClient({
    url: SURREALDB_URL,
    namespace: SURREALDB_NAMESPACE,
    database: SURREALDB_DATABASE,
  });

  try {
    await client.connect();
    console.log('Connected to SurrealDB');
  } catch (error) {
    console.error('Error connecting to SurrealDB:', error);
    throw error;
  }

  return client;
};
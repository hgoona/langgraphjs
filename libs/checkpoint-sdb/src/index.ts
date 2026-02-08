// filepath: checkpoint-sdb/src/index.ts
import { connectToSurrealDB } from './surrealdb';

export class SurrealDBCheckpointer {
  constructor() {
    this.client = connectToSurrealDB();
  }

  async saveData(data) {
    try {
      const result = await this.client.create('checkpoint', data);
      return result;
    } catch (error) {
      console.error('Error saving data to SurrealDB:', error);
      throw error;
    }
  }

  async retrieveData(id) {
    try {
      const result = await this.client.select('checkpoint', id);
      return result;
    } catch (error) {
      console.error('Error retrieving data from SurrealDB:', error);
      throw error;
    }
  }
}
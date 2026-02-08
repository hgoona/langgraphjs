import { connectToSurrealDB } from '../surrealdb';
import { SurrealDBCheckpointer } from '../index';

describe('SurrealDBCheckpointer Integration Tests', () => {
  let checkpointer;

  beforeAll(async () => {
    const dbClient = await connectToSurrealDB();
    checkpointer = new SurrealDBCheckpointer(dbClient);
  });

  afterAll(async () => {
    await checkpointer.cleanup();
  });

  test('should save data to SurrealDB', async () => {
    const data = { key: 'testKey', value: 'testValue' };
    await checkpointer.save(data);
    
    const retrievedData = await checkpointer.retrieve(data.key);
    expect(retrievedData).toEqual(data);
  });

  test('should handle non-existent keys gracefully', async () => {
    const retrievedData = await checkpointer.retrieve('nonExistentKey');
    expect(retrievedData).toBeNull();
  });

  test('should update existing data in SurrealDB', async () => {
    const data = { key: 'updateKey', value: 'initialValue' };
    await checkpointer.save(data);
    
    const updatedData = { key: 'updateKey', value: 'updatedValue' };
    await checkpointer.save(updatedData);
    
    const retrievedData = await checkpointer.retrieve(updatedData.key);
    expect(retrievedData).toEqual(updatedData);
  });

  test('should delete data from SurrealDB', async () => {
    const data = { key: 'deleteKey', value: 'valueToDelete' };
    await checkpointer.save(data);
    
    await checkpointer.delete(data.key);
    const retrievedData = await checkpointer.retrieve(data.key);
    expect(retrievedData).toBeNull();
  });
});
# SurrealDB Checkpointer

This project implements a checkpointer for SurrealDB, allowing for the saving and retrieval of data in a SurrealDB database. The checkpointer is designed to integrate seamlessly with the LangGraph framework.

## Project Structure

- **src/**: Contains the source code for the SurrealDB checkpointer.
  - **index.ts**: Entry point for the checkpointer, exporting the `SurrealDBCheckpointer` class.
  - **migrations.ts**: Contains migration scripts for setting up the SurrealDB schema.
  - **surrealdb.ts**: Handles the connection to the SurrealDB database.
  - **tests/**: Contains integration tests for the checkpointer.

## Setup Instructions

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd checkpoint-sdb
   ```

2. **Install dependencies**:
   ```bash
   npm install
   ```

3. **Configure environment variables**:
   Copy `.env.example` to `.env` and fill in the required database connection details.

4. **Run migrations**:
   Execute the migration scripts to set up the database schema in SurrealDB.

5. **Run tests**:
   Use the following command to run the integration tests:
   ```bash
   npm test
   ```

## Usage

To use the `SurrealDBCheckpointer`, import it in your application and create an instance:

```typescript
import { SurrealDBCheckpointer } from './src/index';

const checkpointer = new SurrealDBCheckpointer();
// Use checkpointer methods to save and retrieve data
```

## License

This project is licensed under the MIT License. See the LICENSE file for details.
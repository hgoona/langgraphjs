// filepath: checkpoint-sdb/jest.env.cjs
const { TestEnvironment } = require("jest-environment-node");

class AdjustedTestEnvironmentToSupportSurrealDB extends TestEnvironment {
  constructor(config, context) {
    super(config, context);
    // Custom setup for SurrealDB if needed
  }
}

module.exports = AdjustedTestEnvironmentToSupportSurrealDB;
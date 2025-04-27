const { MongoClient } = require("mongodb");
const config = require("../config");

/**
 * Connect to MongoDB
 * @param {string} [collectionName=config.mongodb.collections.articles] - Collection name to use
 * @returns {Promise<{client: MongoClient, collection: Collection}>} MongoDB client and collection
 */
async function connect(collectionName = config.mongodb.collections.articles) {
  try {
    console.log(`Connecting to MongoDB at ${config.mongodb.uri}...`);
    const client = new MongoClient(config.mongodb.uri);
    await client.connect();
    console.log("Connected to MongoDB");

    const db = client.db(config.mongodb.dbName);
    const collection = db.collection(collectionName);

    return { client, collection };
  } catch (error) {
    console.error("Error connecting to MongoDB:", error);
    throw error;
  }
}

module.exports = {
  connect,
};

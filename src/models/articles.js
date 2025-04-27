const db = require("../services/database");
const config = require("../config");

/**
 * Check if articles already exist in MongoDB
 * @param {Array} articleIds - Array of article IDs to check
 * @returns {Promise<Object>} - Object with article IDs as keys and boolean values
 */
async function checkExistingArticles(articleIds) {
  if (!articleIds || articleIds.length === 0) {
    return {};
  }

  let client;
  try {
    // Connect to MongoDB
    const connection = await db.connect();
    client = connection.client;
    const collection = connection.collection;

    console.log(
      `Checking ${articleIds.length} articles for existing entries...`
    );

    // Query for existing articles
    const existingArticles = await collection
      .find({ _id: { $in: articleIds } })
      .project({ _id: 1 })
      .toArray();

    // Create a map of article IDs to existence status
    const existingMap = {};
    articleIds.forEach((id) => {
      existingMap[id] = false;
    });

    existingArticles.forEach((article) => {
      existingMap[article._id] = true;
    });

    const existingCount = existingArticles.length;
    console.log(`Found ${existingCount} articles already in the database`);

    return existingMap;
  } catch (error) {
    console.error("Error checking for existing articles:", error);
    return {};
  } finally {
    // Close the MongoDB connection
    if (client) {
      await client.close();
      console.log("MongoDB connection closed");
    }
  }
}

/**
 * Save articles to MongoDB
 * @param {Array} articles - Array of article objects
 * @returns {Promise<Object>} - Result of the insert operation
 */
async function saveArticles(articles) {
  if (!articles || articles.length === 0) {
    console.log("No articles to save");
    return { upsertedCount: 0, modifiedCount: 0, matchedCount: 0 };
  }

  let client;

  try {
    // Connect to MongoDB
    const connection = await db.connect();
    client = connection.client;
    const collection = connection.collection;

    // Set unique ID field for each article
    const articlesWithId = articles.map((article) => ({
      ...article,
      _id: article.id, // Use our id as MongoDB's _id
    }));

    console.log(`Preparing to save ${articles.length} articles to MongoDB...`);

    // Insert articles with upsert (update if exists, insert if not)
    const bulkOps = articlesWithId.map((article) => ({
      updateOne: {
        filter: { _id: article._id },
        update: { $set: article },
        upsert: true,
      },
    }));

    // Execute bulk operation if there are articles
    let result = { upsertedCount: 0, modifiedCount: 0, matchedCount: 0 };
    if (bulkOps.length > 0) {
      result = await collection.bulkWrite(bulkOps);
      console.log("MongoDB operation completed successfully");
      console.log(
        `Inserted: ${result.upsertedCount}, Updated: ${result.modifiedCount}, Matched: ${result.matchedCount}`
      );
    } else {
      console.log("No articles to save");
    }

    return result;
  } catch (error) {
    console.error("Error saving articles to MongoDB:", error);
    throw error;
  } finally {
    // Close the MongoDB connection
    if (client) {
      await client.close();
      console.log("MongoDB connection closed");
    }
  }
}

module.exports = {
  checkExistingArticles,
  saveArticles,
};

const db = require("../services/database");
const config = require("../config");
const fileProcessor = require("../services/fileProcessor");

/**
 * Get list of already processed files from MongoDB
 * @returns {Promise<Object>} - Map of file keys to last modified timestamps
 */
async function getProcessedFiles() {
  let client;
  try {
    // Connect to MongoDB
    const connection = await db.connect(
      config.mongodb.collections.processedFiles
    );
    client = connection.client;
    const collection = connection.collection;

    // Get all processed file pairs
    const processedPairs = await collection.find({}).toArray();

    // Create a map of individual file paths to last modified timestamps
    const processedFilesMap = {};

    processedPairs.forEach((pair) => {
      // Add HTML file if it exists
      if (pair.html && pair.html.path && pair.html.lastModified) {
        processedFilesMap[pair.html.path] = pair.html.lastModified;
      }

      // Add metadata file if it exists
      if (pair.metadata && pair.metadata.path && pair.metadata.lastModified) {
        processedFilesMap[pair.metadata.path] = pair.metadata.lastModified;
      }
    });

    console.log(
      `Retrieved ${
        Object.keys(processedFilesMap).length
      } processed files from database (${processedPairs.length} file pairs)`
    );
    return processedFilesMap;
  } catch (error) {
    console.error("Error getting processed files from MongoDB:", error);
    return {};
  } finally {
    // Close the MongoDB connection
    if (client) {
      await client.close();
    }
  }
}

/**
 * Save processed files to MongoDB
 * @param {Object} processedFiles - Map of file keys to last modified timestamps
 * @param {Object} processingResults - Results of processing each file (success/fail)
 * @returns {Promise<boolean>} - Success or failure
 */
async function saveProcessedFiles(processedFiles, processingResults = {}) {
  if (!processedFiles || Object.keys(processedFiles).length === 0) {
    return true;
  }

  let client;
  try {
    // Connect to MongoDB
    const connection = await db.connect(
      config.mongodb.collections.processedFiles
    );
    client = connection.client;
    const collection = connection.collection;

    // Group files by their directory (domain/hash)
    const filesByPair = {};

    // First pass: organize files by pair_id
    Object.entries(processedFiles).forEach(([key, lastModified]) => {
      // Extract information from the key
      const keyParts = key.split("/");

      // Skip if we don't have enough parts
      if (keyParts.length < 4) return;

      const domain = keyParts[1];
      const hash = keyParts[2];
      const fileName = keyParts[3];
      const pair_id = `${domain}/${hash}`;
      const fileType = fileName.endsWith(".html")
        ? "html"
        : fileName.endsWith(".json")
        ? "metadata"
        : "other";

      if (!filesByPair[pair_id]) {
        filesByPair[pair_id] = {
          pair_id,
          domain,
          hash,
          files: {},
          processedAt: new Date(),
        };
      }

      // Get processing result info if available
      const result = processingResults[key] || {
        success: true,
        error: null,
      };

      // Add file to the pair
      filesByPair[pair_id].files[fileType] = {
        path: key,
        lastModified,
        lastModifiedDate: new Date(lastModified),
        processingTimeMs: result.processingTimeMs,
        status: result.success ? "success" : "failed",
        error: result.error,
        fileSize: result.fileSize,
      };
    });

    // Convert to array of documents for MongoDB
    const documents = Object.values(filesByPair).map((pair) => ({
      pair_id: pair.pair_id,
      domain: pair.domain,
      hash: pair.hash,
      processedAt: pair.processedAt,
      html: pair.files.html || null,
      metadata: pair.files.metadata || null,
      hasBoth: !!(pair.files.html && pair.files.metadata),
      status: fileProcessor.getOverallStatus(pair),
    }));

    // Insert documents with upsert (update if exists, insert if not)
    const bulkOps = documents.map((doc) => ({
      updateOne: {
        filter: { pair_id: doc.pair_id },
        update: { $set: doc },
        upsert: true,
      },
    }));

    // Execute bulk operation
    const result = await collection.bulkWrite(bulkOps);
    console.log(`Saved ${documents.length} processed file pairs to database`);
    console.log(
      `Inserted: ${result.upsertedCount}, Updated: ${result.modifiedCount}`
    );

    return true;
  } catch (error) {
    console.error("Error saving processed files to MongoDB:", error);
    return false;
  } finally {
    // Close the MongoDB connection
    if (client) {
      await client.close();
    }
  }
}

/**
 * Query processed file pairs from the database by domain, hash, or status
 * @param {Object} query - Query parameters
 * @returns {Promise<Object>} - Query results with pagination
 */
async function queryProcessedFiles(query = {}) {
  let client;
  try {
    // Connect to MongoDB
    const connection = await db.connect(
      config.mongodb.collections.processedFiles
    );
    client = connection.client;
    const collection = connection.collection;

    // Build the MongoDB query
    const mongoQuery = {};

    if (query.pair_id) {
      mongoQuery.pair_id = query.pair_id;
    }

    if (query.domain) {
      mongoQuery.domain = query.domain;
    }

    if (query.hash) {
      mongoQuery.hash = query.hash;
    }

    if (query.status) {
      mongoQuery.status = query.status;
    }

    if (query.hasBoth !== undefined) {
      mongoQuery.hasBoth = query.hasBoth;
    }

    // Date range query for processing time
    if (query.processedAfter || query.processedBefore) {
      mongoQuery.processedAt = {};

      if (query.processedAfter) {
        mongoQuery.processedAt.$gte = new Date(query.processedAfter);
      }

      if (query.processedBefore) {
        mongoQuery.processedAt.$lte = new Date(query.processedBefore);
      }
    }

    // Date range query for HTML file modification time
    if (query.htmlModifiedAfter || query.htmlModifiedBefore) {
      mongoQuery["html.lastModifiedDate"] = {};

      if (query.htmlModifiedAfter) {
        mongoQuery["html.lastModifiedDate"].$gte = new Date(
          query.htmlModifiedAfter
        );
      }

      if (query.htmlModifiedBefore) {
        mongoQuery["html.lastModifiedDate"].$lte = new Date(
          query.htmlModifiedBefore
        );
      }
    }

    // Date range query for metadata file modification time
    if (query.metadataModifiedAfter || query.metadataModifiedBefore) {
      mongoQuery["metadata.lastModifiedDate"] = {};

      if (query.metadataModifiedAfter) {
        mongoQuery["metadata.lastModifiedDate"].$gte = new Date(
          query.metadataModifiedAfter
        );
      }

      if (query.metadataModifiedBefore) {
        mongoQuery["metadata.lastModifiedDate"].$lte = new Date(
          query.metadataModifiedBefore
        );
      }
    }

    // Set up pagination
    const limit = query.limit || 100;
    const skip = query.skip || 0;

    // Execute the query
    const pairs = await collection
      .find(mongoQuery)
      .sort({ processedAt: -1 }) // Most recent first
      .skip(skip)
      .limit(limit)
      .toArray();

    // Get total count
    const total = await collection.countDocuments(mongoQuery);

    console.log(
      `Found ${pairs.length} processed file pairs matching query (total: ${total})`
    );

    return {
      pairs,
      total,
      limit,
      skip,
    };
  } catch (error) {
    console.error("Error querying processed files from MongoDB:", error);
    return { pairs: [], total: 0, limit: 0, skip: 0 };
  } finally {
    // Close the MongoDB connection
    if (client) {
      await client.close();
    }
  }
}

/**
 * Get a summary of processed file pairs, grouped by domain and status
 * @returns {Promise<Object>} - Summary statistics
 */
async function getProcessedFilesSummary() {
  let client;
  try {
    // Connect to MongoDB
    const connection = await db.connect(
      config.mongodb.collections.processedFiles
    );
    client = connection.client;
    const collection = connection.collection;

    // Get counts by domain
    const domainStats = await collection
      .aggregate([
        {
          $group: {
            _id: "$domain",
            totalPairs: { $sum: 1 },
            successPairs: {
              $sum: { $cond: [{ $eq: ["$status", "success"] }, 1, 0] },
            },
            failedPairs: {
              $sum: { $cond: [{ $eq: ["$status", "failed"] }, 1, 0] },
            },
            incompletePairs: {
              $sum: { $cond: [{ $eq: ["$status", "incomplete"] }, 1, 0] },
            },
            completePairs: {
              $sum: { $cond: [{ $eq: ["$hasBoth", true] }, 1, 0] },
            },
            htmlOnlyPairs: {
              $sum: {
                $cond: [
                  {
                    $and: [
                      { $ne: ["$html", null] },
                      { $eq: ["$metadata", null] },
                    ],
                  },
                  1,
                  0,
                ],
              },
            },
            metadataOnlyPairs: {
              $sum: {
                $cond: [
                  {
                    $and: [
                      { $eq: ["$html", null] },
                      { $ne: ["$metadata", null] },
                    ],
                  },
                  1,
                  0,
                ],
              },
            },
            totalHtmlProcessingTime: {
              $sum: { $ifNull: ["$html.processingTimeMs", 0] },
            },
            totalMetadataProcessingTime: {
              $sum: { $ifNull: ["$metadata.processingTimeMs", 0] },
            },
          },
        },
      ])
      .toArray();

    // Get overall stats
    const totalStats = await collection
      .aggregate([
        {
          $group: {
            _id: null,
            totalPairs: { $sum: 1 },
            successPairs: {
              $sum: { $cond: [{ $eq: ["$status", "success"] }, 1, 0] },
            },
            failedPairs: {
              $sum: { $cond: [{ $eq: ["$status", "failed"] }, 1, 0] },
            },
            incompletePairs: {
              $sum: { $cond: [{ $eq: ["$status", "incomplete"] }, 1, 0] },
            },
            completePairs: {
              $sum: { $cond: [{ $eq: ["$hasBoth", true] }, 1, 0] },
            },
            htmlOnlyPairs: {
              $sum: {
                $cond: [
                  {
                    $and: [
                      { $ne: ["$html", null] },
                      { $eq: ["$metadata", null] },
                    ],
                  },
                  1,
                  0,
                ],
              },
            },
            metadataOnlyPairs: {
              $sum: {
                $cond: [
                  {
                    $and: [
                      { $eq: ["$html", null] },
                      { $ne: ["$metadata", null] },
                    ],
                  },
                  1,
                  0,
                ],
              },
            },
            totalHtmlProcessingTime: {
              $sum: { $ifNull: ["$html.processingTimeMs", 0] },
            },
            totalMetadataProcessingTime: {
              $sum: { $ifNull: ["$metadata.processingTimeMs", 0] },
            },
            // Get min and max dates
            minProcessedAt: { $min: "$processedAt" },
            maxProcessedAt: { $max: "$processedAt" },
            minHtmlModifiedDate: { $min: "$html.lastModifiedDate" },
            maxHtmlModifiedDate: { $max: "$html.lastModifiedDate" },
            minMetadataModifiedDate: { $min: "$metadata.lastModifiedDate" },
            maxMetadataModifiedDate: { $max: "$metadata.lastModifiedDate" },
          },
        },
      ])
      .toArray();

    return {
      domainStats,
      totalStats: totalStats[0] || { totalPairs: 0 },
    };
  } catch (error) {
    console.error("Error getting processed files summary from MongoDB:", error);
    return { domainStats: [], totalStats: { totalPairs: 0 } };
  } finally {
    // Close the MongoDB connection
    if (client) {
      await client.close();
    }
  }
}

module.exports = {
  getProcessedFiles,
  saveProcessedFiles,
  queryProcessedFiles,
  getProcessedFilesSummary,
};

require("dotenv").config();
const AWS = require("aws-sdk");
const { Readability } = require("@mozilla/readability");
const { JSDOM } = require("jsdom");
const { MongoClient } = require("mongodb");
const fs = require("fs");
const path = require("path");
const cron = require("node-cron");

// Debug: Print credential provider chain details
const credentialsObj = AWS.config.credentials;
console.log(
  "AWS SDK Credentials:",
  credentialsObj ? "Available" : "Not available"
);

// MongoDB connection string
const MONGODB_URI = process.env.MONGODB_URI || "mongodb://localhost:27017";
const DB_NAME = process.env.DB_NAME || "crawled_news";
const COLLECTION_NAME = process.env.COLLECTION_NAME || "crawled_articles";
const PROCESSED_FILES_COLLECTION = "crawled_processed_files";

// S3 client
const s3 = new AWS.S3();

const params = {
  Bucket: process.env.S3_BUCKET || "second-hapttic-bucket",
};

/**
 * Load the list of already analyzed files
 * @returns {Object} - Object with file keys as properties
 */
function loadAnalyzedFiles() {
  try {
    if (fs.existsSync(ANALYZED_FILES_LOG)) {
      const data = fs.readFileSync(ANALYZED_FILES_LOG, "utf8");
      return JSON.parse(data);
    }
    console.log("No analyzed files log found, creating a new one");
    return { files: {}, lastRun: null };
  } catch (error) {
    console.error("Error loading analyzed files log:", error);
    return { files: {}, lastRun: null };
  }
}

/**
 * Save the list of analyzed files
 * @param {Object} analyzedFiles - Object with file keys and timestamps
 */
function saveAnalyzedFiles(analyzedFiles) {
  try {
    // Update the last run timestamp
    analyzedFiles.lastRun = new Date().toISOString();

    // Ensure directory exists
    const dir = path.dirname(ANALYZED_FILES_LOG);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }

    fs.writeFileSync(
      ANALYZED_FILES_LOG,
      JSON.stringify(analyzedFiles, null, 2),
      "utf8"
    );
    console.log(
      `Saved ${Object.keys(analyzedFiles.files).length} analyzed files to log`
    );
  } catch (error) {
    console.error("Error saving analyzed files log:", error);
  }
}

/**
 * Connect to MongoDB
 * @returns {Promise<Object>} - MongoDB client and collection
 */
async function connectToMongoDB(collectionName = COLLECTION_NAME) {
  try {
    console.log(`Connecting to MongoDB at ${MONGODB_URI}...`);
    const client = new MongoClient(MONGODB_URI);
    await client.connect();
    console.log("Connected to MongoDB");

    const db = client.db(DB_NAME);
    const collection = db.collection(collectionName);

    return { client, collection };
  } catch (error) {
    console.error("Error connecting to MongoDB:", error);
    throw error;
  }
}

/**
 * Get list of already processed files from MongoDB
 * @returns {Promise<Object>} - Map of file keys to last modified timestamps
 */
async function getProcessedFilesFromDB() {
  let client;
  try {
    // Connect to MongoDB
    const connection = await connectToMongoDB(PROCESSED_FILES_COLLECTION);
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
async function saveProcessedFilesToDB(processedFiles, processingResults = {}) {
  if (!processedFiles || Object.keys(processedFiles).length === 0) {
    return true;
  }

  let client;
  try {
    // Connect to MongoDB
    const connection = await connectToMongoDB(PROCESSED_FILES_COLLECTION);
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
      // _id: pair.pair_id, // Let MongoDB generate _id
      pair_id: pair.pair_id,
      domain: pair.domain,
      hash: pair.hash,
      processedAt: pair.processedAt,
      html: pair.files.html || null,
      metadata: pair.files.metadata || null,
      hasBoth: !!(pair.files.html && pair.files.metadata),
      status: getOverallStatus(pair),
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
 * Determine overall status for a file pair
 * @param {Object} pair - File pair object
 * @returns {string} - Overall status
 */
function getOverallStatus(pair) {
  if (!pair.files.html && !pair.files.metadata) {
    return "unknown";
  }

  const html = pair.files.html || { status: "missing" };
  const metadata = pair.files.metadata || { status: "missing" };

  if (html.status === "failed" || metadata.status === "failed") {
    return "failed";
  }

  if (html.status === "missing" || metadata.status === "missing") {
    return "incomplete";
  }

  return "success";
}

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
    const connection = await connectToMongoDB();
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
async function saveArticlesToMongoDB(articles) {
  if (!articles || articles.length === 0) {
    console.log("No articles to save");
    return { upsertedCount: 0, modifiedCount: 0, matchedCount: 0 };
  }

  let client;

  try {
    // Connect to MongoDB
    const connection = await connectToMongoDB();
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

/**
 * Filters S3 files that were modified after the specified hours
 * @param {Array} files - Array of S3 objects from listObjectsV2
 * @param {Number} hours - Number of hours to look back
 * @returns {Array} - Filtered array of S3 objects
 */
function getFilesModifiedAfter(files, hours) {
  if (!files || !Array.isArray(files)) {
    return [];
  }

  const cutoffTime = new Date();
  cutoffTime.setHours(cutoffTime.getHours() - hours);

  return files.filter((file) => {
    return file.LastModified > cutoffTime;
  });
}

/**
 * Gets the content of a file from S3
 * @param {string} key - S3 object key
 * @returns {Promise<string>} - File content as string
 */
async function getFileContent(key) {
  try {
    const fileParams = {
      Bucket: params.Bucket,
      Key: key,
    };

    const data = await s3.getObject(fileParams).promise();
    return data.Body.toString("utf-8");
  } catch (error) {
    console.error(`Error retrieving file ${key}:`, error);
    throw error;
  }
}

/**
 * Parse HTML content using Readability
 * @param {string} html - HTML content
 * @param {string} url - URL of the page (optional)
 * @returns {Object} - Parsed article data
 */
function parseHtmlWithReadability(html, url = "") {
  try {
    // Create a DOM object from the HTML content
    // Use default URL if none provided or if URL is invalid
    const domOptions = {
      url: url && url.startsWith("http") ? url : "https://example.com",
    };

    const dom = new JSDOM(html, domOptions);

    // Create a new Readability object
    const reader = new Readability(dom.window.document);

    // Parse the content
    const article = reader.parse();

    // Check if article content is meaningful (not just boilerplate text)
    if (article && article.textContent) {
      const textLength = article.textContent.trim().length;
      if (textLength < 50) {
        console.log(
          `Warning: Very short article content (${textLength} chars) for URL: ${url}`
        );
        // Still return the article, but flag it as potentially problematic
        article.isPotentiallyEmpty = true;
      }
    }

    return article;
  } catch (error) {
    console.error("Error parsing HTML with Readability:", error);
    return null;
  }
}

/**
 * Extract essential metadata fields
 * @param {Object} metadata - Metadata object
 * @returns {Object|null} - Object with essential metadata or null if missing
 */
function extractEssentialMetadata(metadata) {
  if (!metadata) return null;

  const essentialFields = ["url", "crawl_time", "depth"];
  const extracted = {};

  // Check if any essential fields exist
  const hasEssentialFields = essentialFields.some(
    (key) => metadata[key] !== undefined
  );
  if (!hasEssentialFields) return null;

  // Copy essential fields
  essentialFields.forEach((field) => {
    if (metadata[field] !== undefined) {
      extracted[field] = metadata[field];
    }
  });

  // Parse crawl_time to create crawl_datetime if possible
  if (extracted.crawl_time) {
    try {
      extracted.crawl_datetime = new Date(extracted.crawl_time);
    } catch (error) {
      console.error(`Error parsing crawl_time: ${extracted.crawl_time}`, error);
    }
  }

  return extracted;
}

/**
 * Creates an article object with essential data
 * @param {Object} data - Article data including metadata and parsed content
 * @returns {Object|null} - Formatted article object or null if invalid
 */
function createArticleObject(data) {
  if (!data.metadata || !data.parsed) return null;

  // Extract essential metadata
  const metadata = extractEssentialMetadata(data.metadata);
  if (!metadata) return null;

  // Create article object
  return {
    id: `${data.domain}/${data.hash}`,
    domain: data.domain,
    hash: data.hash,
    title: data.parsed.title,
    excerpt: data.parsed.excerpt || "",
    content: data.parsed.textContent,
    contentLength: data.parsed.textContent.length,
    isPotentiallyEmpty: data.parsed.isPotentiallyEmpty || false,
    url: metadata.url || "",
    crawl_time: metadata.crawl_time || "",
    crawl_datetime: metadata.crawl_datetime || null,
    depth: metadata.depth || "",
  };
}

/**
 * Check if an article has valid metadata
 * @param {Object} metadata - Metadata object
 * @returns {boolean} - Whether the metadata is valid
 */
function hasValidMetadata(metadata) {
  if (!metadata) return false;

  // Check if url exists and is a valid URL
  if (!metadata.url || typeof metadata.url !== "string") return false;

  try {
    new URL(metadata.url); // This will throw if URL is invalid
    return true;
  } catch (error) {
    return false;
  }
}

/**
 * Groups files by their hash directory
 * @param {Array} files - Array of S3 object keys
 * @returns {Object} - Object with hash directories as keys and arrays of files as values
 */
function groupFilesByHash(files) {
  const grouped = {};

  files.forEach((file) => {
    // Parse the path to extract domain, hash, and filename
    const pathParts = file.Key.split("/");
    if (pathParts.length >= 3) {
      const domain = pathParts[1];
      const hash = pathParts[2];
      const filename = pathParts[3] || "";

      const groupKey = `${domain}/${hash}`;

      if (!grouped[groupKey]) {
        grouped[groupKey] = {
          domain,
          hash,
          files: [],
        };
      }

      grouped[groupKey].files.push({
        key: file.Key,
        filename,
        size: file.Size,
        lastModified: file.LastModified,
      });
    }
  });

  return grouped;
}

/**
 * Checks each hash group for broken links (missing metadata.json)
 * @param {Object} groupedFiles - Files grouped by hash
 * @returns {Object} - Same object with isBroken flag added
 */
function identifyBrokenLinks(groupedFiles) {
  Object.keys(groupedFiles).forEach((groupKey) => {
    const group = groupedFiles[groupKey];
    const filenames = group.files.map((file) => file.filename);

    // Check if this group has page.html but no metadata.json
    const hasHtml = filenames.includes("page.html");
    const hasMetadata = filenames.includes("metadata.json");

    group.isBroken = hasHtml && !hasMetadata;
    group.isComplete = hasHtml && hasMetadata;
  });

  return groupedFiles;
}

/**
 * Reads page.html and metadata from files with specified pattern
 * @param {Array} files - Array of S3 objects
 * @param {Object} processedFiles - Object tracking already processed files
 * @returns {Promise<Object>} - Object with results and newly processed files
 */
async function readHtmlAndMetadata(files, processedFiles) {
  const results = [];
  const newlyProcessedFiles = {};
  const processingResults = {};
  let skippedCount = 0;

  for (const file of files) {
    try {
      // Check if file is page.html or metadata.json
      if (
        file.Key.endsWith("/page.html") ||
        file.Key.endsWith("/metadata.json")
      ) {
        // Check if this file has been processed before and hasn't been modified
        const fileKey = file.Key;
        const lastModified = file.LastModified.toISOString();

        if (
          processedFiles[fileKey] &&
          processedFiles[fileKey] === lastModified
        ) {
          skippedCount++;
          continue; // Skip this file as it's already been processed
        }

        // Track processing time
        const startTime = Date.now();
        let success = false;
        let error = null;

        try {
          const content = await getFileContent(fileKey);

          // Mark this file as processed
          newlyProcessedFiles[fileKey] = lastModified;

          results.push({
            key: fileKey,
            type: fileKey.endsWith("/page.html") ? "html" : "metadata",
            lastModified: file.LastModified,
            content,
          });

          success = true;
        } catch (err) {
          success = false;
          error = err.message || "Unknown error";
          console.error(`Error processing file ${fileKey}:`, err);
        }

        // Record processing result
        const endTime = Date.now();
        processingResults[fileKey] = {
          success,
          error,
          processingTimeMs: endTime - startTime,
          fileSize: file.Size,
        };
      }
    } catch (error) {
      console.error(`Error processing file ${file.Key}:`, error);
    }
  }

  console.log(
    `Skipped ${skippedCount} previously processed files that haven't changed`
  );

  return {
    results,
    newlyProcessedFiles,
    processingResults,
  };
}

/**
 * Lists all files in bucket using pagination
 * @param {Object} options - Options for listing objects
 * @returns {Promise<Array>} - All files from the bucket
 */
async function listAllFilesInBucket(options = {}) {
  const allFiles = [];
  let continuationToken = null;
  let pageCount = 0;

  do {
    try {
      // Prepare parameters for the next request
      const listParams = {
        ...params,
        MaxKeys: options.maxKeys || 1000,
        ContinuationToken: continuationToken,
      };

      // If prefix is specified, add it to the params
      if (options.prefix) {
        listParams.Prefix = options.prefix;
      }

      // Request the next page of results
      const data = await s3.listObjectsV2(listParams).promise();

      // Add the files to our collection
      if (data.Contents && data.Contents.length > 0) {
        allFiles.push(...data.Contents);
        pageCount++;

        console.log(
          `Retrieved page ${pageCount} with ${data.Contents.length} files (total: ${allFiles.length})`
        );
      }

      // Set the continuation token for the next request
      continuationToken = data.IsTruncated ? data.NextContinuationToken : null;

      // If we hit a page limit, stop
      if (options.maxPages && pageCount >= options.maxPages) {
        console.log(
          `Reached maximum page count (${options.maxPages}), stopping pagination`
        );
        break;
      }
    } catch (error) {
      console.error("Error listing files from S3 bucket:", error);
      throw error;
    }
  } while (continuationToken);

  console.log(
    `Retrieved ${allFiles.length} total files from ${pageCount} pages`
  );
  return allFiles;
}

async function listFilesInBucket() {
  try {
    const data = await s3.listObjectsV2(params).promise();

    console.log(`Found ${data.KeyCount} files in bucket:`);

    data.Contents.forEach((item) => {
      console.log(
        `- ${item.Key} (Size: ${item.Size} bytes, Last Modified: ${item.LastModified})`
      );
    });

    // If there are more files (pagination)
    if (data.IsTruncated) {
      console.log(
        "More files exist but were not listed due to pagination limits"
      );
      console.log("Use ContinuationToken to retrieve more files");
    }

    return data.Contents;
  } catch (error) {
    console.error("Error listing files from S3 bucket:", error);
    throw error;
  }
}

// Get files modified in the last 24 hours
async function getRecentFiles(hours = 24, useAllPagination = false) {
  try {
    let allFiles;

    if (useAllPagination) {
      // Get all files using pagination
      allFiles = await listAllFilesInBucket();
    } else {
      // Get just the first page of files
      allFiles = await listFilesInBucket();
    }

    const recentFiles = getFilesModifiedAfter(allFiles, hours);

    console.log(
      `Found ${recentFiles.length} files modified in the last ${hours} hours:`
    );
    recentFiles.forEach((file) => {
      console.log(`- ${file.Key} (Last Modified: ${file.LastModified})`);
    });

    return recentFiles;
  } catch (error) {
    console.error("Error getting recent files:", error);
    throw error;
  }
}

// Process and analyze HTML and metadata files
async function readRecentFiles(
  hours = 1,
  useAllPagination = false,
  saveToMongoDB = true
) {
  try {
    // Get list of already processed files from database
    const processedFiles = await getProcessedFilesFromDB();
    console.log(
      `Loaded ${
        Object.keys(processedFiles).length
      } previously processed files from database`
    );

    const recentFiles = await getRecentFiles(hours, useAllPagination);
    console.log(
      `Processing ${recentFiles.length} files for HTML and metadata...`
    );

    // Group files by hash directory
    let groupedByHash = groupFilesByHash(recentFiles);

    // Identify broken links (missing metadata)
    groupedByHash = identifyBrokenLinks(groupedByHash);

    // Count broken links
    const brokenLinks = Object.values(groupedByHash).filter(
      (group) => group.isBroken
    );
    const completeLinks = Object.values(groupedByHash).filter(
      (group) => group.isComplete
    );

    console.log(
      `\nFound ${Object.keys(groupedByHash).length} unique hash directories:`
    );
    console.log(
      `- ${completeLinks.length} complete links (have both page.html and metadata.json)`
    );
    console.log(
      `- ${brokenLinks.length} broken links (have page.html but missing metadata.json)`
    );

    // Display grouped files
    Object.keys(groupedByHash).forEach((groupKey) => {
      const group = groupedByHash[groupKey];

      // Add status indicator to the group header
      let statusIndicator = "";
      if (group.isBroken) {
        statusIndicator = "❌ BROKEN LINK (Missing metadata)";
      } else if (group.isComplete) {
        statusIndicator = "✓ Complete";
      }

      console.log(
        `\n=== ${group.domain}/${group.hash} ${statusIndicator} (${group.files.length} files) ===`
      );

      // Sort files by filename for consistent display
      group.files.sort((a, b) => a.filename.localeCompare(b.filename));

      group.files.forEach((file) => {
        console.log(
          `- ${file.filename} (Size: ${file.size} bytes, Last Modified: ${file.lastModified})`
        );
      });
    });

    // Read HTML and metadata files, skipping previously processed ones
    const htmlAndMetadata = await readHtmlAndMetadata(
      recentFiles,
      processedFiles
    );
    const fileContents = htmlAndMetadata.results;
    const newlyProcessedFiles = htmlAndMetadata.newlyProcessedFiles;
    const processingResults = htmlAndMetadata.processingResults;

    console.log(
      `Retrieved ${fileContents.length} HTML/metadata files (after skipping previously processed files)`
    );

    // Summarize processing results
    const successCount = Object.values(processingResults).filter(
      (r) => r.success
    ).length;
    const failureCount = Object.values(processingResults).filter(
      (r) => !r.success
    ).length;
    const totalProcessingTime = Object.values(processingResults).reduce(
      (sum, r) => sum + r.processingTimeMs,
      0
    );
    const totalProcessedSize = Object.values(processingResults).reduce(
      (sum, r) => sum + (r.fileSize || 0),
      0
    );

    console.log("\n\n================ PROCESSING SUMMARY ================");
    console.log(
      `Total files processed: ${Object.keys(processingResults).length}`
    );
    console.log(`Successful: ${successCount}, Failed: ${failureCount}`);
    console.log(`Total processing time: ${totalProcessingTime}ms`);
    console.log(
      `Total data processed: ${(totalProcessedSize / 1024 / 1024).toFixed(2)}MB`
    );

    if (failureCount > 0) {
      console.log("\n----- PROCESSING FAILURES -----");
      Object.entries(processingResults)
        .filter(([_, result]) => !result.success)
        .forEach(([key, result]) => {
          console.log(`- ${key}: ${result.error}`);
        });
    }

    // Process each file content
    const processedContent = {};
    const invalidMetadataUrls = [];
    const failedReadabilityLinks = [];

    for (const file of fileContents) {
      // Extract the group key (domain/hash)
      const keyParts = file.key.split("/");
      const domain = keyParts[1];
      const hash = keyParts[2];
      const groupKey = `${domain}/${hash}`;

      if (!processedContent[groupKey]) {
        processedContent[groupKey] = {
          domain,
          hash,
          metadata: null,
          html: null,
          parsed: null,
        };
      }

      if (file.type === "metadata") {
        try {
          processedContent[groupKey].metadata = JSON.parse(file.content);

          // Check for invalid metadata URL
          if (!hasValidMetadata(processedContent[groupKey].metadata)) {
            invalidMetadataUrls.push({
              id: groupKey,
              metadata: processedContent[groupKey].metadata,
            });
          }
        } catch (error) {
          console.error(`Error parsing metadata JSON for ${groupKey}:`, error);
          processedContent[groupKey].metadata = { error: "Invalid JSON" };

          // Add to invalid metadata list
          invalidMetadataUrls.push({
            id: groupKey,
            error: "Invalid JSON",
          });
        }
      } else if (file.type === "html") {
        processedContent[groupKey].html = file.content;

        // Use Readability to parse HTML content
        const url = processedContent[groupKey].metadata?.url || "";
        const parsedArticle = parseHtmlWithReadability(file.content, url);
        processedContent[groupKey].parsed = parsedArticle;

        // Check if Readability failed to extract meaningful content
        if (
          !parsedArticle ||
          !parsedArticle.content ||
          parsedArticle.textContent.trim().length < 100
        ) {
          failedReadabilityLinks.push({
            id: groupKey,
            url: url,
            htmlLength: file.content.length,
            parsedResult: parsedArticle
              ? {
                  title: parsedArticle.title,
                  excerpt: parsedArticle.excerpt,
                  contentLength: parsedArticle.textContent
                    ? parsedArticle.textContent.trim().length
                    : 0,
                }
              : null,
          });
        }
      }
    }

    // Print invalid metadata URLs
    console.log("\n\n================ INVALID METADATA URLS ================");
    if (invalidMetadataUrls.length === 0) {
      console.log("No invalid metadata URLs found");
    } else {
      console.log(`Found ${invalidMetadataUrls.length} invalid metadata URLs:`);
      invalidMetadataUrls.forEach((item) => {
        console.log(`\n--- ${item.id} ---`);
        if (item.error) {
          console.log(`Error: ${item.error}`);
        } else if (item.metadata) {
          console.log(`URL: ${item.metadata.url || "undefined"}`);
          console.log(`crawl_time: ${item.metadata.crawl_time || "undefined"}`);
          console.log(
            `All metadata: ${JSON.stringify(item.metadata, null, 2)}`
          );
        }
      });
    }

    // Print links where Readability failed to extract content
    console.log(
      "\n\n================ FAILED READABILITY EXTRACTION ================"
    );
    if (failedReadabilityLinks.length === 0) {
      console.log("No failed Readability extractions found");
    } else {
      console.log(
        `Found ${failedReadabilityLinks.length} links where Readability failed to extract meaningful content:`
      );
      failedReadabilityLinks.forEach((item) => {
        console.log(`\n--- ${item.id} ---`);
        console.log(`URL: ${item.url || "undefined"}`);
        console.log(`HTML Size: ${item.htmlLength} bytes`);
        if (item.parsedResult) {
          console.log(`Title: ${item.parsedResult.title || "undefined"}`);
          console.log(`Excerpt: ${item.parsedResult.excerpt || "undefined"}`);
          console.log(
            `Content Length: ${item.parsedResult.contentLength} characters`
          );
        } else {
          console.log("Parsing returned null result");
        }
      });
    }

    // Create article objects from processed content
    const articleCandidates = [];

    Object.keys(processedContent).forEach((groupKey) => {
      const content = processedContent[groupKey];
      const article = createArticleObject(content);

      if (article) {
        articleCandidates.push(article);
      }
    });

    // Check which articles already exist in the database
    const articleIds = articleCandidates.map((article) => article.id);
    const existingArticles = saveToMongoDB
      ? await checkExistingArticles(articleIds)
      : {};

    // Filter out articles that already exist in the database
    const newArticles = articleCandidates.filter(
      (article) => !existingArticles[article.id]
    );
    const skippedArticles = articleCandidates.filter(
      (article) => existingArticles[article.id]
    );

    // Display article objects
    console.log("\n\n================ VALID ARTICLES ================");
    console.log(
      `Found ${articleCandidates.length} valid articles with metadata and content`
    );
    console.log(
      `- ${newArticles.length} new articles to be added to the database`
    );
    console.log(
      `- ${skippedArticles.length} articles already exist in the database`
    );

    if (newArticles.length > 0) {
      console.log("\n----- NEW ARTICLES -----");
      newArticles.forEach((article) => {
        console.log(`\n--- Article object for ${article.id} ---`);
        console.log(JSON.stringify(article, null, 2));
        console.log("=".repeat(80));
      });
    }

    if (skippedArticles.length > 0) {
      console.log("\n----- SKIPPED ARTICLES (ALREADY IN DATABASE) -----");
      skippedArticles.forEach((article) => {
        console.log(`- ${article.id} (${article.title})`);
      });
    }

    // Save articles to MongoDB if requested
    let mongoResult = null;
    if (saveToMongoDB && newArticles.length > 0) {
      console.log("\n\n================ SAVING TO MONGODB ================");
      mongoResult = await saveArticlesToMongoDB(newArticles);
    } else if (saveToMongoDB) {
      console.log("\n\n================ MONGODB UPDATE ================");
      console.log("No new articles to save to MongoDB");
    }

    // Print summary of broken links
    if (brokenLinks.length > 0) {
      console.log("\n\n================ BROKEN LINKS SUMMARY ================");
      brokenLinks.forEach((group) => {
        console.log(`- ${group.domain}/${group.hash}`);
      });
    }

    // Save the newly processed files to database
    if (Object.keys(newlyProcessedFiles).length > 0) {
      await saveProcessedFilesToDB(newlyProcessedFiles, processingResults);
    }

    return {
      recentFiles,
      fileContents,
      groupedByHash,
      brokenLinks,
      completeLinks,
      processedContent,
      articles: newArticles,
      skippedArticles,
      invalidMetadataUrls,
      failedReadabilityLinks,
      mongoResult,
    };
  } catch (error) {
    console.error("Error reading recent files:", error);
    throw error;
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
    const connection = await connectToMongoDB(PROCESSED_FILES_COLLECTION);
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
    const connection = await connectToMongoDB(PROCESSED_FILES_COLLECTION);
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

// Main entry point for the application
const newsProcessor = require("./controllers/newsProcessor");
const processedFilesModel = require("./models/processedFiles");

// Command line arguments
const args = process.argv.slice(2);
const command = args[0] || "process";
const hoursArg = args[1] || "1";
const hours = parseInt(hoursArg, 10);
const useAllPagination = args[2] === "true" || args[2] === "1";

/**
 * Process recent articles with default settings (1 hour lookback)
 * Used for the cron job
 */
async function processRecentArticles() {
  const startTime = new Date();
  console.log(
    `[${startTime.toISOString()}] Cron job started: Checking for new articles in the last hour`
  );

  try {
    // Always process the last hour of data
    // Don't use pagination to make the cron job faster
    const result = await newsProcessor.processRecentFiles(1, false, true);

    const endTime = new Date();
    const duration = (endTime - startTime) / 1000; // in seconds

    console.log(
      `[${endTime.toISOString()}] Cron job completed in ${duration.toFixed(2)}s`
    );
    console.log(
      `Found ${result.recentFiles.length} files modified in the last hour`
    );
    console.log(`Processed ${result.fileContents.length} HTML/metadata files`);
    console.log(`Created ${result.articles.length} new articles`);
    console.log(`Skipped ${result.skippedArticles.length} existing articles`);

    return result;
  } catch (error) {
    console.error(`[${new Date().toISOString()}] Cron job error:`, error);
  }
}

// Execute command based on arguments
async function main() {
  try {
    switch (command) {
      case "process":
        console.log(
          `Processing files modified in the last ${hours} hours (useAllPagination: ${useAllPagination})`
        );
        await newsProcessor.processRecentFiles(hours, useAllPagination, true);
        break;

      case "summary":
        console.log("Generating processed files summary...");
        const summary = await processedFilesModel.getProcessedFilesSummary();
        console.log("\n================ SUMMARY ================");
        console.log(
          `Total processed pairs: ${summary.totalStats.totalPairs || 0}`
        );
        console.log(`Success: ${summary.totalStats.successPairs || 0}`);
        console.log(`Failed: ${summary.totalStats.failedPairs || 0}`);
        console.log(`Incomplete: ${summary.totalStats.incompletePairs || 0}`);

        console.log("\n================ DOMAIN STATS ================");
        summary.domainStats.forEach((domain) => {
          console.log(`\n--- ${domain._id || "unknown"} ---`);
          console.log(`Total: ${domain.totalPairs}`);
          console.log(`Success: ${domain.successPairs}`);
          console.log(`Failed: ${domain.failedPairs}`);
          console.log(`Incomplete: ${domain.incompletePairs}`);
          console.log(`HTML only: ${domain.htmlOnlyPairs}`);
          console.log(`Metadata only: ${domain.metadataOnlyPairs}`);
        });
        break;

      case "query":
        const domain = args[1];
        const status = args[2];
        const limit = parseInt(args[3] || "100", 10);

        console.log(
          `Querying processed files (domain: ${domain || "any"}, status: ${
            status || "any"
          }, limit: ${limit})`
        );
        const query = {
          limit: limit,
        };

        if (domain) query.domain = domain;
        if (status) query.status = status;

        const results = await processedFilesModel.queryProcessedFiles(query);
        console.log(
          `Found ${results.total} matching pairs (showing ${results.pairs.length})`
        );
        results.pairs.forEach((pair) => {
          console.log(`\n--- ${pair.pair_id} ---`);
          console.log(`Status: ${pair.status}`);
          console.log(`Processed: ${pair.processedAt}`);
          console.log(`Has HTML: ${!!pair.html}`);
          console.log(`Has Metadata: ${!!pair.metadata}`);
        });
        break;

      case "cron":
        console.log("Starting cron job service...");
        // Schedule a task to run every 20 minutes
        cron.schedule("*/20 * * * *", async () => {
          await processRecentArticles();
        });

        // Run immediately on startup
        await processRecentArticles();

        // Keep the process running
        console.log("Cron job service is running. Press Ctrl+C to exit.");
        // This will keep the script running
        break;

      default:
        console.log(`Unknown command: ${command}`);
        console.log("Available commands:");
        console.log(
          "  process [hours=1] [useAllPagination=false] - Process recently modified files"
        );
        console.log("  summary - Generate summary of processed files");
        console.log(
          "  query [domain] [status] [limit=100] - Query processed files"
        );
        console.log(
          "  cron - Start a service that checks for new articles every 20 minutes"
        );
    }
  } catch (error) {
    console.error("Error executing command:", error);
    process.exit(1);
  }
}

// Run the main function
if (command !== "cron") {
  // For regular commands, run and exit
  main()
    .then(() => {
      console.log("Completed successfully");
      process.exit(0);
    })
    .catch((error) => {
      console.error("Fatal error:", error);
      process.exit(1);
    });
} else {
  // For cron mode, just run without exiting
  main().catch((error) => {
    console.error("Fatal error in cron mode:", error);
    process.exit(1);
  });
}

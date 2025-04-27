require("dotenv").config();
const AWS = require("aws-sdk");
const { Readability } = require("@mozilla/readability");
const { JSDOM } = require("jsdom");
const { MongoClient } = require("mongodb");
const fs = require("fs");
const path = require("path");

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

// Tracking file for analyzed files
const ANALYZED_FILES_LOG = path.join(__dirname, "../analyzed_files.json");

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
async function connectToMongoDB() {
  try {
    console.log(`Connecting to MongoDB at ${MONGODB_URI}...`);
    const client = new MongoClient(MONGODB_URI);
    await client.connect();
    console.log("Connected to MongoDB");

    const db = client.db(DB_NAME);
    const collection = db.collection(COLLECTION_NAME);

    return { client, collection };
  } catch (error) {
    console.error("Error connecting to MongoDB:", error);
    throw error;
  }
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
 * @param {Object} analyzedFiles - Object tracking already analyzed files
 * @returns {Promise<Array>} - Array of objects with file info and content
 */
async function readHtmlAndMetadata(files, analyzedFiles) {
  const results = [];
  const newlyAnalyzedFiles = {};
  let skippedCount = 0;

  for (const file of files) {
    try {
      // Check if file is page.html or metadata.json
      if (
        file.Key.endsWith("/page.html") ||
        file.Key.endsWith("/metadata.json")
      ) {
        // Check if this file has been analyzed before and hasn't been modified
        const fileKey = file.Key;
        const lastModified = file.LastModified.toISOString();

        if (
          analyzedFiles.files[fileKey] &&
          analyzedFiles.files[fileKey] === lastModified
        ) {
          skippedCount++;
          continue; // Skip this file as it's already been analyzed
        }

        const content = await getFileContent(fileKey);

        // Mark this file as analyzed
        newlyAnalyzedFiles[fileKey] = lastModified;

        results.push({
          key: fileKey,
          type: fileKey.endsWith("/page.html") ? "html" : "metadata",
          lastModified: file.LastModified,
          content,
        });
      }
    } catch (error) {
      console.error(`Error processing file ${file.Key}:`, error);
    }
  }

  console.log(
    `Skipped ${skippedCount} previously analyzed files that haven't changed`
  );

  // Update analyzed files with new entries
  Object.assign(analyzedFiles.files, newlyAnalyzedFiles);

  return results;
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
    // Load list of already analyzed files
    const analyzedFiles = loadAnalyzedFiles();
    console.log(
      `Loaded ${
        Object.keys(analyzedFiles.files).length
      } previously analyzed files`
    );
    console.log(`Last run: ${analyzedFiles.lastRun || "Never"}`);

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

    const fileContents = await readHtmlAndMetadata(recentFiles, analyzedFiles);
    console.log(
      `Retrieved ${fileContents.length} HTML/metadata files (after skipping previously analyzed files)`
    );

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

    // Save the updated list of analyzed files
    saveAnalyzedFiles(analyzedFiles);

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

// Execute the function
// listFilesInBucket().catch(console.error);
// getRecentFiles(1).catch(console.error);
readRecentFiles(1, true).catch(console.error);

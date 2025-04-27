require("dotenv").config();
const AWS = require("aws-sdk");
const { Readability } = require("@mozilla/readability");
const { JSDOM } = require("jsdom");

// Debug: Print credential provider chain details
const credentialsObj = AWS.config.credentials;
console.log(
  "AWS SDK Credentials:",
  credentialsObj ? "Available" : "Not available"
);

const s3 = new AWS.S3();

const params = {
  Bucket: process.env.S3_BUCKET || "second-hapttic-bucket",
};

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
    const domOptions = {
      url: url && url.startsWith("http") ? url : "https://example.com",
    };

    const dom = new JSDOM(html, domOptions);

    // Create a new Readability object
    const reader = new Readability(dom.window.document);

    // Parse the content
    const article = reader.parse();

    return article;
  } catch (error) {
    console.error("Error parsing HTML with Readability:", error);
    return null;
  }
}

/**
 * Format essential metadata as a readable string
 * @param {Object} metadata - Metadata object
 * @returns {string|null} - Formatted metadata string or null if essential fields are missing
 */
function formatEssentialMetadata(metadata) {
  if (!metadata) return null;

  const formattedLines = [];

  // Define essential fields to display
  const essentialFields = {
    url: "URL",
    crawl_time: "Crawl Time",
    depth: "Depth",
  };

  // Check if any essential fields exist
  const hasEssentialFields = Object.keys(essentialFields).some(
    (key) => metadata[key] !== undefined
  );
  if (!hasEssentialFields) return null;

  // Add essential fields
  for (const [key, label] of Object.entries(essentialFields)) {
    if (metadata[key] !== undefined) {
      formattedLines.push(`${label}: ${metadata[key]}`);
    }
  }

  return formattedLines.join("\n");
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
 * @returns {Promise<Array>} - Array of objects with file info and content
 */
async function readHtmlAndMetadata(files) {
  const results = [];

  for (const file of files) {
    try {
      // Check if file is page.html or metadata.json
      if (
        file.Key.endsWith("/page.html") ||
        file.Key.endsWith("/metadata.json")
      ) {
        const content = await getFileContent(file.Key);

        results.push({
          key: file.Key,
          type: file.Key.endsWith("/page.html") ? "html" : "metadata",
          lastModified: file.LastModified,
          content,
        });
      }
    } catch (error) {
      console.error(`Error processing file ${file.Key}:`, error);
    }
  }

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
async function readRecentFiles(hours = 1, useAllPagination = false) {
  try {
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

    const fileContents = await readHtmlAndMetadata(recentFiles);
    console.log(`Retrieved ${fileContents.length} HTML/metadata files`);

    // Process each file content
    const processedContent = {};

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
        } catch (error) {
          console.error(`Error parsing metadata JSON for ${groupKey}:`, error);
          processedContent[groupKey].metadata = { error: "Invalid JSON" };
        }
      } else if (file.type === "html") {
        processedContent[groupKey].html = file.content;

        // Use Readability to parse HTML content
        const url = processedContent[groupKey].metadata?.url || "";
        processedContent[groupKey].parsed = parseHtmlWithReadability(
          file.content,
          url
        );
      }
    }

    // Display parsed content with metadata
    console.log(
      "\n\n================ ARTICLE CONTENT WITH METADATA ================"
    );
    Object.keys(processedContent).forEach((groupKey) => {
      const content = processedContent[groupKey];

      // Skip items without metadata or without parsed content
      if (!content.metadata || !content.parsed) return;

      // Format essential metadata
      const formattedMetadata = formatEssentialMetadata(content.metadata);

      // Skip if no essential metadata fields exist
      if (!formattedMetadata) return;

      console.log(`\n=== ${content.domain}/${content.hash} ===`);

      // Article title and excerpt
      console.log(`Title: ${content.parsed.title}`);
      console.log(
        `Excerpt: ${content.parsed.excerpt || "No excerpt available"}`
      );
      console.log(`Length: ${content.parsed.textContent.length} characters`);

      // Article content (first part)
      console.log(
        `\nCONTENT:\n${content.parsed.textContent.substring(0, 300)}...`
      );

      // Essential metadata information
      console.log(`\nMETADATA:\n${formattedMetadata}`);

      // Separator for readability
      console.log("\n" + "=".repeat(80));
    });

    // Print summary of broken links
    if (brokenLinks.length > 0) {
      console.log("\n\n================ BROKEN LINKS SUMMARY ================");
      brokenLinks.forEach((group) => {
        console.log(`- ${group.domain}/${group.hash}`);
      });
    }

    return {
      recentFiles,
      fileContents,
      groupedByHash,
      brokenLinks,
      completeLinks,
      processedContent,
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

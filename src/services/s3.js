const AWS = require("aws-sdk");
const config = require("../config");

// Initialize S3 client
const s3 = new AWS.S3();

// Base parameters for S3 operations
const baseParams = {
  Bucket: config.s3.bucket,
};

/**
 * Lists all files in bucket using pagination
 * @param {Object} options - Options for listing objects
 * @returns {Promise<Array>} - All files from the bucket
 */
async function listAllFiles(options = {}) {
  const allFiles = [];
  let continuationToken = null;
  let pageCount = 0;

  do {
    try {
      // Prepare parameters for the next request
      const listParams = {
        ...baseParams,
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

/**
 * List files in the S3 bucket (single page)
 * @returns {Promise<Array>} - Files in the bucket (first page)
 */
async function listFiles() {
  try {
    const data = await s3.listObjectsV2(baseParams).promise();

    console.log(`Found ${data.KeyCount} files in bucket`);

    if (data.IsTruncated) {
      console.log(
        "More files exist but were not listed due to pagination limits"
      );
    }

    return data.Contents;
  } catch (error) {
    console.error("Error listing files from S3 bucket:", error);
    throw error;
  }
}

/**
 * Get the content of a file from S3
 * @param {string} key - S3 object key
 * @returns {Promise<string>} - File content as string
 */
async function getFileContent(key) {
  try {
    const fileParams = {
      Bucket: baseParams.Bucket,
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
 * Filters S3 files that were modified after the specified hours
 * @param {Array} files - Array of S3 objects
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
 * Get files modified in the last specified hours
 * @param {Number} hours - Hours to look back
 * @param {Boolean} useAllPagination - Whether to use pagination to get all files
 * @returns {Promise<Array>} - Recent files
 */
async function getRecentFiles(hours = 24, useAllPagination = false) {
  try {
    let allFiles;

    if (useAllPagination) {
      // Get all files using pagination
      allFiles = await listAllFiles();
    } else {
      // Get just the first page of files
      allFiles = await listFiles();
    }

    const recentFiles = getFilesModifiedAfter(allFiles, hours);

    console.log(
      `Found ${recentFiles.length} files modified in the last ${hours} hours`
    );

    return recentFiles;
  } catch (error) {
    console.error("Error getting recent files:", error);
    throw error;
  }
}

module.exports = {
  listAllFiles,
  listFiles,
  getFileContent,
  getFilesModifiedAfter,
  getRecentFiles,
};

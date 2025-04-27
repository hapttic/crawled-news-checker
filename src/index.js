require("dotenv").config();
const AWS = require("aws-sdk");

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
async function getRecentFiles(hours = 24) {
  try {
    const allFiles = await listFilesInBucket();
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

// Execute the function
// listFilesInBucket().catch(console.error);
getRecentFiles(1).catch(console.error);

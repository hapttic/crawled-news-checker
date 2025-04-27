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

// Execute the function
listFilesInBucket().catch(console.error);

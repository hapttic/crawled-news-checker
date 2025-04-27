require("dotenv").config();

module.exports = {
  // MongoDB configuration
  mongodb: {
    uri: process.env.MONGODB_URI || "mongodb://localhost:27017",
    dbName: process.env.DB_NAME || "crawled_news",
    collections: {
      articles: process.env.COLLECTION_NAME || "crawled_articles",
      processedFiles: "crawled_processed_files",
    },
  },

  // AWS S3 configuration
  s3: {
    bucket: process.env.S3_BUCKET || "second-hapttic-bucket",
    region: process.env.AWS_REGION,
  },

  // Readability configuration
  readability: {
    minContentLength: 100, // Minimum length for meaningful content
    defaultUrl: "https://example.com",
  },

  // File processing configuration
  files: {
    htmlFileName: "page.html",
    metadataFileName: "metadata.json",
  },
};

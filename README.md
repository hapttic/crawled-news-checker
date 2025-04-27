# Crawled News Checker

A Node.js application for extracting and analyzing HTML and metadata from crawled news articles stored in S3, parsing the content with Readability, and storing structured results in MongoDB.

## Features

- Retrieves HTML and metadata files from S3 bucket
- Processes recent files based on modification time
- Extracts article content using Mozilla's Readability library
- Detects broken links (HTML without metadata)
- Validates metadata URLs and other essential fields
- Stores processed article data in MongoDB
- Tracks processed files to avoid redundant processing
- Command-line interface for different operations

## Prerequisites

- Node.js 14+
- MongoDB database
- AWS S3 bucket with crawled news articles
- AWS credentials configured for S3 access

## Installation

1. Clone the repository
2. Install dependencies:

```bash
npm install
```

3. Create a `.env` file with your configuration:

```env
# AWS Configuration
AWS_REGION=us-east-1
S3_BUCKET=your-s3-bucket-name

# MongoDB Configuration
MONGODB_URI=mongodb://localhost:27017
DB_NAME=crawled_news
COLLECTION_NAME=crawled_articles
```

## Usage

### Process Recent Files

Process files modified in the last 1 hour:

```bash
npm run process
# or
node src/index.js process
```

Process files modified in the last X hours:

```bash
node src/index.js process 24
```

Process files with full pagination (get all files, may be slow):

```bash
npm run process:all
# or
node src/index.js process 24 true
```

### Generate Summary

View summary statistics of processed files:

```bash
npm run summary
# or
node src/index.js summary
```

### Query Processed Files

Query processed files by domain, status, or other criteria:

```bash
# Query all processed files (limited to 100)
npm run query

# Query by domain
node src/index.js query example.com

# Query by domain and status
node src/index.js query example.com success

# Query with limit
node src/index.js query example.com success 200
```

## Project Structure

```
src/
├── config/          # Configuration settings
├── controllers/     # Workflow orchestration
├── models/          # Data models & database operations
├── services/        # Service implementations
│   ├── database.js  # MongoDB connection
│   ├── fileManager.js # File processing
│   ├── fileProcessor.js # File organization
│   ├── parser.js    # Content parsing (Readability)
│   └── s3.js        # S3 operations
└── index.js         # Entry point
```

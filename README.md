# AWS S3 File Operations

Simple Node.js application to work with AWS S3 buckets.

## Setup

1. Install dependencies:

   ```
   npm install
   ```

2. Configure AWS credentials using one of these methods:

   - Environment variables:
     ```
     export AWS_ACCESS_KEY_ID=your_access_key
     export AWS_SECRET_ACCESS_KEY=your_secret_key
     ```
   - AWS credentials file (`~/.aws/credentials`):
     ```
     [default]
     aws_access_key_id = your_access_key
     aws_secret_access_key = your_secret_key
     ```

3. Update the configuration in `src/index.js`:
   - Set your bucket's region
   - Set your bucket name
   - Optionally configure listing parameters:
     - `MaxKeys`: Maximum number of files to list (default: 1000)
     - `Prefix`: List only files with this prefix/in this folder

## Run

```
node src/index.js
```

## Features

1. **List all files in a bucket**: The application will display:
   - Total number of files found
   - File names with sizes and last modified dates
   - Indication if more files exist (pagination)

## Notes

This application uses AWS SDK v2. For production, you might want to consider using AWS SDK v3 for better modularity.

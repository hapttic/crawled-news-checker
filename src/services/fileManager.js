const s3Service = require("./s3");
const config = require("../config");

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
        file.Key.endsWith(`/${config.files.htmlFileName}`) ||
        file.Key.endsWith(`/${config.files.metadataFileName}`)
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
          const content = await s3Service.getFileContent(fileKey);

          // Mark this file as processed
          newlyProcessedFiles[fileKey] = lastModified;

          results.push({
            key: fileKey,
            type: fileKey.endsWith(`/${config.files.htmlFileName}`)
              ? "html"
              : "metadata",
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

module.exports = {
  readHtmlAndMetadata,
};

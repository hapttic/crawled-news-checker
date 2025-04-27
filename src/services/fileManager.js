const s3Service = require("./s3");
const config = require("../config");
const cliProgress = require("cli-progress");
const colors = require("colors");

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

  // Filter files to only those we need to process (HTML and metadata)
  const filesToProcess = files.filter(
    (file) =>
      file.Key.endsWith(`/${config.files.htmlFileName}`) ||
      file.Key.endsWith(`/${config.files.metadataFileName}`)
  );

  // Count how many files we'll actually process (exclude already processed)
  const filesToDownload = filesToProcess.filter((file) => {
    const fileKey = file.Key;
    const lastModified = file.LastModified.toISOString();
    return !(
      processedFiles[fileKey] && processedFiles[fileKey] === lastModified
    );
  });

  // Create a new progress bar instance
  const progressBar = new cliProgress.SingleBar({
    format:
      colors.cyan("Downloading files |") +
      "{bar}" +
      colors.cyan("| {percentage}% || {value}/{total} files || ETA: {eta}s"),
    barCompleteChar: "\u2588",
    barIncompleteChar: "\u2591",
    hideCursor: true,
  });

  if (filesToDownload.length > 0) {
    console.log(`\nDownloading ${filesToDownload.length} files...`);
    progressBar.start(filesToDownload.length, 0);
  }

  let processedCount = 0;

  for (const file of filesToProcess) {
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

        // Update progress bar
        processedCount++;
        if (filesToDownload.length > 0) {
          progressBar.update(processedCount);
        }
      }
    } catch (error) {
      console.error(`Error processing file ${file.Key}:`, error);
    }
  }

  // Stop the progress bar
  if (filesToDownload.length > 0) {
    progressBar.stop();
  }

  console.log(
    `\nSkipped ${skippedCount} previously processed files that haven't changed`
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

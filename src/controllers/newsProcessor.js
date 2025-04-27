const s3Service = require("../services/s3");
const fileManager = require("../services/fileManager");
const fileProcessor = require("../services/fileProcessor");
const parser = require("../services/parser");
const articlesModel = require("../models/articles");
const processedFilesModel = require("../models/processedFiles");

/**
 * Process and analyze HTML and metadata files
 * @param {Number} hours - Number of hours to look back for modified files
 * @param {Boolean} useAllPagination - Whether to use pagination for all files
 * @param {Boolean} saveToMongoDB - Whether to save results to MongoDB
 * @returns {Promise<Object>} - Processing results
 */
async function processRecentFiles(
  hours = 1,
  useAllPagination = false,
  saveToMongoDB = true
) {
  try {
    // Get list of already processed files from database
    const processedFiles = await processedFilesModel.getProcessedFiles();
    console.log(
      `Loaded ${
        Object.keys(processedFiles).length
      } previously processed files from database`
    );

    // Get recent files from S3
    const recentFiles = await s3Service.getRecentFiles(hours, useAllPagination);
    console.log(
      `Processing ${recentFiles.length} files for HTML and metadata...`
    );

    // Group files by hash directory
    let groupedByHash = fileProcessor.groupFilesByHash(recentFiles);

    // Identify broken links (missing metadata)
    groupedByHash = fileProcessor.identifyBrokenLinks(groupedByHash);

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

    // Read HTML and metadata files, skipping previously processed ones
    const htmlAndMetadata = await fileManager.readHtmlAndMetadata(
      recentFiles,
      processedFiles
    );
    const fileContents = htmlAndMetadata.results;
    const newlyProcessedFiles = htmlAndMetadata.newlyProcessedFiles;
    const processingResults = htmlAndMetadata.processingResults;

    console.log(
      `Retrieved ${fileContents.length} HTML/metadata files (after skipping previously processed files)`
    );

    // Summarize processing results
    const successCount = Object.values(processingResults).filter(
      (r) => r.success
    ).length;
    const failureCount = Object.values(processingResults).filter(
      (r) => !r.success
    ).length;
    const totalProcessingTime = Object.values(processingResults).reduce(
      (sum, r) => sum + r.processingTimeMs,
      0
    );
    const totalProcessedSize = Object.values(processingResults).reduce(
      (sum, r) => sum + (r.fileSize || 0),
      0
    );

    console.log("\n\n================ PROCESSING SUMMARY ================");
    console.log(
      `Total files processed: ${Object.keys(processingResults).length}`
    );
    console.log(`Successful: ${successCount}, Failed: ${failureCount}`);
    console.log(`Total processing time: ${totalProcessingTime}ms`);
    console.log(
      `Total data processed: ${(totalProcessedSize / 1024 / 1024).toFixed(2)}MB`
    );

    if (failureCount > 0) {
      console.log("\n----- PROCESSING FAILURES -----");
      Object.entries(processingResults)
        .filter(([_, result]) => !result.success)
        .forEach(([key, result]) => {
          console.log(`- ${key}: ${result.error}`);
        });
    }

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
          if (!parser.hasValidUrl(processedContent[groupKey].metadata)) {
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
        const parsedArticle = parser.parseHtml(file.content, url);
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
      const article = fileProcessor.createArticleObject(content);

      if (article) {
        articleCandidates.push(article);
      }
    });

    // Check which articles already exist in the database
    const articleIds = articleCandidates.map((article) => article.id);
    const existingArticles = saveToMongoDB
      ? await articlesModel.checkExistingArticles(articleIds)
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
      mongoResult = await articlesModel.saveArticles(newArticles);
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

    // Save the newly processed files to database
    if (Object.keys(newlyProcessedFiles).length > 0) {
      await processedFilesModel.saveProcessedFiles(
        newlyProcessedFiles,
        processingResults
      );
    }

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
    console.error("Error processing recent files:", error);
    throw error;
  }
}

module.exports = {
  processRecentFiles,
};

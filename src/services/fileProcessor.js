const config = require("../config");

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
    const hasHtml = filenames.includes(config.files.htmlFileName);
    const hasMetadata = filenames.includes(config.files.metadataFileName);

    group.isBroken = hasHtml && !hasMetadata;
    group.isComplete = hasHtml && hasMetadata;
  });

  return groupedFiles;
}

/**
 * Determine overall status for a file pair
 * @param {Object} pair - File pair object
 * @returns {string} - Overall status
 */
function getOverallStatus(pair) {
  if (!pair.files.html && !pair.files.metadata) {
    return "unknown";
  }

  const html = pair.files.html || { status: "missing" };
  const metadata = pair.files.metadata || { status: "missing" };

  if (html.status === "failed" || metadata.status === "failed") {
    return "failed";
  }

  if (html.status === "missing" || metadata.status === "missing") {
    return "incomplete";
  }

  return "success";
}

/**
 * Creates an article object with essential data
 * @param {Object} data - Article data including metadata and parsed content
 * @returns {Object|null} - Formatted article object or null if invalid
 */
function createArticleObject(data) {
  if (!data.metadata || !data.parsed) return null;

  // Extract essential metadata from parser service
  const metadata = require("./parser").extractMetadata(data.metadata);
  if (!metadata) return null;

  // Create article object
  return {
    id: `${data.domain}/${data.hash}`,
    domain: data.domain,
    hash: data.hash,
    title: data.parsed.title,
    excerpt: data.parsed.excerpt || "",
    content: data.parsed.textContent,
    contentLength: data.parsed.textContent.length,
    isPotentiallyEmpty: data.parsed.isPotentiallyEmpty || false,
    url: metadata.url || "",
    crawl_time: metadata.crawl_time || "",
    crawl_datetime: metadata.crawl_datetime || null,
    depth: metadata.depth || "",
  };
}

module.exports = {
  groupFilesByHash,
  identifyBrokenLinks,
  getOverallStatus,
  createArticleObject,
};

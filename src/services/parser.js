const { Readability } = require("@mozilla/readability");
const { JSDOM } = require("jsdom");
const config = require("../config");

/**
 * Parse HTML content using Readability
 * @param {string} html - HTML content
 * @param {string} url - URL of the page (optional)
 * @returns {Object} - Parsed article data
 */
function parseHtml(html, url = "") {
  try {
    // Create a DOM object from the HTML content
    // Use default URL if none provided or if URL is invalid
    const domOptions = {
      url: url && url.startsWith("http") ? url : config.readability.defaultUrl,
    };

    const dom = new JSDOM(html, domOptions);

    // Create a new Readability object
    const reader = new Readability(dom.window.document);

    // Parse the content
    const article = reader.parse();

    // Check if article content is meaningful (not just boilerplate text)
    if (article && article.textContent) {
      const textLength = article.textContent.trim().length;
      if (textLength < config.readability.minContentLength) {
        console.log(
          `Warning: Very short article content (${textLength} chars) for URL: ${url}`
        );
        // Still return the article, but flag it as potentially problematic
        article.isPotentiallyEmpty = true;
      }
    }

    return article;
  } catch (error) {
    console.error("Error parsing HTML with Readability:", error);
    return null;
  }
}

/**
 * Extract essential metadata fields
 * @param {Object} metadata - Metadata object
 * @returns {Object|null} - Object with essential metadata or null if missing
 */
function extractMetadata(metadata) {
  if (!metadata) return null;

  const essentialFields = ["url", "crawl_time", "depth"];
  const extracted = {};

  // Check if any essential fields exist
  const hasEssentialFields = essentialFields.some(
    (key) => metadata[key] !== undefined
  );
  if (!hasEssentialFields) return null;

  // Copy essential fields
  essentialFields.forEach((field) => {
    if (metadata[field] !== undefined) {
      extracted[field] = metadata[field];
    }
  });

  // Parse crawl_time to create crawl_datetime if possible
  if (extracted.crawl_time) {
    try {
      extracted.crawl_datetime = new Date(extracted.crawl_time);
    } catch (error) {
      console.error(`Error parsing crawl_time: ${extracted.crawl_time}`, error);
    }
  }

  return extracted;
}

/**
 * Check if metadata has valid URL
 * @param {Object} metadata - Metadata object
 * @returns {boolean} - Whether the metadata has a valid URL
 */
function hasValidUrl(metadata) {
  if (!metadata) return false;

  // Check if url exists and is a valid URL
  if (!metadata.url || typeof metadata.url !== "string") return false;

  try {
    new URL(metadata.url); // This will throw if URL is invalid
    return true;
  } catch (error) {
    return false;
  }
}

module.exports = {
  parseHtml,
  extractMetadata,
  hasValidUrl,
};

const express = require('express');
const axios = require('axios');
const moment = require('moment'); // For easy date/time manipulation
const { mean, std, subtract, multiply, sum, sqrt } = require('mathjs'); // For statistical calculations
const app = express();

// --- CONFIGURATION ---
// !!! IMPORTANT: REPLACE THESE WITH YOUR ACTUAL, VALID CREDENTIALS !!!
// !!! Ensure your ACCESS_TOKEN is FRESH (obtained from Phase 2 and NOT EXPIRED) !!!
// If you're getting "An authorization header is required", this is the most likely culprit.
const CLIENT_ID = "8fa25491-2d20-43b5-a897-1b748ebd3e40";
const CLIENT_SECRET = "wcCsGwekkWknyJTT";
const ACCESS_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJNYXBDbGFpbXMiOnsiZXhwIjoxNzQ4MDcxMTc4LCJpYXQiOjE3NDgwNzA4NzgsImlzcyI6IkFmZm9yZG1lZCIsImp0aSI6IjhmYTI1NDkxLTJkMjAtNDNiNS1hODk3LTFiNzQ4ZWJkM2U0MCIsInN1YiI6ImhhcmlzaGxpZmU1NUBnbWFpbC5jb20ifSwiZW1haWwiOiJoYXJpc2hsaWZlNTVAZ21haWwuY29tIiwibmFtZSI6ImhhcmlzaCBrIiwicm9sbE5vIjoiOTI3NjIyYmFsMDE0IiwiYWNjZXNzQ29kZSI6IndoZVFVeSIsImNsaWVudElEIjoiOGZhMjU0OTEtMmQyMC00M2I1LWE4OTctMWI3NDhlYmQzZTQwIiwiY2xpZW50U2VjcmV0Ijoid2NDc0d3ZWtrV2tueUpUVCJ9.Hffl255qMGohMCaEYS5v2VuonSczRySloSXACueIP6g"; // Get a fresh one if the old one expired!
const TOKEN_TYPE = "Bearer"; // Usually "Bearer"

// CONFIRMED BASE URL for the Third-Party Stock Exchange API (from your provided documentation)
const THIRD_PARTY_STOCK_API_BASE_URL = "http://20.244.56.144/evaluation-service";

// CONFIRMED Specific endpoint for fetching stock price history (from your provided documentation)
// Used as `${THIRD_PARTY_STOCK_API_BASE_URL}${STOCK_HISTORY_ENDPOINT}/:ticker`
const STOCK_HISTORY_ENDPOINT = "/stocks"; 

const PORT = 9876; // Your microservice's port

// Increased timeout for calls to the third-party stock API (from 300ms to 450ms)
// This addresses the "timeout of 300ms exceeded" error you faced.
const API_CALL_TIMEOUT_MS = 450; 

const MAX_CORRELATION_TICKERS = 2; // As per requirement: "More than 2 tickers shouldn't be supported"

// --- IN-MEMORY CACHE FOR STOCK PRICES ---
// This cache stores recent price history to reduce API calls and improve performance/cost.
// Structure: Map<ticker: string, {price: number, lastUpdatedAt: Date}[]>
const stockPriceCache = new Map();

// Helper to remove old data from cache periodically to prevent infinite growth.
// It keeps data that falls within `minutesToKeep` from the current time.
function cleanCache(minutesToKeep) {
    const cutoffTime = moment().subtract(minutesToKeep, 'minutes');
    stockPriceCache.forEach((history, ticker) => {
        const filteredHistory = history.filter(item => moment(item.lastUpdatedAt).isAfter(cutoffTime));
        if (filteredHistory.length === 0) {
            stockPriceCache.delete(ticker); // Remove ticker from cache if no recent data
        } else {
            stockPriceCache.set(ticker, filteredHistory);
        }
    });
    // console.log("Cache cleaned. Current tickers in cache:", [...stockPriceCache.keys()]);
}

// Periodically clean the cache (e.g., every 10 minutes, keeping data up to 60 minutes old)
setInterval(() => cleanCache(60), 10 * 60 * 1000); // Clean every 10 minutes (600,000 ms)

// --- HELPER FUNCTION: Fetch Price History from Third-Party API ---
// This function makes the actual authenticated API call to the external stock exchange server
// and updates the local cache with the fetched data.
async function fetchPriceHistoryFromAPI(ticker, minutes) {
    // Construct the URL based on the CONFIRMED documentation:
    // Example: http://20.244.56.144/evaluation-service/stocks/NVDA?minutes=50
    const url = `${THIRD_PARTY_STOCK_API_BASE_URL}${STOCK_HISTORY_ENDPOINT}/${ticker}`;
    const headers = {
        "Authorization": `${TOKEN_TYPE} ${ACCESS_TOKEN}`, // Authorization header is crucial
        "Content-Type": "application/json"
    };

    try {
        // According to documentation, using `?minutes=m` parameter yields an array response directly.
        const response = await axios.get(url, {
            headers: headers,
            params: { minutes: minutes }, // Send 'minutes' as a query parameter as confirmed
            timeout: API_CALL_TIMEOUT_MS // Use the configured timeout
        });

        // CONFIRMED: Response is directly an array when `minutes` parameter is used (e.g., `response.data` IS the array).
        if (response.data && Array.isArray(response.data)) {
            const newHistory = response.data.map(item => ({
                price: parseFloat(item.price), // Ensure price is a number
                lastUpdatedAt: new Date(item.lastUpdatedAt) // Parse timestamp string to Date object
            })).sort((a, b) => a.lastUpdatedAt.getTime() - b.lastUpdatedAt.getTime()); // Sort chronologically

            // Update cache: Add new unique entries received in this fetch.
            let currentHistory = stockPriceCache.get(ticker) || [];
            newHistory.forEach(newItem => {
                // Add only if this exact timestamp is not already in the cache for this ticker.
                if (!currentHistory.some(existingItem => existingItem.lastUpdatedAt.getTime() === newItem.lastUpdatedAt.getTime())) {
                    currentHistory.push(newItem);
                }
            });
            // Re-sort the combined history to maintain chronological order in cache.
            currentHistory.sort((a, b) => a.lastUpdatedAt.getTime() - b.lastUpdatedAt.getTime());
            stockPriceCache.set(ticker, currentHistory);

            return newHistory; // Return the newly fetched (and potentially combined in cache) data
        }
        return []; // Return empty array if response is not valid or empty
    } catch (error) {
        // Consolidated error logging for third-party API calls.
        if (axios.isCancel(error) || (error.code === 'ECONNABORTED' && error.message.includes('timeout'))) {
            console.warn(`Warning: 3rd party API call for '${ticker}' timed out (> ${API_CALL_TIMEOUT_MS}ms). Ignoring response.`);
        } else if (error.response) {
            // Log 401 Unauthorized specifically as it's a common issue (expired token)
            if (error.response.status === 401) {
                console.error(`Error fetching history for '${ticker}': Status 401 Unauthorized. Message: "${error.response.data.message || 'No message provided'}" - Your ACCESS_TOKEN is likely expired or invalid.`);
            } else {
                console.error(`Error fetching history for '${ticker}': Status ${error.response.status}, Data: ${JSON.stringify(error.response.data)}. Ignoring response.`);
            }
        } else if (error.request) {
            console.error(`Error fetching history for '${ticker}': No response received (network error or server down). ${error.message}. Ignoring response.`);
        } else {
            console.error(`Error in setting up 3rd party request for '${ticker}': ${error.message}. Ignoring response.`);
        }
        return [];
    }
}

// Helper to get price history for a given ticker within the specified 'minutes' window.
// It prioritizes cached data but fetches from API if cache is insufficient or explicitly outdated.
async function getPriceHistoryForMinutes(ticker, minutes) {
    const cutoffTime = moment().subtract(minutes, 'minutes');
    
    // Attempt to retrieve and filter data from the local cache.
    let historyFromCache = stockPriceCache.get(ticker) || [];
    const relevantHistory = historyFromCache.filter(item => moment(item.lastUpdatedAt).isAfter(cutoffTime));

    // Heuristic: If cache is empty or has very few recent points, or if the oldest cached point
    // is outside the requested 'minutes' window, try fetching from the external API to refresh.
    if (relevantHistory.length === 0 || 
        (relevantHistory.length < 5 && historyFromCache.length < 100) || // Small cache or overall small history
        (historyFromCache.length > 0 && moment(historyFromCache[0].lastUpdatedAt).isBefore(cutoffTime))
    ) { 
        // Fetch new data from API and update cache.
        const fetchedHistory = await fetchPriceHistoryFromAPI(ticker, minutes);
        // Return only the data within the requested 'minutes' from the potentially combined history.
        return fetchedHistory.filter(item => moment(item.lastUpdatedAt).isAfter(cutoffTime)); 
    }

    return relevantHistory; // Return data directly from cache if sufficient and recent enough
}


// --- API ENDPOINT: Average Stock Price in the last "m" minutes ---
// Method: GET
// Route: http://hostname/stocks/:ticker?minutes=m&aggregation=average
app.get('/stocks/:ticker', async (req, res) => {
    const ticker = req.params.ticker.toUpperCase(); // Standardize ticker to uppercase
    const minutes = parseInt(req.query.minutes);
    const aggregation = req.query.aggregation; // Should be 'average' for this endpoint

    // Input validation
    if (isNaN(minutes) || minutes <= 0) {
        return res.status(400).json({ error: "Invalid 'minutes' parameter. Must be a positive number." });
    }
    if (aggregation !== 'average') {
        return res.status(400).json({ error: "Only 'average' aggregation is supported for this endpoint." });
    }

    // Get price history (from cache or new API fetch)
    const priceHistory = await getPriceHistoryForMinutes(ticker, minutes);

    let averagePrice = 0;
    if (priceHistory.length > 0) {
        const prices = priceHistory.map(item => item.price);
        averagePrice = mean(prices); // Calculate mean using mathjs
    } else {
        // If no data is available after fetch, return 404 for average too
        return res.status(404).json({ error: `No price history available for ${ticker} in the last ${minutes} minutes.` });
    }

    // Prepare response based on specified JSON format.
    const responseData = [{
        averageStockPrice: parseFloat(averagePrice.toFixed(6)), // Format to 6 decimal places
        priceHistory: priceHistory.map(item => ({
            price: parseFloat(item.price.toFixed(6)), // Format price in history to 6 decimal places
            lastUpdatedAt: item.lastUpdatedAt.toISOString() // Convert Date object back to ISO string
        }))
    }];

    res.json(responseData);
});

// --- API ENDPOINT: Correlation of Price Movement between 2 stocks ---
// Method: GET
// Route: http://hostname/stock/correlation?minutes=m&ticker={NVDA}&ticker={PYPL}
app.get('/stock/correlation', async (req, res) => {
    const minutes = parseInt(req.query.minutes);
    const tickers = req.query.ticker; // This will be an array if multiple 'ticker' params are sent

    // Input validation
    if (isNaN(minutes) || minutes <= 0) {
        return res.status(400).json({ error: "Invalid 'minutes' parameter. Must be a positive number." });
    }

    let tickerList = [];
    if (typeof tickers === 'string') {
        tickerList.push(tickers.toUpperCase());
    } else if (Array.isArray(tickers)) {
        tickerList = tickers.map(t => t.toUpperCase());
    } else {
        return res.status(400).json({ error: "At least one 'ticker' parameter is required for correlation." });
    }

    // Requirement: "More than 2 tickers shouldn't be supported for correlation simultaneously."
    if (tickerList.length !== MAX_CORRELATION_TICKERS) {
        return res.status(400).json({ error: `Correlation endpoint supports exactly ${MAX_CORRELATION_TICKERS} tickers.` });
    }

    const [ticker1, ticker2] = tickerList; // Destructure the two tickers

    // Fetch price history for both tickers from cache/API concurrently for efficiency.
    const [history1, history2] = await Promise.all([
        getPriceHistoryForMinutes(ticker1, minutes),
        getPriceHistoryForMinutes(ticker2, minutes)
    ]);

    // --- TEMPORARY DEBUGGING LOGS (You can remove these after testing) ---
    console.log(`Correlation Debug: History for ${ticker1} has ${history1.length} data points.`);
    console.log(`Correlation Debug: History for ${ticker2} has ${history2.length} data points.`);
    // END TEMPORARY DEBUGGING LOGS ---

    if (history1.length === 0 || history2.length === 0) {
        return res.status(404).json({ error: "Not enough data available for one or both tickers in the specified time frame." });
    }

    // Align data points by common timestamps for correlation calculation.
    // This addresses "time alignment of chosen tickers" requirement.
    const alignedPrices = alignPriceHistories(history1, history2);

    if (alignedPrices.length < 2) { // Need at least 2 common data points for correlation calculation
        console.warn(`Correlation: Not enough overlapping data points for ${ticker1} and ${ticker2} within ${minutes} minutes. Aligned points: ${alignedPrices.length}`);
        return res.status(404).json({ error: "Not enough overlapping data points for correlation calculation within the specified minutes." });
    }

    const prices1 = alignedPrices.map(p => p[0]); // Prices for ticker1 from aligned data
    const prices2 = alignedPrices.map(p => p[1]); // Prices for ticker2 from aligned data

    const correlation = calculatePearsonCorrelation(prices1, prices2);

    // Prepare response data for correlation.
    // The example response only showed one stock's details, but for clarity and usefulness,
    // this response includes details for both correlated stocks.
    const responseData = {
        correlation: parseFloat(correlation.toFixed(4)), // Correlation coefficient usually rounded to 4 decimals
        stocks: {
            [ticker1]: {
                averagePrice: parseFloat(mean(prices1).toFixed(6)), // Average of the aligned prices
                priceHistory: history1.map(item => ({ // Full history (within 'minutes') for the ticker
                    price: parseFloat(item.price.toFixed(6)),
                    lastUpdatedAt: item.lastUpdatedAt.toISOString()
                }))
            },
            [ticker2]: {
                averagePrice: parseFloat(mean(prices2).toFixed(6)),
                priceHistory: history2.map(item => ({ // Full history (within 'minutes') for the ticker
                    price: parseFloat(item.price.toFixed(6)),
                    lastUpdatedAt: item.lastUpdatedAt.toISOString()
                }))
            }
        }
    };

    res.json(responseData);
});

// --- HELPER FUNCTIONS FOR STATISTICAL CALCULATIONS ---

/**
 * Aligns two price histories by finding exact common timestamps.
 * This is crucial for "time alignment of chosen tickers" for accurate correlation.
 * @param {Array<{price: number, lastUpdatedAt: Date}>} history1 Price history for stock 1.
 * @param {Array<{price: number, lastUpdatedAt: Date}>} history2 Price history for stock 2.
 * @returns {Array<[number, number]>} An array of [price1, price2] pairs for common timestamps.
 */
function alignPriceHistories(history1, history2) {
    const alignedData = [];
    // Use a Map for faster lookup of prices by timestamp in the second history.
    const map2 = new Map(history2.map(item => [item.lastUpdatedAt.getTime(), item.price]));

    history1.forEach(item1 => {
        const time1 = item1.lastUpdatedAt.getTime();
        if (map2.has(time1)) {
            // If there's an exact timestamp match, add the price pair.
            alignedData.push([item1.price, map2.get(time1)]);
        }
    });
    return alignedData;
}

/**
 * Calculates Pearson's Correlation Coefficient between two sets of data (X and Y).
 * Formula: ρ = cov(X,Y) / (σX * σY)
 * @param {Array<number>} X Array of numerical data points for variable X.
 * @param {Array<number>} Y Array of numerical data points for variable Y.
 * @returns {number} The Pearson correlation coefficient (between -1 and 1), or 0 if correlation cannot be calculated.
 */
function calculatePearsonCorrelation(X, Y) {
    if (X.length !== Y.length || X.length < 2) {
        // Need at least two data points and same number of points for both arrays.
        console.warn("Insufficient or mismatched data points for correlation calculation.");
        return 0; 
    }

    const n = X.length;
    const meanX = mean(X); // Calculate mean of X using mathjs
    const meanY = mean(Y); // Calculate mean of Y using mathjs

    // Calculate Covariance: cov(X,Y) = (1/(n-1)) * Sum((Xi - X_bar)(Yi - Y_bar))
    let covarianceSum = 0;
    for (let i = 0; i < n; i++) {
        covarianceSum += (X[i] - meanX) * (Y[i] - meanY);
    }
    const covariance = covarianceSum / (n - 1);

    // Calculate Sample Standard Deviations: σX = sqrt( (1/(n-1)) * Sum((Xi - X_bar)^2) )
    // The problem's formula for standard deviation uses n-1 in the denominator (sample standard deviation).
    let sumSqDevX = 0;
    let sumSqDevY = 0;
    for (let i = 0; i < n; i++) {
        sumSqDevX += (X[i] - meanX) ** 2;
        sumSqDevY += (Y[i] - meanY) ** 2;
    }
    const sampleStdDevX = Math.sqrt(sumSqDevX / (n - 1));
    const sampleStdDevY = Math.sqrt(sumSqDevY / (n - 1));

    // If either standard deviation is zero (meaning no variance in data), correlation is undefined/0.
    if (sampleStdDevX === 0 || sampleStdDevY === 0) {
        return 0;
    }

    // Calculate Pearson's Correlation Coefficient: ρ = cov(X,Y) / (σX * σY)
    return covariance / (sampleStdDevX * sampleStdDevY);
}


// --- START THE EXPRESS SERVER ---
app.listen(PORT, () => {
    console.log(`Stock Insights Microservice listening on port ${PORT}`);
    console.log(`Access Average Price at: http://localhost:${PORT}/stocks/:ticker?minutes=m&aggregation=average`);
    console.log(`Access Correlation at: http://localhost:${PORT}/stock/correlation?minutes=m&ticker={TICKER1}&ticker={TICKER2}`);
    console.log('\n--- IMPORTANT ---');
    console.log('1. Ensure CLIENT_ID, CLIENT_SECRET, ACCESS_TOKEN are replaced with your actual values!');
    console.log('2. Ensure your ACCESS_TOKEN is not expired. Obtain a new one from Phase 2 if needed.');
    console.log('3. Remember to run `npm install express axios moment mathjs` to get all dependencies.');
});
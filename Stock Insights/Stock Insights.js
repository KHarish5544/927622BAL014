const express = require('express');
const axios = require('axios');
const moment = require('moment'); 
const { mean, std, subtract, multiply, sum, sqrt } = require('mathjs');
const app = express();
const CLIENT_ID = "8fa25491-2d20-43b5-a897-1b748ebd3e40";
const CLIENT_SECRET = "wcCsGwekkWknyJTT";
const ACCESS_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJNYXBDbGFpbXMiOnsiZXhwIjoxNzQ4MDcxMTc4LCJpYXQiOjE3NDgwNzA4NzgsImlzcyI6IkFmZm9yZG1lZCIsImp0aSI6IjhmYTI1NDkxLTJkMjAtNDNiNS1hODk3LTFiNzQ4ZWJkM2U0MCIsInN1YiI6ImhhcmlzaGxpZmU1NUBnbWFpbC5jb20ifSwiZW1haWwiOiJoYXJpc2hsaWZlNTVAZ21haWwuY29tIiwibmFtZSI6ImhhcmlzaCBrIiwicm9sbE5vIjoiOTI3NjIyYmFsMDE0IiwiYWNjZXNzQ29kZSI6IndoZVFVeSIsImNsaWVudElEIjoiOGZhMjU0OTEtMmQyMC00M2I1LWE4OTctMWI3NDhlYmQzZTQwIiwiY2xpZW50U2VjcmV0Ijoid2NDc0d3ZWtrV2tueUpUVCJ9.Hffl255qMGohMCaEYS5v2VuonSczRySloSXACueIP6g"; // Get a fresh one if the old one expired!
const TOKEN_TYPE = "Bearer"; 


const THIRD_PARTY_STOCK_API_BASE_URL = "http://20.244.56.144/evaluation-service";


const STOCK_HISTORY_ENDPOINT = "/stocks"; 

const PORT = 9876; 

const API_CALL_TIMEOUT_MS = 450; 

const MAX_CORRELATION_TICKERS = 2; 

const stockPriceCache = new Map();

function cleanCache(minutesToKeep) {
    const cutoffTime = moment().subtract(minutesToKeep, 'minutes');
    stockPriceCache.forEach((history, ticker) => {
        const filteredHistory = history.filter(item => moment(item.lastUpdatedAt).isAfter(cutoffTime));
        if (filteredHistory.length === 0) {
            stockPriceCache.delete(ticker); 
        } else {
            stockPriceCache.set(ticker, filteredHistory);
        }
    });
    
}

setInterval(() => cleanCache(60), 10 * 60 * 1000);

async function fetchPriceHistoryFromAPI(ticker, minutes) {
    const url = `${THIRD_PARTY_STOCK_API_BASE_URL}${STOCK_HISTORY_ENDPOINT}/${ticker}`;
    const headers = {
        "Authorization": `${TOKEN_TYPE} ${ACCESS_TOKEN}`, 
        "Content-Type": "application/json"
    };

    try {
       
        const response = await axios.get(url, {
            headers: headers,
            params: { minutes: minutes }, 
            timeout: API_CALL_TIMEOUT_MS 
        });

        
        if (response.data && Array.isArray(response.data)) {
            const newHistory = response.data.map(item => ({
                price: parseFloat(item.price), 
                lastUpdatedAt: new Date(item.lastUpdatedAt) 
            })).sort((a, b) => a.lastUpdatedAt.getTime() - b.lastUpdatedAt.getTime()); 

            let currentHistory = stockPriceCache.get(ticker) || [];
            newHistory.forEach(newItem => {
               
                if (!currentHistory.some(existingItem => existingItem.lastUpdatedAt.getTime() === newItem.lastUpdatedAt.getTime())) {
                    currentHistory.push(newItem);
                }
            });
            
            currentHistory.sort((a, b) => a.lastUpdatedAt.getTime() - b.lastUpdatedAt.getTime());
            stockPriceCache.set(ticker, currentHistory);

            return newHistory; 
        }
        return []; 
    } catch (error) {
        
        if (axios.isCancel(error) || (error.code === 'ECONNABORTED' && error.message.includes('timeout'))) {
            console.warn(`Warning: 3rd party API call for '${ticker}' timed out (> ${API_CALL_TIMEOUT_MS}ms). Ignoring response.`);
        } else if (error.response) {
           
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


async function getPriceHistoryForMinutes(ticker, minutes) {
    const cutoffTime = moment().subtract(minutes, 'minutes');
    

    let historyFromCache = stockPriceCache.get(ticker) || [];
    const relevantHistory = historyFromCache.filter(item => moment(item.lastUpdatedAt).isAfter(cutoffTime));

    if (relevantHistory.length === 0 || 
        (relevantHistory.length < 5 && historyFromCache.length < 100) ||
        (historyFromCache.length > 0 && moment(historyFromCache[0].lastUpdatedAt).isBefore(cutoffTime))
    ) { 
        
        const fetchedHistory = await fetchPriceHistoryFromAPI(ticker, minutes);
        
        return fetchedHistory.filter(item => moment(item.lastUpdatedAt).isAfter(cutoffTime)); 
    }

    return relevantHistory; 
}


app.get('/stocks/:ticker', async (req, res) => {
    const ticker = req.params.ticker.toUpperCase();
    const minutes = parseInt(req.query.minutes);
    const aggregation = req.query.aggregation;

    if (isNaN(minutes) || minutes <= 0) {
        return res.status(400).json({ error: "Invalid 'minutes' parameter. Must be a positive number." });
    }
    if (aggregation !== 'average') {
        return res.status(400).json({ error: "Only 'average' aggregation is supported for this endpoint." });
    }

    const priceHistory = await getPriceHistoryForMinutes(ticker, minutes);

    let averagePrice = 0;
    if (priceHistory.length > 0) {
        const prices = priceHistory.map(item => item.price);
        averagePrice = mean(prices);
    } else {

        return res.status(404).json({ error: `No price history available for ${ticker} in the last ${minutes} minutes.` });
    }

    const responseData = [{
        averageStockPrice: parseFloat(averagePrice.toFixed(6)), 
        priceHistory: priceHistory.map(item => ({
            price: parseFloat(item.price.toFixed(6)), 
            lastUpdatedAt: item.lastUpdatedAt.toISOString()
        }))
    }];

    res.json(responseData);
});

app.get('/stock/correlation', async (req, res) => {
    const minutes = parseInt(req.query.minutes);
    const tickers = req.query.ticker; 

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


    if (tickerList.length !== MAX_CORRELATION_TICKERS) {
        return res.status(400).json({ error: `Correlation endpoint supports exactly ${MAX_CORRELATION_TICKERS} tickers.` });
    }

    const [ticker1, ticker2] = tickerList;

    const [history1, history2] = await Promise.all([
        getPriceHistoryForMinutes(ticker1, minutes),
        getPriceHistoryForMinutes(ticker2, minutes)
    ]);

    console.log(`Correlation Debug: History for ${ticker1} has ${history1.length} data points.`);
    console.log(`Correlation Debug: History for ${ticker2} has ${history2.length} data points.`);


    if (history1.length === 0 || history2.length === 0) {
        return res.status(404).json({ error: "Not enough data available for one or both tickers in the specified time frame." });
    }


    const alignedPrices = alignPriceHistories(history1, history2);

    if (alignedPrices.length < 2) { 
        console.warn(`Correlation: Not enough overlapping data points for ${ticker1} and ${ticker2} within ${minutes} minutes. Aligned points: ${alignedPrices.length}`);
        return res.status(404).json({ error: "Not enough overlapping data points for correlation calculation within the specified minutes." });
    }

    const prices1 = alignedPrices.map(p => p[0]); 
    const prices2 = alignedPrices.map(p => p[1]); 
    const correlation = calculatePearsonCorrelation(prices1, prices2);

    const responseData = {
        correlation: parseFloat(correlation.toFixed(4)),
        stocks: {
            [ticker1]: {
                averagePrice: parseFloat(mean(prices1).toFixed(6)), 
                priceHistory: history1.map(item => ({ 
                    price: parseFloat(item.price.toFixed(6)),
                    lastUpdatedAt: item.lastUpdatedAt.toISOString()
                }))
            },
            [ticker2]: {
                averagePrice: parseFloat(mean(prices2).toFixed(6)),
                priceHistory: history2.map(item => ({ 
                    price: parseFloat(item.price.toFixed(6)),
                    lastUpdatedAt: item.lastUpdatedAt.toISOString()
                }))
            }
        }
    };

    res.json(responseData);
});



/**
 * Aligns two price histories by finding exact common timestamps.
 * This is crucial for "time alignment of chosen tickers" for accurate correlation.
 * @param {Array<{price: number, lastUpdatedAt: Date}>} history1 Price history for stock 1.
 * @param {Array<{price: number, lastUpdatedAt: Date}>} history2 Price history for stock 2.
 * @returns {Array<[number, number]>}
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
 * @returns {number}
 */
function calculatePearsonCorrelation(X, Y) {
    if (X.length !== Y.length || X.length < 2) {
        console.warn("Insufficient or mismatched data points for correlation calculation.");
        return 0; 
    }

    const n = X.length;
    const meanX = mean(X); 
    const meanY = mean(Y); 

    let covarianceSum = 0;
    for (let i = 0; i < n; i++) {
        covarianceSum += (X[i] - meanX) * (Y[i] - meanY);
    }
    const covariance = covarianceSum / (n - 1);


    let sumSqDevX = 0;
    let sumSqDevY = 0;
    for (let i = 0; i < n; i++) {
        sumSqDevX += (X[i] - meanX) ** 2;
        sumSqDevY += (Y[i] - meanY) ** 2;
    }
    const sampleStdDevX = Math.sqrt(sumSqDevX / (n - 1));
    const sampleStdDevY = Math.sqrt(sumSqDevY / (n - 1));

    if (sampleStdDevX === 0 || sampleStdDevY === 0) {
        return 0;
    }

    return covariance / (sampleStdDevX * sampleStdDevY);
}


app.listen(PORT, () => {
    console.log(`Stock Insights Microservice listening on port ${PORT}`);
});
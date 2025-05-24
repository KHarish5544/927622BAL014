const express = require('express');
const axios = require('axios');
const app = express();

const clientid = "8fa25491-2d20-43b5-a897-1b748ebd3e40";
const clientSecret = "wcCsGwekkWknyJTT";
const access_token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJNYXBDbGFpbXMiOnsiZXhwIjoxNzQ4MDcxMTc4LCJpYXQiOjE3NDgwNzA4NzgsImlzcyI6IkFmZm9yZG1lZCIsImp0aSI6IjhmYTI1NDkxLTJkMjAtNDNiNS1hODk3LTFiNzQ4ZWJkM2U0MCIsInN1YiI6ImhhcmlzaGxpZmU1NUBnbWFpbC5jb20ifSwiZW1haWwiOiJoYXJpc2hsaWZlNTVAZ21haWwuY29tIiwibmFtZSI6ImhhcmlzaCBrIiwicm9sbE5vIjoiOTI3NjIyYmFsMDE0IiwiYWNjZXNzQ29kZSI6IndoZVFVeSIsImNsaWVudElEIjoiOGZhMjU0OTEtMmQyMC00M2I1LWE4OTctMWI3NDhlYmQzZTQwIiwiY2xpZW50U2VjcmV0Ijoid2NDc0d3ZWtrV2tueUpUVCJ9.Hffl255qMGohMCaEYS5v2VuonSczRySloSXACueIP6g";
const token_type = "Bearer";

const Thirdpartyurl = "http://20.244.56.144/evaluation-service";

const number_paths = {
    'p': '/primes',
    'f': '/fibo',
    'e': '/even',
    'r': '/rand'
};

const PORT = 9876;
const WINDOW_SIZE = 10;
const OVERALL_TIMEOUT_MS = 400;
const THIRD_PARTY_CALL_TIMEOUT_MS = 450;

let currWindow = [];

app.use(express.json());

app.get('/numbers/:numberid', async (req, res) => {
    const numberid = req.params.numberid;
    const startRequestTime = Date.now();

    const windowprevstate = structuredClone(currWindow); 

    let receivedNumbersFrom3rdParty = [];

    const numberApiPath = number_paths[numberid];

    if (!numberApiPath) {
        return res.status(400).json({ error: "Invalid number ID provided. Must be 'p', 'f', 'e', or 'r'." });
    }

    const thirdpartyurl_numapi = `${Thirdpartyurl}${numberApiPath}`;

    try {
        const headers = {
            "Authorization": `Bearer ${access_token}`,
            "Content-Type": "application/json"
        };
        
        const response3rdParty = await axios.get(thirdpartyurl_numapi, {
            headers: headers,
            timeout: THIRD_PARTY_CALL_TIMEOUT_MS
        });

        if (response3rdParty.data && Array.isArray(response3rdParty.data.numbers)) {
            receivedNumbersFrom3rdParty = response3rdParty.data.numbers
                                          .map(n => parseInt(n))
                                          .filter(n => !isNaN(n));
        }

    } catch (error) {
        if (axios.isCancel(error) || (error.code === 'ECONNABORTED' && error.message.includes('timeout'))) {
            console.warn(`Warning: 3rd party API call for '${numberid}' timed out (> ${THIRD_PARTY_CALL_TIMEOUT_MS}ms). Ignoring response.`);
        } else if (error.response) {
            console.error(`Error fetching from 3rd party API for '${numberid}': Status ${error.response.status}, Data: ${JSON.stringify(error.response.data)}. Ignoring response.`);
        } else if (error.request) {
            console.error(`Error fetching from 3rd party API for '${numberid}': No response received. ${error.message}. Ignoring response.`);
        } else {
            console.error(`Error in setting up 3rd party request for '${numberid}': ${error.message}. Ignoring response.`);
        }
        receivedNumbersFrom3rdParty = [];
    }

    if ((Date.now() - startRequestTime) > OVERALL_TIMEOUT_MS) {
        console.warn(`Performance Alert: Overall request for '${numberid}' exceeded ${OVERALL_TIMEOUT_MS}ms.`);
    }

    receivedNumbersFrom3rdParty.forEach(num => {
        if (!currWindow.includes(num)) {
            currWindow.push(num);
            if (currWindow.length > WINDOW_SIZE) {
                currWindow.shift();
            }
        }
    });

    const windowCurrState = structuredClone(currWindow);

    let avg = 0.00;
    if (windowCurrState.length > 0) {
        const sum = windowCurrState.reduce((total, num) => total + num, 0);
        avg = sum / windowCurrState.length;
    }

    const responseData = {
        windowPrevState: windowprevstate,
        windowCurrState: windowCurrState,
        numbers: receivedNumbersFrom3rdParty,
        avg: parseFloat(avg.toFixed(2))
    };

    res.json(responseData);
});

app.listen(PORT, () => {
    console.log(`Average Calculator Microservice listening on port ${PORT}`)
});
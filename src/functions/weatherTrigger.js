const { app } = require('@azure/functions');
const axios = require('axios');
const sql = require('mssql');

const cities = [
    { name: 'Delhi', lat: 28.7041, lon: 77.1025 },
    { name: 'Mumbai', lat: 19.0760, lon: 72.8777 },
    { name: 'Chennai', lat: 13.0827, lon: 80.2707 },
    { name: 'Bangalore', lat: 12.9716, lon: 77.5946 },
    { name: 'Kolkata', lat: 22.5726, lon: 88.3639 },
    { name: 'Hyderabad', lat: 17.3850, lon: 78.4867 }
];

const apiKey = process.env.OPENWEATHER_API_KEY;
const dbConfig = {
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    server: process.env.DB_SERVER,
    database: process.env.DB_DATABASE,
    options: {
        encrypt: true,
        enableArithAbort: true,
        trustServerCertificate: false
    },
    pool: {
        max: 10,
        min: 0,
        idleTimeoutMillis: 30000
    }
};

let sqlPool = null;

async function getSqlPool() {
    if (!sqlPool) {
        sqlPool = await sql.connect(dbConfig);
    }
    return sqlPool;
}

async function fetchWeatherData(city, context) {
    const url = `https://api.openweathermap.org/data/2.5/weather?lat=${city.lat}&lon=${city.lon}&appid=${apiKey}&units=metric`;
    try {
        const response = await axios.get(url, {
            timeout: 5000
        });

        if (!response.data || !response.data.main) {
            throw new Error('Invalid weather data format');
        }

        return {
            city: city.name,
            temperature: response.data.main.temp,
            feels_like: response.data.main.feels_like,
            pressure: response.data.main.pressure,
            humidity: response.data.main.humidity,
            weather: response.data.weather[0]?.main || 'Unknown',
            timestamp: new Date().toISOString()
        };
    } catch (error) {
        context.error(`Error fetching weather data for ${city.name}: ${error.message}`);
        if (error.response) {
            context.error(`API Response: ${JSON.stringify(error.response.data)}`);
        }
        return null;
    }
}

async function insertWeatherData(params, context) {
    try {
        const pool = await getSqlPool();
        const utcTimestamp = new Date(params.timestamp);
        const istOffset = 5.5 * 60 * 60 * 1000;
        const istTimestamp = new Date(utcTimestamp.getTime() + istOffset);

        const query = `
            INSERT INTO weather_data (city, temperature, feels_like, pressure, humidity, weather, timestamp)
            VALUES (@city, @temperature, @feels_like, @pressure, @humidity, @weather, @timestamp);
        `;

        await pool.request()
            .input('city', sql.NVarChar, params.city)
            .input('temperature', sql.Float, params.temperature)
            .input('feels_like', sql.Float, params.feels_like)
            .input('pressure', sql.Int, params.pressure)
            .input('humidity', sql.Int, params.humidity)
            .input('weather', sql.NVarChar, params.weather)
            .input('timestamp', sql.DateTimeOffset, istTimestamp)
            .query(query);

        context.log(`Weather data for ${params.city} inserted successfully.`);
    } catch (err) {
        context.error(`Error inserting weather data for ${params.city}: ${err.message}`);
        throw err;
    }
}

app.timer('weatherTrigger', {
    schedule: '0 */5 * * * *',
    handler: async function (myTimer, context) {
        try {
            context.log('Starting weather data collection...');

            if (!apiKey) {
                throw new Error('OpenWeather API key not configured');
            }

            const weatherPromises = cities.map(city => fetchWeatherData(city, context));
            const weatherDataResults = await Promise.all(weatherPromises);

            const validWeatherData = weatherDataResults.filter(data => data !== null);

            if (validWeatherData.length === 0) {
                context.error('No valid weather data collected for any city');
                return;
            }

            const insertPromises = validWeatherData.map(data => insertWeatherData(data, context));
            await Promise.all(insertPromises);

            context.log(`Successfully processed weather data for ${validWeatherData.length} cities`);
        } catch (error) {
            context.error(`Fatal error in weather collection: ${error.message}`);
            throw error;
        }
    }
});
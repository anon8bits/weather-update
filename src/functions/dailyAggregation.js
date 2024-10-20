const sql = require('mssql');

const config = {
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    server: process.env.DB_SERVER,
    database: process.env.DB_DATABASE,
    options: {
        encrypt: true,
        enableArithAbort: true,
        trustServerCertificate: true
    }
};

const { app } = require('@azure/functions');

app.timer('dailyAggregation', {
    schedule: '0 30 18 * * *',
    handler: async function (myTimer, context) {
        let pool;

        const ist = new Date(new Date().getTime() + (5.5 * 60 * 60 * 1000));
        ist.setDate(ist.getDate() - 1);
        const yesterdayIST = ist.toISOString().split('T')[0];

        context.log('=================== Function Start ===================');
        context.log(`Current IST: ${new Date(Date.now() + (5.5 * 60 * 60 * 1000)).toISOString()}`);
        context.log(`Processing data for date: ${yesterdayIST}`);

        try {
            pool = await sql.connect(config);

            const initialDataQuery = `
                SELECT 
                    FORMAT(MIN(timestamp), 'yyyy-MM-dd HH:mm:ss') as earliest_record,
                    FORMAT(MAX(timestamp), 'yyyy-MM-dd HH:mm:ss') as latest_record,
                    COUNT(*) as total_records,
                    COUNT(DISTINCT CONVERT(date, timestamp)) as distinct_dates,
                    COUNT(DISTINCT city) as distinct_cities
                FROM weather_data;
            `;

            const initialData = await pool.request().query(initialDataQuery);
            context.log('Current data in weather_data table:', initialData.recordset[0]);

            const checkDataQuery = `
                SELECT 
                    city,
                    COUNT(*) as record_count,
                    FORMAT(MIN(timestamp), 'yyyy-MM-dd HH:mm:ss') as earliest_record,
                    FORMAT(MAX(timestamp), 'yyyy-MM-dd HH:mm:ss') as latest_record
                FROM weather_data
                WHERE CONVERT(date, timestamp) = @yesterday
                GROUP BY city;
            `;

            const dataCheck = await pool.request()
                .input('yesterday', sql.Date, yesterdayIST)
                .query(checkDataQuery);

            if (dataCheck.recordset.length === 0) {
                context.log(`No data found for ${yesterdayIST}. Stopping execution.`);
                return;
            }

            for (const cityData of dataCheck.recordset) {
                const aggregationQuery = `
                    WITH WeatherCounts AS (
                        SELECT weather, COUNT(*) as count,
                            ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as rn
                        FROM weather_data
                        WHERE CONVERT(date, timestamp) = @yesterday 
                        AND city = @city
                        GROUP BY weather
                    )
                    INSERT INTO daily_weather_summary (
                        city, 
                        date, 
                        avg_temp, 
                        min_temp, 
                        max_temp, 
                        dominant_weather,
                        avg_feels_like, 
                        avg_pressure, 
                        avg_humidity,
                        record_count
                    )
                    SELECT 
                        @city,
                        @yesterday,
                        ROUND(AVG(CAST(w.temperature AS FLOAT)), 2),
                        MIN(w.temperature),
                        MAX(w.temperature),
                        (SELECT TOP 1 weather FROM WeatherCounts WHERE rn = 1),
                        ROUND(AVG(CAST(w.feels_like AS FLOAT)), 2),
                        ROUND(AVG(CAST(w.pressure AS FLOAT)), 2),
                        ROUND(AVG(CAST(w.humidity AS FLOAT)), 2),
                        COUNT(*)
                    FROM weather_data w
                    WHERE CONVERT(date, w.timestamp) = @yesterday
                    AND w.city = @city
                    GROUP BY w.city;
                `;

                const result = await pool.request()
                    .input('city', sql.VarChar, cityData.city)
                    .input('yesterday', sql.Date, yesterdayIST)
                    .query(aggregationQuery);

                context.log(`Processed city ${cityData.city}: ${result.rowsAffected[0]} summary row inserted`);
            }

            const verifyQuery = `
                SELECT city, date, record_count, dominant_weather
                FROM daily_weather_summary
                WHERE date = @yesterday;
            `;

            const verification = await pool.request()
                .input('yesterday', sql.Date, yesterdayIST)
                .query(verifyQuery);

            context.log('Inserted summaries:', verification.recordset);
            const transaction = new sql.Transaction(pool);
            await transaction.begin();

            try {
                const countQuery = `
                    SELECT COUNT(*) as delete_count
                    FROM weather_data
                    WHERE CONVERT(date, timestamp) = @yesterday;
                `;

                const countResult = await transaction.request()
                    .input('yesterday', sql.Date, yesterdayIST)
                    .query(countQuery);

                const recordsToDelete = countResult.recordset[0].delete_count;
                context.log(`Will delete ${recordsToDelete} records`);
                const deleteQuery = `
                    DELETE FROM weather_data
                    WHERE CONVERT(date, timestamp) = @yesterday;
                `;

                const deleteResult = await transaction.request()
                    .input('yesterday', sql.Date, yesterdayIST)
                    .query(deleteQuery);

                const resetIdentityQuery = `
                    DECLARE @TableName NVARCHAR(128) = 'weather_data';
                    DECLARE @NewSeed INT;
                    
                    SELECT @NewSeed = ISNULL(MIN(id), 0) - 1
                    FROM weather_data;
                    
                    DECLARE @SQL NVARCHAR(MAX) = N'DBCC CHECKIDENT (' + QUOTENAME(@TableName) + ', RESEED, ' + 
                        CAST(@NewSeed AS NVARCHAR(20)) + ');';
                    
                    EXEC sp_executesql @SQL;
                `;

                await transaction.request().query(resetIdentityQuery);
                await transaction.commit();
                context.log(`Deleted ${deleteResult.rowsAffected[0]} records and reset identity`);

                // Verify final state
                const finalStateQuery = `
                    SELECT 
                        (SELECT COUNT(*) FROM weather_data) as remaining_records,
                        (SELECT IDENT_CURRENT('weather_data')) as current_identity;
                `;
                const finalState = await pool.request().query(finalStateQuery);
                context.log('Final database state:', finalState.recordset[0]);

            } catch (err) {
                await transaction.rollback();
                context.log('Transaction rolled back due to error:', err);
                throw err;
            }

        } catch (err) {
            context.log('Error in daily aggregation process:', err);
            throw err;
        } finally {
            if (pool) {
                await pool.close();
            }
            context.log('=================== Function End ===================');
        }
    }
});
import express from "express";
import { dbGetDatabase, getMsSqlPoolConnect, dbPushDatabase, getOracleSqlConnect } from "./db.connection.js";


const app = express();
app.use(express.json());

function getDeviceLogsQuery(month, year, existingRows = []) {
    const paddedMonth = month.toString().padStart(2, '0');
    let baseQuery = `SELECT * FROM dbo.DeviceLogs_${paddedMonth}_${year}`;
    const conditions = [];
    if (existingRows && existingRows.length > 0) {
        const deviceConditions = existingRows.map(row => {
            const deviceId = row[2]; // DeviceId is at index 2
            let downloadDate = row[1];
            console.log(downloadDate, "downloadDate")
            function formatIST(date) {
                const ist = new Date(date.getTime() + (5.5 * 60 * 60 * 1000));

                return ist.toISOString()
                    .replace('T', ' ')
                    .replace('Z', '')
                    .substring(0, 19);
            }
            downloadDate = formatIST(downloadDate)
            return `(DeviceId = ${deviceId} AND DownloadDate > '${downloadDate}')`;
        });
        conditions.push(`${deviceConditions.join(' AND ')}`);
    }

    if (conditions.length) {
        baseQuery += ' WHERE ' + conditions.join(' AND ');
    }
    return baseQuery;
}

const currentDate = new Date();
const month = currentDate.getMonth() + 1;
const year = currentDate.getFullYear();
const oracleQuery = `SELECT *
FROM (
    SELECT
        t.*,
        ROW_NUMBER() OVER (
            PARTITION BY t.DEVICEID          
            ORDER BY t.DOWNLOADDATE DESC     
        ) AS rn
    FROM DEVICELOGS t
) sub
WHERE rn = 1  `

export async function getRes(connection, sqlQuery) {
    try {
        const result = await connection.execute(sqlQuery)

        return result
    } catch (error) {
        console.log(error);
        return false
    }
}

export async function getRes1(connection, sqlQuery) {
    try {
        const result = await connection.request().query(sqlQuery)

        return result
    } catch (error) {
        console.log(error);
        return false
    }
}

export async function pushDataToOracle(connection, tableName, dataArray) {
    if (!dataArray || dataArray.length === 0) {
        console.log("No data to push to Oracle.");
        return;
    }

    // Get the keys from the first object to form columns
    const keys = Object.keys(dataArray[0]);
    const columns = keys.map(k => k.toUpperCase()).join(', ');
    // Create bind placeholders like :1, :2, :3
    const bindVars = keys.map((_, i) => `:${i + 1}`).join(', ');

    const sqlQuery = `INSERT INTO ${tableName} (${columns}) VALUES (${bindVars})`;

    // Map the array of objects into an array of arrays (which executeMany expects)
    const binds = dataArray.map(obj => keys.map(k => obj[k]));

    const options = {
        autoCommit: true // Automatically commit the insert
    };

    // console.log(sqlQuery, "sqlQuery")

    try {
        // console.log(`Pushing ${dataArray.length} records to Oracle table: ${tableName}`);
        const result = await connection.executeMany(sqlQuery, binds, options);
        // console.log(`Successfully inserted ${result.rowsAffected} rows into ${tableName}`);
        return result;
    } catch (error) {
        console.error("Error pushing data to Oracle:", error);
        return false;
    }
}

async function testConnection() {





    const connectionForOracle = await getOracleSqlConnect(dbPushDatabase);
    const existingResult = await getRes(connectionForOracle, oracleQuery);
    const existingRows = existingResult?.rows || [];
    // Helper to safely convert dates to ISO strings
    const toISO = (val) => {
        if (!val) return '';
        const d = val instanceof Date ? val : new Date(val);
        return isNaN(d.getTime()) ? '' : d.toISOString();
    };
    const existingSet = new Set(
        existingRows.map(row => {
            const deviceId = row[2];
            const downloadDate = row[1];
            return `${deviceId}_${toISO(downloadDate)}`;
        })
    );
    console.log(existingRows, "existingRows");


    const pool = await getMsSqlPoolConnect(dbGetDatabase);
    const connectionForMsSql = await pool.connect();

    const mssqlQuery = getDeviceLogsQuery(month, year, existingRows);

    console.log(mssqlQuery, "mssqlQuery")
    const resultForMsSql = await getRes1(connectionForMsSql, mssqlQuery);
    const msRecords = resultForMsSql?.recordsets?.[0] || [];

    // Filter out records that are already present in Oracle
    // const newRows = msRecords.filter(row => {
    //     const key = `${row.UserId}_${toISO(row.LogDate)}`;
    //     console.log({
    //         key,
    //         existingSet: existingSet.has(key)
    //     })
    //     return !existingSet.has(key);
    // });

    // if (newRows.length === 0) {
    //     console.log("No new records to push to Oracle.");
    //     return;
    // }
    // console.log(newRows, "newRows");

    // Push the new rows to Oracle table 'devicelogs'
    await pushDataToOracle(connectionForOracle, 'DEVICELOGS', msRecords);
    console.log(`Pushed ${msRecords.length} new records to Oracle table devicelogs.`);
}




testConnection()

// CREATE TABLE DeviceLogs (
//     DeviceLogId INT,
//     DownloadDate TIMESTAMP,
//     DeviceId INT,
//     UserId VARCHAR(50),
//     LogDate TIMESTAMP,
//     Direction VARCHAR(50),
//     AttDirection VARCHAR(50),
//     C1 VARCHAR(50),
//     C2 VARCHAR(50),
//     C3 VARCHAR(50),
//     C4 VARCHAR(50),
//     C5 VARCHAR(50),
//     C6 VARCHAR(50),
//     C7 VARCHAR(50),
//     WorkCode VARCHAR(50),
//     UpdateFlag INT,
//     EmployeeImage BLOB,
//     FileName VARCHAR(255),
//     Longitude VARCHAR(50),
//     Latitude VARCHAR(50),
//     IsApproved INT,
//     CreatedDate TIMESTAMP,
//     LastModifiedDate TIMESTAMP,
//     LocationAddress VARCHAR(500),
//     BodyTemperature DECIMAL(10,2),
//     IsMaskOn INT
// )




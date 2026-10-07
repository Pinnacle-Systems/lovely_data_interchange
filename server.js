import express from "express";
import { dbGetDatabase, getMsSqlPoolConnect, dbPushDatabase, getOracleSqlConnect, dbPushMysql, getRes, getRes1, getMySqlConnect } from "./db.connection.js";


const app = express();
app.use(express.json());

function getDeviceLogsQuery(month, year, existingRows = []) {
    const paddedMonth = month.toString();
    let baseQuery = `SELECT * FROM dbo.DeviceLogs_${paddedMonth}_${year}`;
    const conditions = [];
    if (existingRows && existingRows.length > 0) {
        const deviceConditions = existingRows.map(row => {
            const deviceId = row[2]; // DeviceId is at index 2
            let downloadDate = row[1];

            function formatIST(date) {
                const d = new Date(date);
                const ist = new Date(d.getTime() + (5.5 * 60 * 60 * 1000));

                return ist.toISOString()
                    .replace('T', ' ')
                    .replace('Z', '')
                    .substring(0, 19);
            }
            downloadDate = formatIST(downloadDate)
            return `(DeviceId = ${deviceId} AND DownloadDate > '${downloadDate}')`;
        });
        conditions.push(`(${deviceConditions.join(' OR ')})`);
    }

    if (conditions.length) {
        baseQuery += ' WHERE ' + conditions.join(' AND ');
    }
    return baseQuery;
}

const currentDate = new Date();
const oracleQuery = `SELECT * FROM (
    SELECT
        t.*,
        ROW_NUMBER() OVER (
            PARTITION BY t.DEVICEID          
            ORDER BY t.DOWNLOADDATE DESC     
        ) AS rn
    FROM DEVICELOGS t
) sub
WHERE rn = 1  `



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
        autoCommit: true
    };

    // console.log(sqlQuery, "sqlQuery")

    try {
        const result = await connection.executeMany(sqlQuery, binds, options);
        return result;
    } catch (error) {
        console.error("Error pushing data to Oracle:", error);
        return false;
    }
}


export async function pushDataToMysql(connection, tableName, dataArray) {
    if (!dataArray || dataArray.length === 0) {
        console.log("No data to push to MySQL.");
        return;
    }

    const keys = Object.keys(dataArray[0]);

    const columns = keys.map(k => `\`${k}\``).join(', ');

    const placeholders = keys.map(() => '?').join(', ');

    const sqlQuery = `
        INSERT INTO \`${tableName}\` (${columns})
        VALUES (${placeholders})
    `;

    try {
        for (const row of dataArray) {
            const values = keys.map(key => row[key]);

            await connection.execute(sqlQuery, values);
        }

        console.log(`Inserted ${dataArray.length} records into ${tableName}`);

        return true;
    } catch (error) {
        console.error("Error pushing data to MySQL:", error);
        return false;
    }
}


function getDeviceLogsFromMysqlQuery(month, year, existingRows = []) {
    const paddedMonth = month.toString();
    let baseQuery = `SELECT * FROM DeviceLogs_${paddedMonth}_${year}`;
    const conditions = [];
    if (existingRows && existingRows.length > 0) {
        const deviceConditions = existingRows.map(row => {
            const deviceId = row[2]; // DeviceId is at index 2
            let downloadDate = row[1];

            function formatIST(date) {
                const d = new Date(date);
                const ist = new Date(d.getTime() + (5.5 * 60 * 60 * 1000));

                return ist.toISOString()
                    .replace('T', ' ')
                    .replace('Z', '')
                    .substring(0, 19);
            }
            downloadDate = formatIST(downloadDate)
            return `(DeviceId = ${deviceId} AND DownloadDate > '${downloadDate}')`;
        });
        conditions.push(`(${deviceConditions.join(' OR ')})`);
    }

    if (conditions.length) {
        baseQuery += ' WHERE ' + conditions.join(' AND ');
    }
    return baseQuery;
}



async function testConnection() {

    const connectionForMysql = await getMySqlConnect(dbPushMysql);  // connect mysql

    const connectionForOracle = await getOracleSqlConnect(dbPushDatabase);  // connect oracle


    const pool = await getMsSqlPoolConnect(dbGetDatabase);
    const connectionForMsSql = await pool.connect();



    const existingResult = await getRes(connectionForOracle, oracleQuery);
    const existingRows = existingResult?.rows || [];                    // get data for latest record in oracle

    console.log(existingRows, "existingRows");


    // Find the earliest date to determine how many tables to query
    let minDate = new Date();
    if (existingRows && existingRows.length > 0) {
        existingRows.forEach(row => {
            let downloadDate = row[1];
            const d = new Date(downloadDate);
            if (!isNaN(d.getTime()) && d < minDate) {
                minDate = d;
            }
        });
    }

    // Generate list of months to query
    const tablesToQuery = [];
    let tempDate = new Date(minDate.getFullYear(), minDate.getMonth(), 1);
    while (tempDate <= currentDate) {
        tablesToQuery.push({
            month: tempDate.getMonth() + 1,
            year: tempDate.getFullYear()
        });
        tempDate.setMonth(tempDate.getMonth() + 1);
    }



    let msRecords = [];
    for (const { month, year } of tablesToQuery) {
        const mssqlQuery = getDeviceLogsQuery(month, year, existingRows);
        const resultForMsSql = await getRes1(connectionForMsSql, mssqlQuery);
        const records = resultForMsSql?.recordsets?.[0] || [];
        msRecords = msRecords.concat(records);
    }
    await pushDataToOracle(connectionForOracle, 'DEVICELOGS', msRecords);
    console.log(`Pushed ${msRecords.length} new records to Oracle table devicelogs.`);

    console.log(tablesToQuery, "tablesToQuery")


    for (const { month, year } of tablesToQuery) {
        const mysqlQuery = getDeviceLogsFromMysqlQuery(month, year, existingRows);
        const resultForMsSql = await getRes(connectionForMysql, mysqlQuery);

        // console.log(mysqlQuery, "mysqlQuery")
        // console.log(resultForMsSql?.[0], "resultForMsSql")

        const records = resultForMsSql?.[0] || [];
        await pushDataToOracle(connectionForOracle, 'DEVICELOGS', records);

    }




}




testConnection()






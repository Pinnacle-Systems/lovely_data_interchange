import express from "express";
import dotenv from "dotenv";
import cron from "node-cron";

dotenv.config();
import {
  dbGetDatabase,
  getMsSqlPoolConnect,
  dbPushDatabase,
  getOracleSqlConnect,
  dbPushMysql,
  getRes,
  getRes1,
  getMySqlConnect,
} from "./db.connection.js";

const app = express();
app.use(express.json());

function getDeviceLogsQuery(month, year, existingRows = []) {
  const paddedMonth = month.toString();
  let baseQuery = `SELECT * FROM dbo.DeviceLogs_${paddedMonth}_${year}`;
  const conditions = [];
  if (existingRows && existingRows.length > 0) {
    const deviceConditions = existingRows.map((row) => {
      const deviceId = row[2]; // DeviceId is at index 2
      let downloadDate = row[1];

      function formatIST(date) {
        const d = new Date(date);
        const ist = new Date(d.getTime() + 5.5 * 60 * 60 * 1000);

        return ist
          .toISOString()
          .replace("T", " ")
          .replace("Z", "")
          .substring(0, 19);
      }
      downloadDate = formatIST(downloadDate);
      return `(DeviceId = ${deviceId} AND DownloadDate > '${downloadDate}')`;
    });
    conditions.push(`(${deviceConditions.join(" OR ")})`);
  }

  if (conditions.length) {
    baseQuery += " WHERE " + conditions.join(" AND ");
  }
  return baseQuery;
}

function getDeviceLogsFromMysqlQuery(month, year, existingRows = []) {
  const paddedMonth = month.toString();
  let baseQuery = `SELECT * FROM DeviceLogs_${paddedMonth}_${year}`;
  const conditions = [];
  if (existingRows && existingRows.length > 0) {
    const deviceConditions = existingRows.map((row) => {
      const deviceId = row[2]; // DeviceId is at index 2
      let downloadDate = row[1];

      function formatIST(date) {
        const d = new Date(date);
        const ist = new Date(d.getTime() + 5.5 * 60 * 60 * 1000);

        return ist
          .toISOString()
          .replace("T", " ")
          .replace("Z", "")
          .substring(0, 19);
      }
      downloadDate = formatIST(downloadDate);
      return `(DeviceId = ${deviceId} AND DownloadDate > '${downloadDate}')`;
    });
    conditions.push(`(${deviceConditions.join(" OR ")})`);
  }

  if (conditions.length) {
    baseQuery += " WHERE " + conditions.join(" AND ");
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
WHERE rn = 1  `;

export async function pushDataToOracle(connection, tableName, dataArray) {
  if (!dataArray || dataArray.length === 0) {
    console.log("No data to push to Oracle.");
    return;
  }

  // Get the keys from the first object to form columns
  const keys = Object.keys(dataArray[0]);
  const columns = keys.map((k) => k.toUpperCase()).join(", ");
  // Create bind placeholders like :1, :2, :3
  const bindVars = keys.map((_, i) => `:${i + 1}`).join(", ");

  const sqlQuery = `INSERT INTO ${tableName} (${columns}) VALUES (${bindVars})`;

  // Map the array of objects into an array of arrays (which executeMany expects)
  const binds = dataArray.map((obj) => keys.map((k) => obj[k]));

  const options = {
    autoCommit: false,
  };

  // console.log(sqlQuery, "sqlQuery")

  try {
    const result = await connection.executeMany(sqlQuery, binds, options);
    await connection.commit();
    return result;
  } catch (error) {
    console.error(
      "Error pushing data to Oracle, rolling back transaction:",
      error,
    );
    try {
      await connection.rollback();
    } catch (rollbackError) {
      console.error("Failed to rollback transaction:", rollbackError);
    }
    return false;
  }
}

export async function pushDataToMysql(connection, tableName, dataArray) {
  if (!dataArray || dataArray.length === 0) {
    console.log("No data to push to MySQL.");
    return;
  }

  const keys = Object.keys(dataArray[0]);

  const columns = keys.map((k) => `\`${k}\``).join(", ");

  const placeholders = keys.map(() => "?").join(", ");

  const sqlQuery = `
        INSERT INTO \`${tableName}\` (${columns})
        VALUES (${placeholders})
    `;

  try {
    for (const row of dataArray) {
      const values = keys.map((key) => row[key]);

      await connection.execute(sqlQuery, values);
    }

    console.log(`Inserted ${dataArray.length} records into ${tableName}`);

    return true;
  } catch (error) {
    console.error("Error pushing data to MySQL:", error);
    return false;
  }
}

export async function testConnection() {
  let connectionForMysql, connectionForOracle, connectionForMsSql;

  try {
    // connectionForMysql = await getMySqlConnect(dbPushMysql);
    // if (!connectionForMysql) throw new Error("MySQL connection failed.");

    connectionForOracle = await getOracleSqlConnect(dbPushDatabase);
    if (!connectionForOracle) throw new Error("Oracle connection failed.");

    connectionForMsSql = await getMsSqlPoolConnect(dbGetDatabase);
    if (!connectionForMsSql) throw new Error("MSSQL connection failed.");

    const existingResult = await getRes(connectionForOracle, oracleQuery);
    const existingRows = existingResult?.rows || [];

    // Find the earliest date to determine how many tables to query
    let minDate = new Date();
    if (existingRows && existingRows.length > 0) {
      existingRows.forEach((row) => {
        let downloadDate = row[1];
        const d = new Date(downloadDate);
        if (!isNaN(d.getTime()) && d < minDate) {
          minDate = d;
        }
      });
    }

    // Dynamic current date so 24/7 scheduler always uses live date
    const now = new Date();

    // Generate list of months to query
    const tablesToQuery = [];
    let tempDate = new Date(minDate.getFullYear(), minDate.getMonth(), 1);
    while (tempDate <= now) {
      tablesToQuery.push({
        month: tempDate.getMonth() + 1,
        year: tempDate.getFullYear(),
      });
      tempDate.setMonth(tempDate.getMonth() + 1);
    }

    console.log(tablesToQuery, "tablesToQuery");

    let msRecords = [];
    for (const { month, year } of tablesToQuery) {
      const mssqlQuery = getDeviceLogsQuery(month, year, existingRows);
      console.log(mssqlQuery, "mssqlQuery");

      const resultForMsSql = await getRes1(connectionForMsSql, mssqlQuery);
      const records = resultForMsSql?.recordsets?.[0] || [];
      msRecords = msRecords.concat(records);
    }

    await pushDataToOracle(connectionForOracle, "DEVICELOGS", msRecords);
    console.log(`Pushed ${msRecords.length} records to Oracle DEVICELOGS`);

    return {
      success: true,
      recordsCount: msRecords.length,
      message: `Successfully pushed ${msRecords.length} records to Oracle`
    };
  } catch (error) {
    console.error("Database connection / sync error:", error.message || error);
    throw error;
  } finally {
    // Cleanly close connections to prevent socket & session leaks
    // if (connectionForMysql) {
    //   try {
    //     if (typeof connectionForMysql.end === "function") await connectionForMysql.end();
    //   } catch (e) {}
    // }
    if (connectionForMsSql) {
      try {
        if (typeof connectionForMsSql.close === "function") await connectionForMsSql.close();
      } catch (e) { }
    }
    if (connectionForOracle) {
      try {
        if (typeof connectionForOracle.close === "function") await connectionForOracle.close();
      } catch (e) { }
    }
  }
}

// ----------------------------------------------------
// State tracker for monitoring and health check
// ----------------------------------------------------
let isRunning = false;
let lastSyncStatus = {
  lastRunAt: null,
  status: "idle",
  lastMessage: "Service started, awaiting first run",
  recordsPushed: 0,
};

export async function runSyncJob() {
  if (isRunning) {
    console.log(`[${new Date().toISOString()}] Previous sync is still running. Skipping this cycle.`);
    return { success: false, message: "Sync already in progress" };
  }

  isRunning = true;
  lastSyncStatus.status = "running";
  lastSyncStatus.lastRunAt = new Date().toISOString();

  try {
    const result = await testConnection();
    lastSyncStatus.status = "success";
    lastSyncStatus.lastMessage = result?.message || "Sync completed successfully";
    lastSyncStatus.recordsPushed = result?.recordsCount || 0;
    return result;
  } catch (err) {
    lastSyncStatus.status = "error";
    lastSyncStatus.lastMessage = err?.message || String(err);
    return { success: false, error: err?.message };
  } finally {
    isRunning = false;
  }
}

// ----------------------------------------------------
// node-cron Scheduler Setup
// ----------------------------------------------------
const intervalMinutes = parseInt(process.env.SYNC_INTERVAL_MINUTES, 10) || 5;
const cronExpression = process.env.CRON_SCHEDULE || `*/${intervalMinutes} * * * *`;

console.log(`[Cron] Initializing cron job: "${cronExpression}" (every ${intervalMinutes} minutes)`);

cron.schedule(cronExpression, async () => {
  console.log(`[Cron] Scheduled execution triggered at ${new Date().toISOString()}`);
  await runSyncJob();
});

// ----------------------------------------------------
// Express API Routes (for Subdomain / Reverse Proxy / Health Check)
// ----------------------------------------------------
app.get("/", (req, res) => {
  res.json({
    service: "Lovely Data Interchange Service",
    status: "online",
    intervalMinutes,
    cronExpression,
    isRunning,
    lastSyncStatus,
  });
});

app.get("/status", (req, res) => {
  res.json({
    isRunning,
    ...lastSyncStatus,
  });
});

app.all("/sync", async (req, res) => {
  if (isRunning) {
    return res.status(409).json({ message: "Sync already in progress", isRunning: true });
  }
  runSyncJob();
  res.json({ message: "Sync triggered successfully" });
});

// ----------------------------------------------------
// Start Express Server
// ----------------------------------------------------
const PORT = process.env.PORT || 5000;
app.listen(PORT, () => {
  console.log(`Server is running and listening on port ${PORT}`);
  // Run once immediately on startup
  runSyncJob();
});

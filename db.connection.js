import { createRequire } from "module";
import sql from "mssql";
import dotenv from "dotenv"
import mysql from 'mysql2/promise';

dotenv.config();

const require = createRequire(import.meta.url);
const oracledb = require('oracledb');
oracledb.initOracleClient({ libDir: "C:\\oracle\\instantclient_19_20" });

export const dbGetDatabase = {
  user: process.env.GETDB_USER || "",
  password: process.env.GETDB_PASSWORD || "",
  server: process.env.GETDB_DB_HOST || "",
  port: Number(process.env.GETDB_PORT) || "",
  database: process.env.GETDB_DATABASE || "",
  options: {
    encrypt: false,
    trustServerCertificate: true
  },
  pool: {
    max: 10,
    min: 0,
    idleTimeoutMillis: 30000
  }
};


export const dbPushDatabase = {
  user: "LOVELYPAYROLL",
  password: "LOVELYPAYROLL_JUN2026",
  connectString: "203.95.216.182:1557/AVT07P",
};



export const dbPushMysql = {
  user: "root",
  password: "karthick10124",
  host: "localhost",
  port: 3306,
  database: "lovely",
}


export async function getOracleSqlConnect(dbConfig) {

  let connection;
  try {
    connection = await oracledb.getConnection({
      user: dbConfig.user,
      password: dbConfig.password,
      connectString: dbConfig.connectString
    });
    console.log("Oracle Db Connected");

    return connection


  } catch (err) {
    return console.log(err);

  }
}


export async function getMsSqlPoolConnect(dbConfig) {
  try {
    const pool = await new sql.ConnectionPool(dbConfig).connect();
    console.log("MSSQL Pool Connected");
    return pool;
  } catch (err) {
    console.error("MSSQL Pool Connection Failed:", err);
    throw err;
  }
}

export async function getMySqlConnect(dbConfig) {
  try {
    const pool = await new mysql.createConnection(dbConfig);
    console.log("MySQL Pool Connected");
    return pool;
  } catch (err) {
    console.error("MSSQL Pool Connection Failed:", err);
    throw err;
  }
}


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

import { chromium } from "playwright";
import { Sequelize, DataTypes, Optional } from "sequelize";
import axios from "axios";
import fs from "fs";
import path from "path";
import csv from "csv-parser";
import dotenv from "dotenv";
// import  from "unzipper";

const unzipper = require("unzipper");
import type { Entry } from "unzipper";

dotenv.config();

// Environment Variables
const KAGGLE_EMAIL = process.env.KAGGLE_EMAIL || "";
const KAGGLE_PASSWORD = process.env.KAGGLE_PASSWORD || "";
const HUBSPOT_API_KEY = process.env.HUBSPOT_API_KEY || "";
const MYSQL_USER = process.env.MYSQL_USER || "";
const MYSQL_PASSWORD = process.env.MYSQL_PASSWORD || "";
const MYSQL_DATABASE = process.env.MYSQL_DATABASE || "";

const sequelize = new Sequelize(MYSQL_DATABASE, MYSQL_USER, MYSQL_PASSWORD, {
  host: "localhost",
  dialect: "mysql",
  logging: console.log, // Enable SQL query logging
});

// Define Model
const BabyName = sequelize.define("BabyName", {
  name: { type: DataTypes.STRING(45) },
  sex: { type: DataTypes.STRING(10) },
});

// Login and Download CSV
async function loginAndDownloadCSV() {
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext();
  const page = await context.newPage();
  const downloadPath = path.resolve(__dirname, "downloads");
  fs.mkdirSync(downloadPath, { recursive: true });

  try {
    await page.goto(
      "https://www.kaggle.com/account/login?phase=emailSignIn&returnUrl=%2F"
    );
    console.debug("Putting in email and password");
    await page.fill('input[name="email"]', KAGGLE_EMAIL);
    await page.fill('input[name="password"]', KAGGLE_PASSWORD);
    await page.click('button[type="submit"]');
    await page.waitForSelector('button[data-menutarget="true"]', {
      timeout: 50000,
    });
    console.debug("Logged in");

    await page.goto(
      "https://www.kaggle.com/datasets/thedevastator/us-baby-names-by-year-of-birth"
    );
    await page.click('button:has-text("Download")');
    console.debug("Download clicked");
    const [download] = await Promise.all([
      page.waitForEvent("download"),
      page.click('li:has-text("Download as zip")'),
    ]);

    const zipFilePath = path.join(
      downloadPath,
      await download.suggestedFilename()
    );
    await download.saveAs(zipFilePath);
    console.log(`CSV downloaded successfully to: ${zipFilePath}`);

    return zipFilePath;
  } catch (error) {
    console.error("Error during download:", error);
    return null;
  } finally {
    await browser.close();
  }
}

// Extract CSV from Zip
// async function extractCSV(zipFilePath: fs.PathLike) {
//   const csvFilePath = path.resolve(
//     __dirname,
//     "downloads",
//     "babyNamesUSYOB-full.csv"
//   );

//   return new Promise((resolve, reject) => {
//     fs.createReadStream(zipFilePath)
//       .pipe(unzipper.Parse())
//       .on(
//         "entry",
//         (entry: {
//           path: string;
//           pipe: (arg0: fs.WriteStream) => {
//             (): any;
//             new (): any;
//             on: {
//               (arg0: string, arg1: () => void): {
//                 (): any;
//                 new (): any;
//                 on: {
//                   (arg0: string, arg1: (reason?: any) => void): void;
//                   new (): any;
//                 };
//               };
//               new (): any;
//             };
//           };
//           autodrain: () => void;
//         }) => {
//           if (entry.path === "babyNamesUSYOB-full.csv") {
//             console.debug("reached");
//             entry
//               .pipe(fs.createWriteStream(csvFilePath))
//               .on("finish", () => resolve(csvFilePath))
//               .on("error", reject);
//           } else {
//             entry.autodrain();
//           }
//         }
//       )
//       .on("error", reject);
//   });
// }
async function extractCSV(zipFilePath: string): Promise<string> {
  const csvFilePath = path.resolve(
    __dirname,
    "downloads",
    "babyNamesUSYOB-full.csv"
  );

  return new Promise((resolve, reject) => {
    fs.createReadStream(zipFilePath)
      .pipe(unzipper.Parse())
      .on("entry", (entry: Entry) => {
        if (entry.path === "babyNamesUSYOB-full.csv") {
          entry
            .pipe(fs.createWriteStream(csvFilePath))
            .on("finish", () => resolve(csvFilePath))
            .on("error", reject);
        } else {
          entry.autodrain();
        }
      })
      .on("error", reject);
  });
}

// Parse CSV and Store in MySQL
async function parseAndStoreCSV(csvFilePath: fs.PathLike) {
  await sequelize.sync();
  const names: Optional<any, string>[] | { name: any; sex: any }[] = [];

  return new Promise((resolve, reject) => {
    fs.createReadStream(csvFilePath)
      .pipe(csv())
      .on("data", (row) => names.push({ name: row["Name"], sex: row["Sex"] }))
      .on("end", async () => {
        try {
          await BabyName.bulkCreate(names);
          console.log("Data stored in MySQL database");
          resolve("Done");
        } catch (error) {
          console.error("Error storing data:", error);
          reject(error);
        }
      })
      .on("error", reject);
  });
}

// Send Data to HubSpot
async function sendToHubSpot() {
  const names = await BabyName.findAll();

  for (const name of names) {
    try {
      await axios.post(
        "https://api.hubapi.com/contacts/v1/contact",
        {
          properties: [
            { property: "name", value: name.getDataValue("name") },
            { property: "sex", value: name.getDataValue("sex") },
          ],
        },
        { headers: { Authorization: `Bearer ${HUBSPOT_API_KEY}` } }
      );
      console.log(`Sent contact to HubSpot: ${name.getDataValue("name")}`);
    } catch (error) {
      console.error("Error sending data to HubSpot:", error);
    }
  }
}

// Main Process
async function main() {
  try {
    console.log("Starting download...");
    const zipFilePath = await loginAndDownloadCSV();
    console.debug(zipFilePath);
    if (!zipFilePath) return;
    console.debug(zipFilePath);

    console.log("Extracting CSV...");
    const csvFilePath = await extractCSV(zipFilePath);
    console.debug(csvFilePath);
    console.log("Parsing and storing data in MySQL...");
    await parseAndStoreCSV(csvFilePath);

    console.log("Sending data to HubSpot...");
    await sendToHubSpot();

    console.log("Process completed successfully.");
    fs.unlinkSync(zipFilePath);
    fs.unlinkSync(csvFilePath);
    console.log("Temporary files deleted.");
  } catch (error) {
    console.error("Error in the process:", error);
  } finally {
    await sequelize.close(); // Close the database connection
  }
}

// Run the Main Process
main().catch(console.error);

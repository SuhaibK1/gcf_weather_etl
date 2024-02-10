const {Storage} = require('@google-cloud/storage');
const csv = require('csv-parser');

const {BigQuery} = require('@google-cloud/bigquery');
const bq = new BigQuery();
const datasetId = 'weather_etl';
const tableId = 'weathdata';

exports.readObservation = (file, context) => {

    const gcs = new Storage();

    const dataFile = gcs.bucket(file.bucket).file(file.name);

    dataFile.createReadStream()
    .on('error', () => {
        // Error handling
        console.error(error);
    })
    .pipe(csv())
    .on('data', (row) => {
        transformData(row, file.name);
        printDict(row);
    })
    .on('end', () => {
        // Handle end of csv
        console.log('End!');
    })
}

// Function to transform the data appropriately
function transformData(row, fileName) {
    // Array of fields that are numeric
    const numFields = ['airtemp', 'dewpoint', 'pressure', 'windspeed', 'precip1hour', 'precip6hour'];
    const intFields = ['year', 'month', 'day', 'hour', 'sky'];

    for (let key in row) {
        if (row[key] === '-9999.0') {
            row[key] = null;
        } 
        // Check the necessary fields and convert the value to a float and divide by 10
        else if (numFields.includes(key)) { 
            row[key] = parseFloat(row[key]) / 10;
        }
        // Check the necessary fields and convert the value to a int
        else if (intFields.includes(key)) { 
            row[key] = parseInt(row[key]);
        }
    }

    // Extract station identifier code from file name
    const station = fileName.split('.')[0]; // Extracts the part before the first period
    row['station'] = station;

}

// Asynchronous helper function that writes to BQ
async function writeToBq(obj) {
    // BQ expects array of objects
    var dataRows = [];
    dataRows.push(obj);

    await bq
    .dataset(datasetId)
    .table(tableId)
    .insert(dataRows)
    .then( () => {
        dataRows.forEach ( (row) => { console.log(`Inserted: ${row}`)} )
    } )
    .catch( (err) => { console.error(`ERROR: ${err}`)} )
}

// Helper functions
function printDict(row) {
    for (let key in row) {
        console.log(`${key} : ${row[key]}`);
    }

    writeToBq(row);
}
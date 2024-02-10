const {Storage} = require('@google-cloud/storage');
const csv = require('csv-parser');

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
        // Log row data
        // console.log(row);
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
    const numericFields = ['airtemp', 'dewpoint', 'pressure', 'windspeed', 'precip1hour', 'precip6hour'];

    for (let key in row) {
        if (row[key] === '-9999') {
            row[key] = null;
        } 
        // Check the necessary fields andconvert the value to a float and divide by 10
            else if (numericFields.includes(key)) { 
            row[key] = parseFloat(row[key]) / 10;
        }
    }

    // Extract station identifier code from file name
    const station = fileName.split('_')[0]; // Extracts the part before the first period
    row['station'] = station;
}


// Helper functions

function printDict(row) {
    for (let key in row) {
        console.log(`${key} : ${row[key]}`);
    }
}
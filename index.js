const {Storage} = require('@google-cloud/storage');
const csv = require('csv-parser');

exports.readObservation = (file, context) => {
    // console.log(`  Event: ${context.eventId}`);
    // console.log(`  Event Type: ${context.eventType}`);
    // console.log(`  Bucket: ${file.bucket}`);
    // console.log(`  File: ${file.name}`);

    const gcs = new Storage();

    const dataFile = gcs.bucket(file.bucket).file(file.name);

    dataFile.createReadStream()
    .on('error', () => {
        // Error handling
        console.error(error);
    })
    .pipe(csv())
    .on('data', (row) => {
        // Log row data
        // console.log(row);
        printDict(row);
    })
    .on('end', () => {
        // Handle end of csv
        console.log('End!');
    })
}

// Helper functions

function printDict(row) {
    for (let key in row) {
        console.log(`${key} : ${row[key]}`);
    }
}
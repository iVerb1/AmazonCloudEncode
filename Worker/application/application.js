var ffmpeg = require('fluent-ffmpeg');
var inputFile = process.argv[2];

var fileExtension = inputFile.split('.').pop();
var fileName = inputFile.split('/').pop().replace('.'+fileExtension, "");
var resultFile = './application/results/' + fileName + '_out.' + fileExtension;

ffmpeg(inputFile)
    .output(resultFile)
    .on('error', function(err) {
        console.log("An error occurred", err);
        process.send({
            status: "failed"
        });
    })
    .on('end', function() {
        process.send({
            status: "success",
            file: resultFile
        })
    })
    .renice(20)
    .run();


/**
 * Author: iVerb
 */

var http = require('http');
var multiparty = require('multiparty');
var fs = require('fs');
var express = require('express');
var app = express();
var bodyParser = require('body-parser');
var os  = require('os-utils');
var faye = require('faye');
var cp = require('child_process');

var fayeClient;
var workerId;
var controllerIp;
var initialized;

var jobs = {};

var resourceMonitoringInterval = 500;

function Job(id) {
    this.id = id;
    this.inputFile = undefined;
    this.outputFile = undefined;
    this.returnName = undefined;
    this.experiment = false;
}

function monitorResources() {
    setTimeout(function () {
        os.cpuUsage(function(cpu){
            fayeClient.publish('/workerStatistics', {
                workerId: workerId,
                cpuUsage: cpu
            });
        });
        monitorResources();
    }, resourceMonitoringInterval);
}

function performJob(job) {

    var child = cp.fork('./application/application.js', [job.inputFile]);
    child.on('message', function (message) {

        if (message.status === "success") {
            console.log("Job " + job.id + " succeeded" + ".");
            job.outputFile = message.file;

            fayeClient.publish('/jobStatus', {
                status: "success",
                workerId: workerId,
                jobId: job.id,
                urlPathToFile: '/results/' + job.id
            })
        }
        else if (message.status === "failed") {
            console.log("Job " + job.id + " failed during processing.");
            fayeClient.publish('/jobStatus', {
                status: "failed",
                jobId: job.id
            });
        }
    });

    fayeClient.publish('/jobStatus', {
        status: "running",
        workerId: workerId,
        jobId: job.id
    });
}

//Starting server to listen to requests from the controller and the client.
app.use(function (req, res, next) {
    // Website you wish to allow to connect
    res.setHeader('Access-Control-Allow-Origin', '*');

    // Request methods you wish to allow
    res.setHeader('Access-Control-Allow-Methods', 'POST');

    // Request headers you wish to allow
    res.setHeader('Access-Control-Allow-Headers', 'X-Requested-With, content-type');

    // Pass to next layer of middleware
    next();
});

app.get('/logs', function(req, res) {
    var logFile = "./server.log";
    var logStat = fs.statSync(logFile);
    fs.readFile(logFile, function (err, data) {
        res.writeHead(200, {
            'Content-Type': 'text/plain',
            'Content-Length': logStat.size
        });
        res.write(data, 'binary');
        res.end(data);
    });
});

app.get('/results/*', function(req, res) {

    var jobId = req.url.replace("/results/", "");
    var job = jobs[jobId];

    console.log("Client obtaining result for job " + jobId);

    var jobResultFile = job.outputFile;
    var jobResultStat = fs.statSync(jobResultFile);
    fs.readFile(jobResultFile, function (err, data) {
        res.writeHead(200, {
            'Content-Type': 'application/download',
            'Content-Length': jobResultStat.size,
            'Content-Disposition': 'attachment; filename=' + job.returnName
        });
        res.write(data, 'binary');
        res.end(data);

        if (!job.experiment) {
            fs.unlink(job.inputFile);
        }
        fs.unlink(job.outputFile);
    });
});

app.use("/initialize", bodyParser.json());
app.post('/initialize', function(req, res){

    workerId = req.body.workerId;
    controllerIp = req.body.controllerIp;
    initialized = true;

    console.log("Initializing. Worker id: " + workerId + " and controller IP: " + controllerIp);

    fayeClient = new faye.Client("http://"+controllerIp+req.body.fayePath);
    monitorResources();

    res.writeHead(200);
    res.end();
});

app.post('/upload', function(req, res){

    var form = new multiparty.Form();
    form.parse(req, function(err, fields, files) {

        var jobId = fields.jobId[0];
        var job = new Job(jobId);

        jobs[jobId] = job;

        console.log("retrieving data from client for job " + jobId + ".");

        var filePath = Object.keys(files)[0];
        var extension = filePath.split('.').pop();

        job.returnName = filePath.split('/').pop();
        var newPath = "./application/uploads/"+jobId+"."+extension;

        fs.rename(files[filePath][0].path, newPath, function(err) {
            if (err) {
                console.log("Job " + job.id + " failed to start.");
                fayeClient.publish('/jobStatus', {
                    status: "failed",
                    jobId: job.id
                });
            }
            else {
                console.log("saved " + filePath + " as: " + newPath + ".");
                job.inputFile = newPath;
                performJob(job);
            }
        });

        res.writeHead(200, {'Content-Type': 'text/html'});
        res.end();
    });
});

app.use("/experiment", bodyParser.json());
app.post('/experiment', function(req, res){
    var job = new Job(req.body.jobId);
    job.experiment = true;
    job.inputFile = "./application/experiments/" + req.body.inputFile;
    job.returnName = job.inputFile.split("/").pop();

    jobs[req.body.jobId] = job;

    console.log("Starting experiment " + job.id);

    performJob(job);

    res.writeHead(200, {'Content-Type': 'text/html'});
    res.end();
});

app.post('/poll', function(req, res) {
    res.writeHead(200, {'Content-Type': 'application/json'});
    res.end(JSON.stringify({
        initialized: initialized
    }, null, 2));
});

app.listen(80);
/**
 * Author: iVerb
 */

var aws = require('aws-sdk');
var fs = require('fs');
var http = require('http');
var express = require('express');
var app = express();
var bodyParser = require('body-parser');
var network = require('network');
var faye = require('faye');
var Stopwatch = require("timer-stopwatch");

aws.config.region = 'eu-central-1';
aws.config.update({accessKeyId: 'none', secretAccessKey: 'none'});

var fayePath = '/faye';
var fayeNodeAdapter = new faye.NodeAdapter({mount: fayePath});
var fayeClient = fayeNodeAdapter.getClient();

var ec2 = new aws.EC2();
var workerImage = 'ami-27aaba4b';

var controllerIp;

var workers = {};               //all workers in the system (except workers shutting down)
var idleWorkers = {};           //running workers that are idle
var activeWorkers = {};         //running workers that are active on some job
var runningWorkers = {};        //all workers that are running and initialized (and not shutting down)
var pendingWorkers = {};        //all workers that are leased, but not yet initialized
var workersShuttingDown = {};

var jobIdGenerator = 0;
var jobs = {};                  //all jobs
var runningJobs = {};           //all jobs that are being executed
var pendingJobs = {};           //all jobs that are allocated to workers, but haven't started.
var succeededJobs = {};         //all succeeded jobs
var failedJobs = {};            //all failed jobs
var abortedJobs = {};           //all aborted jobs due to worker failure
var jobQueue = [];              //all jobs to be allocated to workers.

//global timer
var globalTimer = new Stopwatch();

//worker initialization
var maxWorkerInitializationAttempts = 10;
var workerInitializationAttemptInterval = 3000;

//Thresholds, timeouts and intervals
var jobQueueProcessingInterval = 1000;
var resourceScalingInterval = 2000;
var workerPollingInterval = 6000;
var statisticsLoggingInterval = 1000;
var workerPollingResponseTimeout = 3000;
var scalingDownIdleTimeThreshold = 20000;

//resource scaling
var useResourceScaling = true;
var minRunningWorkers = 1;
var initialWorkers = 6;
var maxWorkersAlive = 9; //Imposed constraint by the AWS free tier
var minIdleWorkers = 0;
var queuedJobUpScalingFactor = 0.2;

//load balancing
var maxJobsOnWorker = 2;
var numRecentCpuMeasurements = 2;
var WorkerAllocationMaxCpu = 0.7;

//experiments
var experimentRunning = false;
var experimentStartTime = undefined;
var globalStatsFile = './experimentStatistics/globalStats.csv';
var completionTimesStatsFile = './experimentStatistics/completionTimes.csv';
var jobAllocationTimesStatsFile = './experimentStatistics/jobAllocationTimes.csv';
var chargedTimeStatsFile = './experimentStatistics/chargedTime.csv';
var workersStatsFile = './experimentStatistics/workersStats.csv';


function Worker(workerId) {
    this.id = workerId;
    this.ip = undefined;
    this.jobs = {};
    this.pendingJobs = {};
    this.runningJobs = {};
    this.avgCpu = 0;
    this.recentCPU = [];
    this.idle = true;
    this.idleTimeStopwatch = new Stopwatch();
    this.isBeingPolled = false;
    this.launchTime = undefined;
}

function Job(jobId) {
    this.id = jobId;
    this.startTime = undefined;
    this.allocatedWorker = undefined;
    this.resultUrl = undefined;
}

function deallocateJob(job) {
    var allocatedWorker = job.allocatedWorker;
    if (allocatedWorker !== undefined) {
        delete allocatedWorker.pendingJobs[job.id];
        delete allocatedWorker.runningJobs[job.id];
        delete allocatedWorker.jobs[job.id];
    }

    delete runningJobs[job.id];
    delete pendingJobs[job.id];
}

function allocateJob(job, allocatedWorker) {
    allocatedWorker.jobs[job.id] = job;
    allocatedWorker.pendingJobs[job.id] = job;
    pendingJobs[job.id] = job;

    job.allocatedWorker = allocatedWorker;

    activeWorkers[allocatedWorker.id] = allocatedWorker;

    delete idleWorkers[allocatedWorker.id];
    allocatedWorker.idle = false;
    allocatedWorker.idleTimeStopwatch.stop();
    allocatedWorker.idleTimeStopwatch.reset();
}

function checkIfWorkerIsIdle(worker) {
    if (numKeys(worker.jobs) == 0) {
        delete activeWorkers[worker.id];
        idleWorkers[worker.id] = worker;
        worker.idle = true;
        worker.idleTimeStopwatch.start();
    }
}

function numKeys(obj) {
    return Object.keys(obj).length;
}

function getElapsedTime(stopwatch) {
    return stopwatch.ms;
}

function initializeWorker(worker, callback, attempt) {

    if (attempt === undefined) {
        attempt = 1;
    }

    var data = JSON.stringify({
        workerId: worker.id,
        controllerIp: controllerIp,
        fayePath: fayePath
    }, null, -1);

    var request = http.request({
            host: worker.ip,
            path: '/initialize',
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Content-Length': Buffer.byteLength(data)
            }},
        function(res) {
            if (res.statusCode === 200) {
                console.log("Worker initialized: " + worker.id + ".");
                if (callback !== undefined) {
                    callback();
                }
            }
        }
    );

    request.on('error', function(e) {
        if (attempt < maxWorkerInitializationAttempts) {
            setTimeout(function () {
                initializeWorker(worker, callback, attempt++);
            }, workerInitializationAttemptInterval);
        }
        else {
            console.log("Worker timed out too often during initialization: " + worker.id + ". Removing worker.");
            deregisterWorker(worker);
        }
    });

    request.write(data);
    request.end();
}

function launchNewWorker() {

    var params = {
        ImageId: workerImage,
        InstanceType: 't2.micro',
        MinCount: 1, MaxCount: 1,
        SecurityGroups: ["FullAccess"],
        KeyName: "keypair1"
    };

    ec2.runInstances(params, function(err, data) {

        if (err) {
            console.log("Could not create worker instance. " + err);
            return;
        }

        var instanceId = data.Instances[0].InstanceId;
        console.log("Created worker instance " + instanceId + ". Launching...");

        var worker = new Worker(instanceId);
        workers[instanceId] = worker;
        pendingWorkers[instanceId] = worker;

        ec2.waitFor('instanceRunning', {InstanceIds: [instanceId]}, function(err, data) {
            if (err) {
                console.log("Error occurred while launching worker " + worker.id);
            }
            else {
                console.log("Worker running: ", instanceId + ".");

                worker.ip = data.Reservations[0].Instances[0].PublicIpAddress;

                initializeWorker(worker, function() {
                    runningWorkers[worker.id] = worker;
                    delete pendingWorkers[worker.id];
                    checkIfWorkerIsIdle(worker);
                    worker.launchTime = getElapsedTime(globalTimer);
                });
            }
        });
    });
}

function deregisterWorker(worker) {

    delete workers[worker.id];
    delete activeWorkers[worker.id];
    delete idleWorkers[worker.id];
    delete pendingWorkers[worker.id];
    delete runningWorkers[worker.id];
    workersShuttingDown[worker.id] = worker;

    ec2.terminateInstances({InstanceIds: [worker.id]}, function(err) {
        if (err) {
            console.log("Error occurred killing worker: " + worker.id + ".");
            deregisterWorker(worker);
        }
        else {
            var waitForWorkerToTerminate = function() {
                ec2.waitFor('instanceTerminated', {InstanceIds: [worker.id]}, function (err, data) {
                    if (err) {
                        console.log("Error occurred while waiting for worker to die: " + worker.id);
                        waitForWorkerToTerminate();
                    }
                    else {
                        console.log("Worker successfully killed: " + worker.id + ".");
                        delete workersShuttingDown[worker.id];
                    }
                });
            };
            waitForWorkerToTerminate();

            if (experimentRunning) {
                logElapsedTime(worker, "worker killed");
            }
        }
    });
}

function getAvailableWorkers() {
    var availableWorkers = [];
    for (var workerId in runningWorkers) {
        var worker = runningWorkers[workerId];
        //console.log(worker.avgCpu + " " + numKeys(worker.jobs));
        if (worker.avgCpu < WorkerAllocationMaxCpu && numKeys(worker.jobs) < maxJobsOnWorker) {
            availableWorkers.push(runningWorkers[workerId]);
        }
    }
    return availableWorkers;
}

function processJobQueue() {

    if (jobQueue.length > 0) {

        var availableWorkers = getAvailableWorkers();

        availableWorkers.sort(function(w1, w2) {
            if (w1.idle && w2.idle) {
                return getElapsedTime(w1.idleTimeStopwatch) - getElapsedTime(w2.idleTimeStopwatch);
            }
            else {
                return numKeys(w1.jobs) - numKeys(w2.jobs);
            }
        });

        if (availableWorkers.length > 0) {

            var currentWorker = 0;
            for (var i = 0; i < jobQueue.length; i++) {

                var job = jobQueue[0];
                var allocatedWorker = availableWorkers[currentWorker];

                console.log("Assigning job " + job.id + " to worker " + allocatedWorker.id + ".");

                allocateJob(job, allocatedWorker);

                jobQueue.shift();


                if (numKeys(allocatedWorker.jobs) >= maxJobsOnWorker) {
                    availableWorkers.splice(currentWorker, 1);
                    if (availableWorkers.length == 0) {
                        return;
                    }
                }
                else {
                    currentWorker++;
                }

                if (currentWorker === availableWorkers.length) {
                    currentWorker = 0;
                }
            }
        }
    }
}

function scaleResources() {
    if (useResourceScaling) {

        var numWorkersAlive = numKeys(workers) + numKeys(workersShuttingDown);
        if (numWorkersAlive < maxWorkersAlive) {

            var scalingPotential = maxWorkersAlive - numWorkersAlive;
            var suggestedNewWorkers1 = 0;
            var suggestedNewWorkers2 = 0;
            var availableWorkers = getAvailableWorkers();
            if (availableWorkers.length === 0) {
                suggestedNewWorkers1 = 1 + Math.floor(jobQueue.length * queuedJobUpScalingFactor);
                if (suggestedNewWorkers1 > 0) {
                    console.log("Suggesting to scale up. No workers available for allocating " + jobQueue.length + " jobs.");
                }
            }

            //if too little workers are running due to worker crashes
            if (numKeys(runningWorkers) < minRunningWorkers || numKeys(idleWorkers) < minIdleWorkers) {
                suggestedNewWorkers2 = Math.max(minRunningWorkers - numKeys(runningWorkers), minIdleWorkers - numKeys(idleWorkers));

                if (numKeys(workers) < minRunningWorkers)
                    console.log("Suggesting to scale up. Too few workers are running.");

                if (numKeys(idleWorkers) < minIdleWorkers)
                    console.log("Suggesting to scale up. Too few idle workers are running.");
            }

            var suggestedNewWorkers = Math.max(0, Math.max(suggestedNewWorkers1, suggestedNewWorkers2) - numKeys(pendingWorkers));
            var numNewWorkers = Math.min(scalingPotential, suggestedNewWorkers);
            if (numNewWorkers > 0) {
                console.log("Scaling up by " + numNewWorkers + " workers.");
                for (var i = 0; i < numNewWorkers; i++) {
                    launchNewWorker();
                }
            }
        }

        //scaling down
        for (var index in idleWorkers) {
            var worker = idleWorkers[index];
            if (getElapsedTime(worker.idleTimeStopwatch) > scalingDownIdleTimeThreshold
                && numKeys(idleWorkers) > minIdleWorkers
                && numKeys(runningWorkers) > minRunningWorkers) {
                    console.log("Killing worker: " + worker.id + ". It has been idle for too long.");
                    deregisterWorker(worker);
            }
        }
    }
}

function pollRunningWorkers() {

    for (var workerId in runningWorkers) {

        (function(worker) {

            if (!worker.isBeingPolled) {

                worker.isBeingPolled = true;

                var request = http.request({
                    host: worker.ip,
                    path: '/poll',
                    method: 'POST'
                }, function (response) {
                    response.setEncoding('utf8');
                    response.on('data', function (data) {
                        worker.isBeingPolled = false;
                        var response = JSON.parse(data);
                        if ("initialized" in response) {
                            if (!response.initialized) {
                                console.log("Worker died and restarted: " + worker.id + ". Recovering...");
                                abortJobs(worker);

                                delete runningWorkers[worker.id];
                                pendingWorkers[worker.id] = worker;

                                initializeWorker(worker, function () {
                                    delete pendingWorkers[worker.id];
                                    runningWorkers[worker.id] = worker;
                                });
                            }
                        }
                    });
                });

                request.setTimeout(workerPollingResponseTimeout, function () {
                    console.log("Worker died and failed to restart:  " + worker.id + ". Recovering...");
                    abortJobs(worker);
                    deregisterWorker(worker);
                    request.abort();
                });

                request.on('error', function (e) {
                    //error handling
                });

                request.end();
            }
        })(runningWorkers[workerId]);
    }
}

function abortJobs(worker) {
    for (var jobId in worker.jobs) {
        var job = worker.jobs[jobId];

        console.log("Aborting job " + jobId + " because worker " + job.allocatedWorker.id + " crashed.");

        deallocateJob(job);
        abortedJobs[jobId] = job;
    }
}

function runOnTimeInterval(callback, timeout) {
    callback();
    setTimeout(function () {
        runOnTimeInterval(callback, timeout);
    }, timeout);
}

function logExperimentStatistics() {
    if (experimentRunning) {
        var time = formatTime(getElapsedTime(globalTimer));
        var activeJobs = numKeys(runningJobs) + numKeys(pendingJobs) + jobQueue.length;
        var leasedWorkers = numKeys(runningWorkers) + numKeys(workersShuttingDown) + numKeys(pendingWorkers);
        fs.appendFile(globalStatsFile, time
            + ';' + activeJobs
            + ';' + leasedWorkers
            + ';' + numKeys(runningWorkers)
            + ';' + numKeys(activeWorkers)
            + ';' + numKeys(idleWorkers)
            + ';' + jobQueue.length
            + '\n', function (err) {
            if (err) {
                console.log("An error occurred writing global statistics.");
            }
        });

        for (var workerId in runningWorkers) {
            var worker = runningWorkers[workerId];
            fs.appendFile(workersStatsFile, time + ';' + worker.id + ';' + worker.avgCpu + ';' + numKeys(worker.jobs) + '\n', function (err) {
                if (err) {
                    console.log("An error occurred writing worker statistics: " + worker.id);
                }
            });
        }
    }
}

function logElapsedTime(worker, context) {
    var startTime = Math.max(experimentStartTime, worker.launchTime);
    var endTime = getElapsedTime(globalTimer);
    var chargedTime = endTime - startTime;
    fs.appendFile(chargedTimeStatsFile, worker.id
        + ';' + formatTime(startTime)
        + ';' + formatTime(endTime)
        + ';' + formatAbsoluteTime(chargedTime)
        + ";" + context + "\n", function (err) {
            if (err) {
                console.log("An error occurred writing charged time statistics for worker " + worker.id);
            }
    });
}

function formatTime(time) {
    return formatAbsoluteTime(time - experimentStartTime);
}

function formatAbsoluteTime(time) {
    var val = time / 1000;
    return val.toFixed(2).replace(".",",");
}

//create web server for handling client and worker requests
app.use(bodyParser.json());

app.get('/', function(req, res) {
    res.writeHead(200, {'Content-Type': 'text/html'});
    res.end(fs.readFileSync('./client/request.html'));
});

app.get('/logs', function(req, res) {
    var responseFilePath = "./server.log";
    var stat = fs.statSync(responseFilePath);
    fs.readFile(responseFilePath, function (err, data) {
        res.writeHead(200, {
            'Content-Type': 'text/plain',
            'Content-Length': stat.size
        });
        res.write(data, 'binary');
        res.end(data);
    });
});

app.get('/experimentStatistics/*', function(req, res) {
    var responseFilePath = "."+req.url;
    var stat = fs.statSync(responseFilePath);
    fs.readFile(responseFilePath, function (err, data) {
        res.writeHead(200, {
            'Content-Type': 'text/plain',
            'Content-Length': stat.size
        });
        res.write(data, 'binary');
        res.end(data);
    });
});

app.get('/client/*', function(req, res) {
    var path = req.url.replace("/client","");
    res.writeHead(200, {'Content-Type': 'text/plain'});
    res.end(fs.readFileSync('./client' + path));
});

app.post('/clientRequest', function(req, res) {
    var jobId = jobIdGenerator++;
    var job = new Job(jobId);

    job.startTime = getElapsedTime(globalTimer);

    console.log("Client requested new job: " + job.id + ". Adding to job queue.");

    jobs[job.id] = job;
    jobQueue.push(job);
    processJobQueue();

    res.writeHead(200, {'Content-Type': 'text/plain'});
    res.end(JSON.stringify({jobId: job.id}, null, 2));
});

app.get('/workerIp', function(req, res) {

    var jobId = req.query.jobId;
    var job = jobs[jobId];

    res.writeHead(200, {'Content-Type': 'text/plain'});

    var responseJson;
    if (jobId in pendingJobs || jobId in runningJobs) {
        responseJson = {
            jobAllocated: true,
            jobId: jobId,
            workerIp: job.allocatedWorker.ip
        };

        if (experimentRunning) {
            var responseTime = formatAbsoluteTime(getElapsedTime(globalTimer) - job.startTime);
            var time = formatTime(getElapsedTime(globalTimer));
            fs.appendFile(jobAllocationTimesStatsFile, time + ";" + responseTime + '\n', function (err) {
                if (err) {
                    console.log("An error occurred writing worker statistics: " + job.id + ".");
                }
            });
        }
    }
    else {
        responseJson = {
            jobAllocated: false
        };
    }
    res.end(JSON.stringify(responseJson, null, 2));
});

app.get('/jobResult', function(req, res) {

    var jobId = req.query.jobId;
    var job = jobs[jobId];

    res.writeHead(200, {'Content-Type': 'text/plain'});

    var finished = false;
    var status = undefined;
    var log = true;

    if (job.id in succeededJobs) {
        finished = true;
        status = "succeeded";

        if (experimentRunning) {
            var completionTime = formatAbsoluteTime(getElapsedTime(globalTimer) - job.startTime);
            var time = formatTime(getElapsedTime(globalTimer));
            fs.appendFile(completionTimesStatsFile, time  + ';' + completionTime + '\n', function (err) {
                if (err) {
                    console.log("An error occurred writing worker statistics: " + job.id + ".");
                }
            });
        }
    }
    else if (job.id in failedJobs) {
        finished = true;
        status = "failed";
    }
    else if (job.id in abortedJobs) {
        finished = true;
        status = "workerFailed";
    }
    else {
        log = false;
    }

    if (log) {
        console.log("Sending job result for job " + jobId + " back to client.");
    }

    res.end(JSON.stringify({
        finished: finished,
        status: status,
        resultUrl: job.resultUrl
    }, null, 2));
});

app.post('/experiment/simulateWorkerFailure', function(req, res) {

    if (experimentRunning) {

        var failureFraction = req.body.failureFraction;
        var numFailures = Math.floor(failureFraction * numKeys(runningWorkers));

        if (numFailures > 0) {
            var workerFailures = [];

            for (var workerId in runningWorkers) {
                workerFailures.push(workerId);
            }

            workerFailures = workerFailures.slice(0, numFailures);
            console.log("Simulating worker failure for " + numFailures + " workers: " + workerFailures);

            ec2.terminateInstances({InstanceIds: workerFailures}, function (err) {
                if (err) {
                    console.log("Error occurred simulating worker failure for workers: " + workerFailures);
                }
                else {
                    console.log("Successfully simulated worker failure for workers: " + workerFailures);
                }
            });
        }

        res.end();
    }
});

app.post('/experiment/start', function(req, res) {

    console.log("Starting experiment");

    experimentRunning = true;
    experimentStartTime = getElapsedTime(globalTimer);

    var errorFunc = function(err) {
        if (err) {
            console.log("Error occurred creating statistics files:" + err + ". Exiting.");
            process.exit(1);
        }
    };

    fs.open(globalStatsFile, 'w', errorFunc);
    fs.appendFile(globalStatsFile, "Time ; Active Jobs ; Leased Workers ; Running Workers ; Active Workers ; Idle Workers ; Job queue length \n", errorFunc);
    fs.open(jobAllocationTimesStatsFile, 'w', errorFunc);
    fs.appendFile(jobAllocationTimesStatsFile, "Time ; Job allocation time \n", errorFunc);
    fs.open(completionTimesStatsFile, 'w', errorFunc);
    fs.appendFile(completionTimesStatsFile, "Time ; Completion time \n", errorFunc);
    fs.open(chargedTimeStatsFile, 'w', errorFunc);
    fs.appendFile(chargedTimeStatsFile, "Worker ; Start time ; End time ; Charged time ; Context \n", errorFunc);
    fs.open(workersStatsFile, 'w', errorFunc);
    fs.appendFile(workersStatsFile, "Time ; Worker id ; CPU usage ; Number of jobs \n", errorFunc);

    setInterval(logExperimentStatistics, statisticsLoggingInterval);

    res.writeHead(200);
    res.end();
});

app.post('/experiment/end', function(req, res) {

    console.log("Ending experiment");

    experimentRunning = false;

    for (var workerId in runningWorkers) {
        logElapsedTime(workers[workerId], "Experiment ended");
    }

    res.writeHead(200);
    res.end();
});

fayeClient.subscribe('/jobStatus', function(message) {
    var job;
    if (message.status === "running") {
        job = jobs[message.jobId];
        if (job.id in pendingJobs) {
            console.log("Job " + message.jobId + " running.");

            delete runningWorkers[message.workerId].pendingJobs[message.jobId];
            delete pendingJobs[message.jobId];
            runningWorkers[message.workerId].runningJobs[message.jobId] = job;
            runningJobs[message.jobId] = job;
        }
    }
    else if (message.status === "success") {
        console.log("Job " + message.jobId + " succeeded.");

        job = jobs[message.jobId];
        job.resultUrl = job.allocatedWorker.ip + message.urlPathToFile;
        succeededJobs[job.id] = job;

        deallocateJob(job);
        checkIfWorkerIsIdle(job.allocatedWorker);
        processJobQueue();
    }
    else if (message.status === "failed") {
        console.log("Job " + message.jobId + " failed.");
        job = jobs[message.jobId];
        failedJobs[job.id] = job;

        deallocateJob(job);
        checkIfWorkerIsIdle(job.allocatedWorker);
        processJobQueue();
    }
});

fayeClient.subscribe('/workerStatistics', function(message) {
    var workerId = message.workerId;
    var worker = runningWorkers[workerId];
    var cpuUsage = message.cpuUsage;

    worker.recentCPU.push(cpuUsage);
    if (worker.recentCPU.length > numRecentCpuMeasurements) {
        worker.recentCPU.shift();
    }

    var cpuSum = 0;
    for (var index in worker.recentCPU) {
        cpuSum += worker.recentCPU[index];
    }
    worker.avgCpu = cpuSum / numRecentCpuMeasurements;
});

//initialize
network.get_public_ip(function (err, ip) {
    if (err) {
        console.log("Cannot retrieve public IP of controller. Exiting.");
        process.exit(1);
    }
    else {
        console.log("Controller ip: " + ip);
        controllerIp = ip;

        globalTimer.start();

        //continuously process the job queue
        setInterval(processJobQueue, jobQueueProcessingInterval);

        //continuously check if resource scaling is necessary
        setInterval(scaleResources, resourceScalingInterval);

        //start polling running workers
        setInterval(pollRunningWorkers, workerPollingInterval);

        var server = http.createServer(app);
        fayeNodeAdapter.attach(server);
        server.listen(80);

        for (var i = 0; i < initialWorkers; i++) {
            launchNewWorker();
        }
    }
});
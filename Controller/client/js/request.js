var jobAllocationPollingInterval = 3000;
var jobResultPollingInterval = 3000;

function Job(requestApprovedCallback, successCallback) {
    this.id = undefined;
    this.workerIp = undefined;
    this.resultUrl = undefined;
    this.workerIpReceivedCallback = requestApprovedCallback;
    this.successCallback = successCallback;
    this.finished = false;
}

function performMultiple(callback, delay, amount, finishedCallback) {
    if (amount > 0) {
        callback();
        setTimeout(function () {
            performMultiple(callback, delay, amount - 1, finishedCallback);
        }, delay);
    }
    else if (finishedCallback !== undefined) {
        finishedCallback();
    }
}

function startExperiment() {
    var experiment = $("#experiments").find(":selected").val();
    var experimentCallback;
    var jobs = [];

    var jobFunction = function (inputFile) {
        var requestApprovedCallback = function () {
            var job = this;
            $.ajax({
                url: "http://" + job.workerIp + "/experiment",
                method: 'POST',
                contentType: 'application/json',
                data: JSON.stringify({jobId: job.id, inputFile: inputFile}, null, 2),
                success: function () {
                    console.log("Experiment " + job.id + " placed at worker " + job.workerIp + ". Waiting for response from controller...");
                    waitForJobResult(job);
                }
            });
        };
        var job = new Job(requestApprovedCallback);
        jobs.push(job);
        requestJob(job);
    };

    if (experiment === "tiny") {
        experimentCallback = function() {
            performMultiple(function() { jobFunction("small.mp4") }, 1000, 5);
        }
    }

    if (experiment === "medium") {
        experimentCallback = function() {
            performMultiple(function() { jobFunction("small.mp4") }, 1500, 50);
        }
    }

    if (experiment === "workerFailure") {
        experimentCallback = function() {
            performMultiple(function() { jobFunction("small.mp4") }, 2500, 15, function() {
                $.ajax({
                    url: "/experiment/simulateWorkerFailure",
                    method: 'POST',
                    contentType: 'application/json',
                    data: JSON.stringify({failureFraction: 0.5}, null, 2),
                    success: function() {
                        performMultiple(function() { jobFunction("small.mp4") }, 2500, 15);
                    }
                });
            });
        };
    }

    $.ajax({
        url: "/experiment/start",
        method: 'POST',
        success: function () {
            console.log("Starting experiment: " + experiment);
            experimentCallback();
        }
    });
}

function requestUpload() {
    var file = $('#fileChooser').val();
    if (file == null || file == "") {
        return;
    }

    $.each($('#fileChooser')[0].files, function (i, file) {
        var requestApprovedCallback = function() {
            uploadData(job, file);
        };
        var successCallback = function() {
            window.location.href = "http://"+this.resultUrl;
        };
        var job = new Job(requestApprovedCallback, successCallback);
        requestJob(job)
    });
}

function requestJob(job) {

    console.log("Requesting work...");

    $.ajax({
        url: "/clientRequest",
        method: 'POST',
        success: function(data) {
            var response = JSON.parse(data);
            job.id = response.jobId;
            waitForWorkerIp(job);
        }
    });
}

function waitForWorkerIp(job) {
    $.ajax({
        url: "/workerIp",
        method: 'GET',
        data: "jobId="+job.id,
        success: function(data) {
            var response = JSON.parse(data);
            if (response.jobAllocated) {
                console.log("Request approved for job " + response.jobId);
                job.workerIp = response.workerIp;
                job.workerIpReceivedCallback();
            }
            else {
                setTimeout(function() {waitForWorkerIp(job)}, jobAllocationPollingInterval);
            }
        }
    });
}

function uploadData(job, file) {

    console.log("Uploading data for job " + job.id + " to worker " + job.workerIp + ". File: " + file.name);

    var formData = new FormData();
    formData.append(file.name, file);
    formData.append("jobId", job.id);

    $.ajax({
        url: "http://" + job.workerIp + "/upload",
        data: formData,
        cache: false,
        contentType: false,
        processData: false,
        method: 'POST',
        statusCode: {
            200: function() {
                console.log("File uploaded for job " + job.id + ". Waiting for response from controller...");
                waitForJobResult(job);
            },
            400: function() {
                console.log("Upload failed for job " + job.id);
            }
        }
    });
}

function waitForJobResult(job) {
    $.ajax({
        url: "/jobResult",
        data: "jobId="+job.id,
        method: 'GET',
        timeout: 0,
        success: function(data) {
            var response = JSON.parse(data);
            if (response.finished) {
                console.log("Job " + job.id + " finished.");
                jobFinished(data, job);
            }
            else {
                setTimeout(function() {waitForJobResult(job)}, jobResultPollingInterval);
            }
        }
    });
}

function jobFinished(data, job) {
    var response = JSON.parse(data);

    if (response.status === "succeeded") {
        job.resultUrl = response.resultUrl;
        job.finished = true;
        if (job.successCallback !== undefined) {
            job.successCallback();
        }
    }
    else if (response.status === "failed") {
        console.log("Job " + job.id + " failed.");
    }
    else if (response.status === "workerFailed") {
        console.log("Worker failed, retrying...");
        requestJob(job)
    }
}

function stopExperiment() {

    console.log("Experiment ended.");
    $.ajax({
        url: "/experiment/end",
        method: 'POST'
    });

}

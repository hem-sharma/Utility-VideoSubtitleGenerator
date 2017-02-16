var express = require('express'),
    app = express(),
    http = require('http'),
    config = require(__dirname + '/config.json'),
    sql = require('mssql'),
    azure = require('azure-storage'),
    fs = require('fs'),
    retryOperations = new azure.ExponentialRetryPolicyFilter(),
    blobSvc = azure.createBlobService(config.mediaStorageAccountName, config.mediaStorageAccountKey).withFilter(retryOperations),
    LINQ = require('node-linq').LINQ,
    toBeProcessed = 0,
    execSync = require('sync-exec'),
    SYNC = require('sync');

console.log('vtt generator running at : ' + config.port);

//start first and set job after scheduled time in config
app.listen(config.port, function () {
    fetchRecords();
    setTimeout(function () {
        fetchRecords();
    }, config.JobSchedulingInMS);
});

function fetchRecords() {
    try {
        if (toBeProcessed === 0) {
            sql.connect("mssql://" + config.sqlUserName + ":" + config.sqlPassword + "@" + config.sqlServerHost + "/" + config.databaseName).then(function () {
                new sql.Request().query(config.fetchRecordsCmd).then(function (recordset) {
                    console.log('found ' + recordset.length + ' records')
                    toBeProcessed = recordset.length;
                    for (var item in recordset) {
                        SYNC(function () {
                            var response = processVideo(recordset[item])
                        })
                    }
                    while (toBeProcessed === 0) {
                        console.log('files pending to process in current db request: ' + toBeProcessed)
                        fetchRecords();
                    }

                }).catch(function (err) {
                    console.log(err);
                });
            });
        } else {
            console.log('processing previous records..');
        }
    } catch (e) {
        console.log(e);
    }
};

function processVideo(record, callback) {
    SYNC(function () {
        console.log('processing video having ID: ' + record.ID);
        var blobName = record.ContentBlobName,
            conSplitArray = record.AssetId.split(':'),
            containerName = 'asset-' + conSplitArray[conSplitArray.length - 1];
        var downloaded = downloadAsset(record, containerName, blobName);
    })
}

function processCallback(item, res) {
    var videoId = item.ID;
    if (res.status === 1)
        sql.connect("mssql://" + config.sqlUserName + ":" + config.sqlPassword + "@" + config.sqlServerHost + "/" + config.databaseName).then(function () {
            new sql.Request().query(config.updateRecordCmd + videoId).then(function (res) {
                console.log('VIdeo having id ' + videoId + ' processed successfully.')
            }).catch(function (err) {
                console.log(err);
            });
        });
}

function downloadAsset(record, containerName, blobName) {
    var contentUrl = record.ContentUrl,
        sourceLanguage = record.TranslateLanguage,
        destinationLanguage = record.TranslateLanguage,
        result, executed = false;

    result = blobSvc.getBlobToLocalFile(containerName, blobName, __dirname + '/contents/' + blobName, function (error, result, response) {
        if (!error) {
            console.log('downloaded video having ID: ' + record.ID)
            var loc = __dirname + '/contents/' + blobName;
            console.log('running autosub...')
            var cmd = 'autosub ' + '-S ' + sourceLanguage + ' -F vtt' + ' ' + loc;
            var response = generateVtt(loc, cmd);
            console.log('generated vtt for video having ID: ' + record.ID)

            console.log('uploading vtt for video ' + record.ID + ' to blob...')
            var location = __dirname + '/files/';
            var vttFileName = record.ID.concat('.vtt');
            uploadVTTToBlob(location, vttFileName);
            //for updating record to transcribed in database
            processCallback(record, {
                status: 1
            });
            executed = true;
            return;
        } else {
            console.log('some error occured in downloading blob having ID : ' + record.ID)
            return {
                status: 0
            };
        }
    });
    return result;
}

function uploadVTTToBlob(loc, vttFileName) {
    SYNC(function () {
        var containerCreationResponse = blobSvc.createContainerIfNotExists(config.vttContainerName, function (error, result, response) {
            if (!error) {
                console.log('container created successfully or exists already having name : ' + config.vttContainerName)
                console.log('uploading vtt file to blob having location ' + loc)
                blobSvc.createBlockBlobFromLocalFile(config.vttContainerName, vttFileName, loc + vttFileName, function (error, result, response) {
                    if (!error) {
                        console.log('uploaded file to blob having local location as ' + loc)

                    } else {
                        console.log('some error occured in uploading file having location: ' + loc)
                    }
                    toBeProcessed -= 1;
                });
            }
        });
    })
}

function generateVtt(loc, cmd) {
    console.log('running autosub for file having location: ' + loc)
    var output = execSync(cmd);
    console.log(output)
    return output;
}
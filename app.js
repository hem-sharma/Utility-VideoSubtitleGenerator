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

console.log('vtt generator running at : ' + config.port, new Date());

//blobSvc.logger.level = azure.Logger.LogLevels.DEBUG;//for enabling storage logging

app.listen(config.port, function () {
    fetchRecords();
    setInterval(function () {
        fetchRecords();
    }, config.JobSchedulingInMS);
});

function fetchRecords() {
    try {
        if (toBeProcessed === 0) {
            sql.connect("mssql://" + config.sqlUserName + ":" + config.sqlPassword + "@" + config.sqlServerHost + "/" + config.databaseName).then(function () {
                new sql.Request().query(config.fetchRecordsCmd).then(function (recordset) {
                    console.log('found ' + recordset.length + ' records', new Date())
                    toBeProcessed = recordset.length;
                    for (var item in recordset) {
                        SYNC(function () {
                            var response = processVideo(recordset[item])
                        })
                    }
                    while (toBeProcessed === 0) {
                        console.log('files pending to process in current db request: ' + toBeProcessed, new Date())
                        fetchRecords();
                    }

                }).catch(function (err) {
                    console.log(err);
                });
            });
        } else {
            console.log('recalling..\nprocessing previous records first...Will check again in: ' + config.JobSchedulingInMS + ' milliseconds ', new Date());
        }
    } catch (e) {
        console.log(e);
    }
};

function processVideo(record, callback) {
    SYNC(function () {
        console.log('processing video having ID: ' + record.ID, new Date());
        // record.AssetId='0184b983-4f0d-417b-8e0c-4c85d3e488ca';
        // record.ContentBlobName='iPhone8-InnovativeScreen_201702141318120247.mp4';
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
                console.log('VIdeo having id ' + videoId + ' processed successfully.', new Date())
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
            console.log('downloaded video having ID: ' + record.ID, new Date())
            var loc = __dirname + '/contents/' + blobName;
            console.log('running autosub...', new Date())
            var cmd = 'autosub ' + '-S ' + sourceLanguage + ' -F ' + config.SubtitleGenerationFormat + ' ' + loc;
            var response = generateVtt(loc, cmd);
            console.log('generated vtt for video having ID: ' + record.ID, new Date())

            console.log('uploading vtt for video ' + record.ID + ' to blob...', new Date())
            var location = __dirname + '/contents/';
            var vttFileName = record.ID.concat('.' + config.SubtitleGenerationFormat);
            uploadVTTToBlob(location, vttFileName);
            //for updating record to transcribed in database
            processCallback(record, {
                status: 1
            });
            executed = true;
            return;
        } else {
            console.log('some error occured in downloading blob having ID : ' + record.ID, new Date())
            console.log(error)
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
                console.log('container created successfully or exists already having name : ' + config.vttContainerName, new Date())
                console.log('uploading vtt file to blob having location ' + loc)
                blobSvc.createBlockBlobFromLocalFile(config.vttContainerName, vttFileName, loc + vttFileName, function (error, result, response) {
                    if (!error) {
                        console.log('uploaded file to blob having local location as ' + loc, new Date())
                        deleteFile(__dirname + '/contents/' + record.ContentBlobName)
                    } else {
                        console.log('some error occured in uploading file having location: ' + loc, new Date())
                    }
                    toBeProcessed -= 1;
                });
            }
        });
    })
}

function generateVtt(loc, cmd) {
    console.log('running autosub for file having location: ' + loc, new Date())
    var output = execSync(cmd);
    console.log(output)
    return output;
}

function deleteFile(loc) {
    fs.unlinkSync(loc);
    console.log('successfully deleted ' + loc, new Date());
}
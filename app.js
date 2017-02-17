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

console.log('subtitle generator running at : ' + config.port, new Date());

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
                        var response = processVideo(recordset[item])
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
}

function processVideo(record, callback) {
    console.log('processing video having ID: ' + record.ID, new Date());
    var blobName = record.ContentBlobName,
        conSplitArray = record.AssetId.split(':'),
        containerName = 'asset-' + conSplitArray[conSplitArray.length - 1];
    var downloaded = downloadAsset(record, containerName, blobName);
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
            console.log('running autosub...', new Date())

            //rename any special character in blobname for autosub
            var afterRenaming = processForNaming(blobName);
            var loc = __dirname + '/contents/' + afterRenaming;
            var cmd = 'autosub ' + '-S ' + sourceLanguage + ' -F ' + config.SubtitleGenerationFormat + ' ' + loc;
            var response = generateVtt(loc, cmd);
            console.log('generated subtitle for video having ID: ' + record.ID, new Date())
            console.log('uploading subtitle for video ' + record.ID + ' to blob...', new Date())
            var location = __dirname + '/contents/';
            var subtitleFileNameInLocal = afterRenaming
                .toLowerCase().replace('mp4', config.SubtitleGenerationFormat);
            var subtitleFileNameForContainer = record.ID.concat('.' + config.SubtitleGenerationFormat);

            uploadVTTToBlob(location, subtitleFileNameInLocal, subtitleFileNameForContainer, afterRenaming);
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

function uploadVTTToBlob(loc, vttFileName, name, localAfterRenaming) {
    var containerCreationResponse = blobSvc.createContainerIfNotExists(config.vttContainerName, function (error, result, response) {
        if (!error) {
            console.log('container created successfully or exists already having name : ' + config.vttContainerName, new Date())
            console.log('uploading subtitle file to blob having location ' + loc)
            SYNC(function () {
                blobSvc.createBlockBlobFromLocalFile(config.vttContainerName, name, loc + vttFileName, function (error, result, response) {
                    if (!error) {
                        console.log('uploaded subtitle file to container having local location as ' + loc, new Date())
                        console.log('deleting video file from local', new Date())
                        deleteFile(__dirname + '/contents/' + localAfterRenaming)
                        console.log('deleting subtitle file from local', new Date())
                        deleteFile(__dirname + '/contents/' + vttFileName)
                    } else {
                        console.log('some error occured in uploading file having location: ' + loc, new Date())
                    }
                    toBeProcessed -= 1;
                });
            })
        }
    });
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

function processForNaming(previousName) {
    var previousLoc = __dirname + '/contents/' + previousName;
    var newName = getNameWithoutSpecialChars(previousName);
    var newLoc = __dirname + '/contents/' + newName;
    fs.renameSync(previousLoc, newLoc);
    return newName;
}

function getNameWithoutSpecialChars(previousName) {
    var res = previousName.replace(/[^0-9a-zA-Z\.]/g, '').toLowerCase();
    return res;
}
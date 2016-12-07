#!/usr/bin/env node
'use strict'
const child_process = require('child_process');
const spawn = child_process.spawn
const exec = child_process.exec
const fs = require('fs');
const Queue = require('bull');
const cluster = require('cluster');
var Promise = require("bluebird");

const rec = require('request-promise');
const co = require('co');

var config = require('./config.js')
if (fs.existsSync(config.local)) {
  config = Object.assign(config, require(config.local))
}

const redis = require('then-redis').createClient(config.redis)
var global_db = require("./db").db;
var connection = null;
var onErr = function(err, callback) {  
  global_db.close();
  callback(err);
};



process.on('uncaughtException', function(err) {
  console.log(err);
})


var numWorkers = 4;
// Bull queue, which handles mutexes
var processingQueue = Queue('Model processing', 6379, '0.0.0.0');


/* CODE STARTS HERE */

// Returns 0, if file have been processed, json with file id otherwise
var get_file_id_from_experiment = function (experiment, resolve) {
    let latest_file = experiment._files[experiment._files.length-1];
    redis.get(latest_file._id).then(function (value) {
      if (value == 1) {
        resolve(0);
      }
      else {
        resolve(latest_file);
      }
    });
}


var get_file_from_grid_fs = co.wrap(function* (file_obj) {
  var globalDb = global_db;
  let objectId = globalDb.toObjectID(file_obj._id);

  var gfs = globalDb.gridStore(objectId, 'r');

  let file = yield gfs.readAsync();
  fs.writeFileSync(file_obj.filename, file);

  return file_obj.filename;
});



// Gets finished experiments from Mongo DB with "success" status
var getExperiments = function(resolve, reject) {
  var db = connection;
  db.collection('experiments', function (err, collection) {
    if (!err) {
      collection.find({
        _status: "success"
      }).toArray(function (err, docs) {
        if (!err) {
          console.log('Opened connection');
          // Download  all new models
          var mkdir = 'mkdir -p ' + config.models_dir;
          var child = exec(mkdir, function (err, stdout, stderr) {
            if (err) throw err;
            else {
                // Exit from function
                resolve(docs);
            }
          });
        } else {
          console.log(err);
          onErr(err, reject);
        }
      }); //end collection.find
    } else {
      console.log(err);
      onErr(err, reject);
    }
  }); //end db.collection
};



// Handles launch of bash scripts, which launches processing with model
function process_examples (card_number, model, file_id, resolve, reject) {

  model = model.slice(0,-3);
  var hash = file_id;

  var env = Object.create(process.env);
  var process_process = spawn('bash', ['process_results.bash', card_number, model, hash], {env: env})

  process_process.stdout.on('data', (data) => {
    console.log(`[${card_number}]: ${data}`);
  });

  process_process.stderr.on('data', (data) => {
    console.log(`[${card_number}] ERR: ${data}`);
  });

  process_process.stderr.on('close', (code) => {
    console.log(`[${card_number}] CLOSED WITH CODE: ${code}`);
  });

  process_process.on('exit', (code) => {
      console.log(`[${card_number}] process exited with code ${code}`);
      if (code != 1) {
        reject(code);
        return 1;
      }
      redis.set(file_id,1);
      resolve(model+' '+file_id);
      return 0;
    });

  console.log(`[${card_number}]: STARTED`);
}

// Main thread
if(cluster.isMaster){
    // Executes only once, for master cluster
    global_db.openAsync().then((con)=>{
        connection = con;
        var promise = new Promise((resolve, reject) => {
            getExperiments(resolve, reject);
        });
        promise
            .then(
                result => {
                  var experiments = result;
                  // Lets create a few jobs for the queue workers
                  for(var i=0; i < experiments.length-1; i++){
                    processingQueue.add({experiment: experiments[i]});
                  };

                  for (var i = 0; i < numWorkers; i++) {
                    cluster.fork();
                  }

                  cluster.on('exit', function(worker, code, signal) {
                    console.log('worker ' + worker.process.pid + ' died');
                  });
                },
                error => {
                    console.log("Didn't get experiments", error, error.stack);
                }
            );
    });
}else{
  processingQueue.process(function(job, jobDone){
    console.log("Job started by worker", cluster.worker.id, job.jobId, job.data.experiment._id);

    var processing_promise = new Promise(( resolve, reject) => {
        // processing starts
        var file_promise = new Promise(( file_resolve, file_reject) => {
            get_file_id_from_experiment(job.data.experiment, file_resolve);
        });
        file_promise.then ((file) => {
            if (file != 0) {
                // If we get file, we can proccess it
                get_file_from_grid_fs(file).then( (result) => {
                    process_examples(cluster.worker.id-1, file.filename, file._id, resolve, reject);
                })
                .catch( (err) => {
                    console.log('Error getting file: ', err);
                });
            }
            else {
                // already processed
                resolve('Model '+job.data.experiment._id+' have been already processed');
            }
        });
    });
    processing_promise
      .then(
        result => {
          // Rendering completed
          console.log("Rendering completed: ", result);
          jobDone();
        },
        error => {
          // Rendering failed
          console.log("Rendering faild, error:",error);
          jobDone();
        }
      );

  });
  processingQueue.on('completed', function(job, result){
        // Job completed with output result!
        console.log("Job completed", job.jobId);
  })
}





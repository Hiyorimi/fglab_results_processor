var Promise = require("bluebird");
var mongoskin = require("mongoskin");
var db;
var config = require('./config.js')
  // Connect to MongoDB
  db = mongoskin.db('mongodb://'+config.mongodb.host+':'+config.mongodb.port+'/'+config.mongodb.db, {native_parser: true});

// Bind collections
db.bind("projects");
db.bind("machines");
db.bind("experiments");

// Promisify all methods
Object.keys(mongoskin).forEach((key) => {
  var value = mongoskin[key];
  if (typeof value === "function") {
    Promise.promisifyAll(value);
    Promise.promisifyAll(value.prototype);
  }
});
Promise.promisifyAll(mongoskin);

// Add helper methods to db
db.ObjectID = mongoskin.ObjectID;
db.toObjectID = mongoskin.helper.toObjectID;
db.GridStore = mongoskin.GridStore;

// TODO Make helper functions to contain GridFS in this file

module.exports.db = db;

//thanks to Bala Clark https://coderwall.com/p/ohjerg?&p=8&q=
//and to: this great library http://danieltao.com/lazy.js/
require('date-utils');
console.log(Date.today());
var fs = require('fs');
var AdmZip = require('adm-zip');
var path = require('path');
var readline = require('readline');
var stream = require('stream');
var Lazy = require('lazy.js');
var jf = require('jsonfile'); //https://github.com/jprichardson/node-jsonfile
var util = require('util');
var asciiJSON = require('ascii-json');
var merge = require('merge');
//var time = require('date-utils'); //
var jsesc = require('jsesc');
var spawnCommand = require('spawn-command');
var co = require('co'); //https://www.npmjs.org/package/co

var couchbase = require('couchbase');

//var cluster = new couchbase.Cluster();
//var db = cluster.openBucket('leis');


var db = new couchbase.Connection({
        connectionTimeout: 10000,  //default is apparently 5000
        operationTimeout: 5000,  //default is 2500
        host:'localhost:8091', 
        bucket:'leis'
       });


//var execSync = require('exec-sync'); //use for directory delete, couchbase tool, and zip
var S = require('string'); //see http://stringjs.com useful string conversions
//var theCsv = path.join(__dirname, 'CSVsIN', 'pleiFull_20140722.csv');
var src = 'pleiFull_20140722';
var theCsv = path.join(__dirname, 'CSVsIN', src +'.csv');
var theTempJsonDir = path.join(__dirname, 'docsTemp');
var dest = path.join(__dirname, 'docs',src);

var zip = new AdmZip();

var theJsonFile = path.join(__dirname, 'docsTemp', '123_temp.json');
var theJsonDir = path.join(__dirname, 'docsTemp');


function doMe (theCmd){
   child = spawnCommand(theCmd);
   child.stdout.on('data', function (data) {
       console.log('data', data);
     });
   child.on('exit', function (exitCode) {
      console.log('exit', exitCode);
     });
};



var ctr = 0;


var instream = fs.createReadStream(theCsv);
var outstream = new stream;
var rl = readline.createInterface(instream, outstream);

var isHeader = true;
var writeToFile = false; //just in case we want to use cbdocloader
var justAtest = false;   //'false' allows writing to database 

var asciiCheck=0;
var detCheck=0;
var rowAsArray = [];
var headerAsArray = [];

//non-null Key/Values to add to each document on load ... any set is ok
var dataToAppend = {
       identtype : "lei",
       identuser : "test-user",
       identtimestamp : Date.today().toString()
     };

//helper tor trim ... tidy data ...  remove quotes etc here 
function trimThem(x) { 
    // if (!asciiJSON.isAscii(x)) {
    //      x = jsesc(x); //escapes language special/accent characters
    //   };
        x = x.replace(/(\r\n|\n|\r)/gm,""); //removes 3 types of line break
        x= S(x).trim().s; 
      return x; 
};


rl.on('line', function(line) {

  // process line here
  ctr= ctr+1;
  if (isHeader) {
      headerAsArray = line.split(",");
      Lazy(headerAsArray).each(trimThem) // trim each header item
      isHeader = false;
    }else{
      rowAsArray = line.split(",");
      Lazy(rowAsArray).each(trimThem) // trim each row item
      
      var resArray = Lazy(headerAsArray).zip(rowAsArray); //uses the header as keys for row
     
      var resObj = Lazy(resArray).toObject(); 
      merge(resObj,dataToAppend);

      var theKey = rowAsArray[0]; //assumes unique key in 1st column
      var theFile = path.join(__dirname, 'docsTemp', theKey +'.json');
    
if (!justAtest) db.set(theKey,resObj, function (err, result) {
      if (err) throw err;
      console.log('that was ... ', theKey, " ", ctr);
     }); 

if (justAtest) console.log('nothing going to doc store just testing ...', theKey, " ", ctr);

/*
db.upsert(theKey,resObj, function (err, result) {
      if (err) throw err;
      console.log('that was ... ', theKey, " ", ctr);
     });          
 */         
           
    if(writeToFile) jf.writeFile(theFile, resObj, function(err) {
                 if (err) throw err;
                 console.log ("writing json file: ", theKey, " ",ctr);
              });

    }; //end else for each row

});



rl.on('close', function() {

  // do something on finish here
   console.log ("all done!! processed ...", ctr);

   console.log ("tidying up ... go for a coffee this will take some time!! ", ctr);
  
  var bunchaCmds = [
      "mv " + theCsv + " " +  theCsv + ".done",
      "rm -r " + theTempJsonDir,
      "mkdir " + theTempJsonDir
    ];
 
      doMe(bunchaCmds.join(" && "));  //use later after figure out load

    

});

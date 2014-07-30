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
var utf8 = require('utf8');
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


//need to do thie parameters for this:
//sh /opt/couchbase/bin/tools/cbdocloader -u Administrator -p canada -n 127.0.0.1:8091 -b leis docsTemp

var ctr = 0;


var instream = fs.createReadStream(theCsv);

var outstream = new stream;
var rl = readline.createInterface(instream, outstream);

var isHeader = true;
var writeToFile = false;
var zipWhileWrite = false;   //if this true it will be very slow 

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

//helper tor trim
function trimThem(x) { 
        if (!asciiJSON.isAscii(x)) {
             x = jsesc(x); //escapes various language special/accent characters
          };
          x = x.replace(/(\r\n|\n|\r)/gm,""); //removes all 3 types of line break
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

//console.log (resObj, "that object");
         //add some info to object here if needed
           /*   resObj.identtype = '"lei"';
              resObj.identuser = '"test-user"';
              var now = new time.Date();
              resObj.identtimestamp = '"'+now.toString()+'"';
*/
      var theKey = rowAsArray[0]; //assumes unique key in 1st column
      var theFile = path.join(__dirname, 'docsTemp', theKey +'.json');
    //do 2nd check ... non-ascii will cause loader to choke
      var resStr = JSON.stringify(resObj);
      if (!asciiJSON.isAscii(resStr)) {
            //try escape again 
             resObj = jsesc(resObj, {'json': true});
             //and check again
               var resStr = JSON.stringify(resObj);
                  if (!asciiJSON.isAscii(resStr)) {
               console.log ("better check: ", ctr , " ", theKey);
              var theFile = path.join(__dirname, 'docsTempREJECTS', theKey +'.json');
                 };
       };
  //    var resStr = JSON.stringify(resObj);
 
//if (!asciiJSON.isAscii(resStr)) resObj = jsesc(resObj, {'json': true}); THIS WORKS alternative place to escape no ascii output!!

db.set("utf8_object",theKey,resObj, function (err, result) {
      if (err) throw err;
      console.log('that was ... ', theKey, " ", ctr);
     }); 
/*
db.upsert(theKey,resObj, function (err, result) {
      if (err) throw err;
      console.log('that was ... ', theKey, " ", ctr);
     });          
 */         
           
          // jf.writeFileSync(theFile, resObj);
  if(writeToFile) jf.writeFile(theFile, resObj, function(err) {
                 if (err) throw err;
                 if(zipWhileWrite){
                 console.log ("zipping json file: ", theKey, " ",ctr);
                 zip.addLocalFile(theFile); //do after write callback
                 zip.writeZip(/*target file name*/"docsTemp/JSONfiles.zip");
                 };
              });
          // console.log(ctr);
//try to zip as we go
           
         //  doMe("zip -rj " + dest + "JSON " + theTempJsonDir + "/"+ theKey + ".json");

    }; //end else for each row



});



rl.on('close', function() {

  // do something on finish here
   console.log ("all done!! processed ...", ctr);

   console.log ("tidying up ... go for a coffee this will take some time!! ", ctr);
  var bunchaCmds = [
      "mv " + theCsv + " " +  theCsv + ".done",
      "zip -rj " + dest + "JSON " + theTempJsonDir + " -q",
      "rm -r " + theTempJsonDir,
      "mkdir " + theTempJsonDir
    ];
 
    // doMe(bunchaCmds.join(" && "));  //use later after figure out load


});

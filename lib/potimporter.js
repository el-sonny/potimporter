'use strict';

var async = require('async'),
	AdmZip = require('adm-zip'),
	fs = require('fs'),
	util = require('util'),
	sys = require('sys'),
	exec = require('child_process').exec

var pot = {};


/*
Descargar contrato.zip desde
http://portaltransparencia.gob.mx/pot/fichaOpenData.do?method=fichaOpenData&fraccion=contrato
TIP si no funciona con algo sensillo como wget intenta usar nightmare
*/
pot.downloadMainFile = function(cb){

}

//One main zip file contains individual zip files with each spending category
pot.extractMainFile = function(){
	console.log('extracting main zip file');
	var zip = new AdmZip('../tmp/contrato.zip');	
	zip.extractAllTo("../tmp/extract-zips",true);
}

//Each spending category zip contains one or more csvs that we extract here
pot.extractZips = function(){
	fs.readdir('../tmp/extract-zips',function(err,files){
		if(err) throw err;
		files.forEach(function(file){
			console.log('extracting zip: '+file);
		    var zip = new AdmZip('../tmp/extract-zips/'+file)
		    zip.extractAllTo('../tmp/extract-csv',true);
		});
	});	
}

// Remove the first line in all csvs is always "CONTRATACIONES" the headerline(to change the field names manually with fielfile)
pot.removeFirstLines = function(cb){
	fs.readdir('../tmp/extract-csv',function(err,files){
		if(err) throw err;
		async.mapSeries(files,function(file,cb){
			console.log('Removing First line: '+file);
			exec('tail -n +3 "../tmp/extract-csv/'+file+'" > "../tmp/fix-csv/'+file+'"',cb);
		},cb);		
	});
}

pot.convertCSV = function(cb){
	fs.readdir('../tmp/fix-csv',function(err,files){
		async.mapSeries(files,function(file,cb){
			console.log('Reformating with ssconvert: '+file);
			var newfile = file.replace(/\s+/g, '_');
			exec('ssconvert "../tmp/fix-csv/'+file+'" "../tmp/convert-csv/'+newfile+'"',cb);
		},cb);	
	});
}

// Import modified CSVs to mongo DB with mongo import
pot.importToMongo = function(cb){
	fs.readdir('../tmp/convert-csv/',function(err,files){
		if(err) throw err;
		async.mapSeries(files,function(file,cb){
			console.log('importing: '+file);
			exec('mongoimport --type csv --db sac --fieldFile fieldfile -c contratospot ../tmp/convert-csv/'+file,function(error, stdout, stderr){
				console.log('stdout: ' + stdout);
				console.log('stderr: ' + stderr);
			    if (error !== null) {
			      console.log('exec error: ' + error);
			    }
			    cb(error,stdout,stderr);
			});
		},cb);
	})
}

//pot.extractMainFile();
//pot.extractZips();
//pot.removeFirstLines();
//pot.convertCSV();
//pot.importToMongo();
async.series([
	//pot.removeFirstLines,
	//pot.convertCSV,
	pot.importToMongo
]);
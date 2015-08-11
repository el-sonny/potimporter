'use strict';

var async = require('async'),
	AdmZip = require('adm-zip'),
	fs = require('fs'),
    http = require('http'),
	util = require('util'),
	sys = require('sys'),
	exec = require('child_process').exec;

var pot = {};

/*
Descargar contrato.zip desde
http://portaltransparencia.gob.mx/pot/fichaOpenData.do?method=fichaOpenData&fraccion=contrato
TIP si no funciona con algo sensillo como wget intenta usar nightmare
*/
pot.downloadMainFile = function(cb){

    var existsDir =  fs.existsSync('tmp');
    if (!existsDir){
        fs.mkdirSync('tmp');
    }
    var existsFile =  fs.existsSync('tmp/contrato.zip');
    if (!existsFile) {
        console.log('downloading main zip file');

        var file = fs.createWriteStream('tmp/contrato.zip');
        var request = http.get("http://portaltransparencia.gob.mx/pot/repoServlet?archivo=contrato.zip", function(response) {
            response.pipe(file);
            response.on('end',function(){
                console.log('donwloaded');
                cb();
            });

        });
    } else {
        console.log('file already exists');
        cb();
    }


};

//One main zip file contains individual zip files with each spending category
pot.extractMainFile = function(cb){
	console.log('extracting main zip file');
	var zip = new AdmZip('tmp/contrato.zip');
	zip.extractAllTo("tmp/extract-zips",true);
    cb();
};

//Each spending category zip contains one or more csvs that we extract here
pot.extractZips = function(cb){
	fs.readdir('tmp/extract-zips',function(err,files){
		if(err) throw err;
        async.each(files,function(file,callback){
			console.log('extracting zip: '+file);
		    var zip = new AdmZip('tmp/extract-zips/'+file);
		    zip.extractAllTo('tmp/extract-csv',true);
            callback();
		},cb);
	});	
};

pot.renameFiles = function(cb) {
    fs.readdir('tmp/extract-csv',function(err,files){
        if(err) throw err;
        async.each(files,function(file,callback){
            console.log('renaiming file : '+ file);
            var newfile = file;
            newfile = newfile.replace(/\s+/g, '_');
            console.log('to ' + newfile);
            exec('mv "tmp/extract-csv/'+file+'" "tmp/extract-csv/'+newfile+'"',function(err,out,code){
                if (err)
                    console.log('error ' + err);
                callback();
            });
        },cb);

    });
};

// Remove the first line in all csvs is always "CONTRATACIONES" the headerline(to change the field names manually with fielfile)
pot.removeFirstLines = function(cb){
	fs.readdir('tmp/extract-csv',function(err,files){
		if(err) throw err;
        var existsDir =  fs.existsSync('tmp/fix-csv');
        if (!existsDir){
            fs.mkdirSync('tmp/fix-csv');
        }
        console.log(files);
		async.each(files,function(file,callback){
			console.log('Removing First line: '+file);
			exec('tail -n +3 "tmp/extract-csv/'+file+'" > "tmp/fix-csv/'+file+'"',function(err,out,code){
                if (err)
                    console.log('error ' + err);
                callback();
            });
		},cb);		
	});
};

pot.convertCSV = function(cb){
	fs.readdir('tmp/fix-csv',function(err,files){
        if(err) throw err;
        var existsDir =  fs.existsSync('tmp/convert-csv');
        if (!existsDir){
            fs.mkdirSync('tmp/convert-csv');
        }
		async.mapSeries(files,function(file,callback){
			console.log('Reformating csv with ssconvert: '+file);
			exec('ssconvert "tmp/fix-csv/'+file+'" "tmp/convert-csv/'+file+'"',function(error,stdout,stderr){
                if (error)
                    console.log('error : ' + error);
                callback();
            });
		},cb);	
	});
};

// Import modified CSVs to mongo DB with mongo import
pot.importToMongo = function(cb){
	fs.readdir('tmp/convert-csv/',function(err,files){
		if(err) throw err;
		async.mapSeries(files,function(file,cb){
			console.log('importing: '+file);
			exec('mongoimport --type csv --db sac --fieldFile fieldfile -c contratospot tmp/convert-csv/'+file,function(error, stdout, stderr){
			    if (error !== null) {
			      console.log('exec error: ' + error);
			    }
			    cb(error,stdout,stderr);
			});
		},cb);
	})
};

pot.formatNames = function(cb) {
    var MongoClient = require('mongodb').MongoClient;
    MongoClient.connect('mongodb://127.0.0.1:27017/sac', function(err, db) {
        if(err)
            throw err;
        console.log("connected to the mongoDB !");
        var counter = 1,c = 1;
        var bulk = db.collection('contratospot').initializeOrderedBulkOp();
        console.log('bulk contratos initialized');
        db.collection('contratospot').find({ dependencia : /^\s+|\s+$/ }).forEach(function(doc) {
            console.log(doc._id);

            bulk.find({ _id: doc._id }).update({
                "$set": {
                    dependencia: doc.dependencia.trim(),
                    codigo_contrato: doc.codigo_contrato.trim(),
                    tipo_procedimiento: doc.tipo_procedimiento.trim(),
                    proveedor_contratista: doc.proveedor_contratista.trim(),
                    fecha_celebracion: doc.fecha_celebracion.trim(),
                    importe_contrato: doc.importe_contrato.trim(),
                    unidad_administrativa: doc.unidad_administrativa.trim(),
                    titulo_contrato: doc.titulo_contrato.trim(),
                    fecha_inicio: doc.fecha_inicio.trim(),
                    fecha_fin: doc.fecha_fin.trim(),
                    anuncio: doc.anuncio.trim(),
                    convenio_modificatorio: doc.convenio_modificatorio.trim()
                }
            });

            if ( counter % 100 == 0 ) {
                console.log('ciclo ' + c++);
                bulk.execute({ w: 0});
                counter = 1;
            }
            counter++;
        });
        if (counter > 1)
            bulk.execute({ w: 0});

        console.log('fin');
        cb();
    });
}


//pot.downloadMainFile();
//pot.extractMainFile();
//pot.extractZips();
//pot.removeFirstLines();
//pot.convertCSV();
//pot.importToMongo();
async.series([
    pot.downloadMainFile,
    pot.extractMainFile,
    pot.extractZips,
    pot.renameFiles,
	pot.removeFirstLines,
	pot.convertCSV,
	pot.importToMongo,
    pot.formatNames
]);
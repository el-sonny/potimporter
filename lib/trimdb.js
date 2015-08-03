var counter = 1;
var bulk = db.collection.initializeOrderedBulkOp();
db.collection.find({ "category": /^\s+|\s+$/ },{ "category": 1}).forEach(
    function(doc) {
        bulk.find({ "_id": doc._id }).update({
            "$set": { "category": doc.category.trim() }
        });

        if ( counter % 1000 == 0 ) {
            bulk.execute();
            counter = 1;
        }
        counter++;
    }
);

if ( counter > 1 )
    bulk.execute();
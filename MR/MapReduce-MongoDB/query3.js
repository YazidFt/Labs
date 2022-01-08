

use DBLP;

var mapFunction = function () {
 	  if(this.publisher == "Springer"){
               emit(this.year, 1)
             }
       };

var reduceFunction = function (key, values) {
	    return  Array.sum(values)
 };



db.publis.mapReduce(mapFunction,
   reduceFunction, {out: "results_set"});

db.results_set.find({value: {$gt: 2}})



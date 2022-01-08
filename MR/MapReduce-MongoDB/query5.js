// Number of publications for ("Taru Ishida", year)

use DBLP;

var mapFunction = function () {
 	  if(Array.contains(this.authors, "Toru Ishida")){
               emit(this.year, 1)
             }
       };

var reduceFunction = function (key, values) {
	    return Array.sum(values)
 };


db.publis.mapReduce(mapFunction,
   reduceFunction, {out: {"inline": 1} });



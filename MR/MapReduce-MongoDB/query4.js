// Number of publications for (publisher, year)

use DBLP;

var mapFunction = function () {
 	  if(this.publisher){
               emit({publisher: this.publisher, year: this.year}, 1)
             }
       };

var reduceFunction = function (key, values) {
	    return Array.sum(values)
 };


db.publis.mapReduce(mapFunction,
   reduceFunction, {out: {"inline": 1} });



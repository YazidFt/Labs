// Average Number of pages for "Taru Ishida" Articles

use DBLP;

var mapFunction = function () {
 	  if(this.type == "Article" && Array.contains(this.authors, "Toru Ishida")){
               emit(null, this.pages.end - this.pages.start)
             }
       };

var reduceFunction = function (key, values) {
	    return Array.avg(values)
 };


db.publis.mapReduce(mapFunction,
   reduceFunction, {out: {"inline": 1} });



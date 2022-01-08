//Publication with more than 3 authors

use DBLP;

var mapFunction = function () {
     if(this.authors.length > 3){
               emit(null, 1);
        }

 }

var reduceFunction = function (key, values) {
      return Array.sum(values)
  };


db.publis.mapReduce(mapFunction,
   reduceFunction, {out: {"inline": 1} });


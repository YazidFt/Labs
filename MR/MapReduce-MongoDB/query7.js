// Title of publications for every author

use DBLP;

var mapFunction = function () {
 for(var i=0;i<this.authors.length;i++)
   emit(this.authors[i], this.title);};

var reduceFunction = function (key, values) {
  return {titles : values};
};


db.publis.mapReduce(mapFunction,
   reduceFunction, {out: {"inline": 1} });



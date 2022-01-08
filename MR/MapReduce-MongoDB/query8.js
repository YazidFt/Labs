use DBLP;

var mapFunction = function () {
 for(var i=0;i<this.authors.length;i++){
       emit({author: this.authors[i], year: this.year}, 1);};
     }
 }

var reduceFunction = function (key, values) {
       return Array.sum(values);
  };


db.publis.mapReduce(mapFunction,
   reduceFunction, {out: {"inline": 1} });



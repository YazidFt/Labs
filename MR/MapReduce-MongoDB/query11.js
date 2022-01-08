// Max year, min year and total number of publications for every author

use DBLP;

var mapFunction = function () {
        for(let i = 0; i < this.authors.length; i++){
   		emit(this.authors[i], {maxYear: this.year, minYear: this.year, nb: 1})             
          }
    }

var reduceFunction = function (key, values) {
       var tot = 0
       var mi = 10000
       var mx = 0

       for(let i = 0; i < values.length; i++){
             mi = min(mi, values[i].minYear)
             mx = max(mx ,values[i].maxYear)
             nb = nb + values[i].nb
          }
      
      return {maxv: mx, minv: mi, total: nb}
  };


db.publis.mapReduce(mapFunction,
   reduceFunction, {out: {"inline": 1} });


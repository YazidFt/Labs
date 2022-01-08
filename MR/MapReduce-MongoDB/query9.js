//Number of authors per year for publisher "Springer"

use DBLP;

var mapFunction = function () {
     if(this.publisher == "Springer"){
         for(let i = 0; i < this.authors.length ; i++){
               emit(this.year, this.authors[i]);
            }
        }

 }

var reduceFunction = function (key, values) {
       //count distinct values of authors for every year
       var distinct = 0
       var authors_distinct = new Array()
       var len = 0
       
       for(let i = 0; i < values.length; i++){
              if(!Array.contains(authors_distinct, values[i])){
                     distinct++
		     authors_distinct[len] = values[i]
	             len++;
                  }
           }
      return distinct
  };


db.publis.mapReduce(mapFunction,
   reduceFunction, {out: {"inline": 1} });


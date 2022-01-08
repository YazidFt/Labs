//Number of authors of a "book"

use DBLP;

var mapFunction = function() {
                     if(this.type == "Book"){
               		   emit(this.title, this.authors.length)      
  			}
                    } 

var reduceFunction = function(title, values){
		     return {nb_authors: values}
                 }

db.publis.mapReduce(mapFunction, reduceFunction, {out: {"inline": 1}});




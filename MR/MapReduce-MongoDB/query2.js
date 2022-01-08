//Number of chapters with 'booktitle' published by Springer

use DBLP;

var mapFunction = function () {
   if(this.publisher=="Springer" && this.booktitle)
         emit(this.booktitle, 1);
       };

var reduceFunction = function (key, values) {
   return Array.sum(values);
 };



db.publis.mapReduce(mapFunction,
   reduceFunction, {out: "results_set"});

db.results_set.find({value: {$gt: 2}})



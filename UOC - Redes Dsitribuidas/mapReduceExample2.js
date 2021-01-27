/********************************************************
 * Example 2: word frequency                            *
 ********************************************************/

// We take into account words longer than 3 caracters, 
// containing at least one letter and not beginning with "http"

var m = function() { 
  this.text.match(/\S+/g).forEach(function(word) {
    if ( word.length > 3 && ! word.match(/^http/) && word.match(/^[a-zA-Z]/) ) {
      emit(word.toLowerCase(),1)
    }
  })
};

var r = function(key, values) { 
  return Array.sum(values); 
};

db.runCommand( {
                 mapReduce: "tweets",
                 map: m,
                 reduce: r,
                 out: {replace : "result_words"}
               } );


// To show results: db.result_words.find().sort({value:-1})

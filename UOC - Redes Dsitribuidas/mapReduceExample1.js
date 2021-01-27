/********************************************************
 * Example 1: count tweets per country                  *
 ********************************************************/

var m = function() { 
  if (this.place != null) {
    emit (this.place.country_code, 1);
    // print("map country=" + this.place.country_code);
  } else {
    emit ("?", 1);
    // print("map country=?");
  }
};

var r = function(key, values) { 
  // print("reduce key=" + key + " values=" + values);
  return Array.sum(values); 
};

// To debug uncomment prints and check their output in /var/log/mongodb/mongodb.log


db.runCommand( {
                 mapReduce: "tweets",
                 map: m,
                 reduce: r,
                 out: {replace : "result_country"}
               } );


// To show results: db.result_country.find().sort({value:-1})

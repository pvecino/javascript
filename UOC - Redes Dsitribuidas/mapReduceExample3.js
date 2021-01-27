/********************************************************
 * Example 3: The 10 most used hashtags per country     *
 ********************************************************/

// Map function
// - For each Tweet emits a pair:
//   { country_code, { hashtag1: 1, hashtag2: 1, ...} }

var m = function() { 
  hashtags = {}
  if (this.place != null) {
    country = this.place.country_code
    // print("map country=" + this.place.country_code);
  } else {
    country = "?"
    // print("map country=?");
  }
  for(var i in this.entities.hashtags) {
    hashtags[this.entities.hashtags[i].text] = 1
  }
  emit(country, hashtags)
};

// Reduce function
// - Receives a list of dictionaries of hashtags and composes
//   them returning a single dictionary
// - Each entry will show the number of ocurrences of a given
//   hashtag

var r = function(key, values) { 
  result = {}
  values.forEach(function(val) {
    for (hashtag in val) {
      if (hashtag in result) {
        result[hashtag] = result[hashtag] + val[hashtag] 
      } else {
        result[hashtag] = val[hashtag] 
      }
    } 
  });
  return(result)
};

// Finalize function:
// - sorts hashtags
// - converts dictionary to array
// - returns only first 10 hashtags

var f = function(key, val) { 
  var hashtags = []; 
  for(var hashtag in val) hashtags.push(hashtag);
  return hashtags.sort(function(a,b) {return val[b]-val[a]}).slice(0,10);
};

db.runCommand( {
                 mapReduce: "tweets",
                 map: m,
                 reduce: r,
                 finalize: f,
                 out: {replace : "result_hashtags"}
               } );


// To show results: db.result_hashtags.find().pretty() 

/********************************************************
 * Return for each country:                             *
 *  1) Total number of tweets in the database           *
 *  2) The 5 users with most tweets                     *
 *  3) The 10 most used words (words > than 3 letters)  *
 ********************************************************/
// Función MAP
// - Por cada tweet se "emite":
//   { country_code, { count: 1, user:{userN: 1}, words: {palabra1: 1, palabra2: 1, ...}} }
var m = function() {

	content = { count: 0, words: {}, users: {} };
	// COUNTRY
	if (this.place != null && this.place != "") {
		country = this.place.country_code
	} else {
		country = "?"
	}
	// COUNT
	content.count = 1
	// USERS
	content.users[this.user.screen_name] = 1
	// print(this.user.screen_name)
	// WORDS
	sentence = this.text.split(' ')
	for(var i in sentence) {
		if ( sentence[i].length > 3 && ! sentence[i].match(/^http/) && sentence[i].match(/^[a-zA-Z]/) ) {
			word = sentence[i].toLowerCase()
			content.words[word] = 1
		}
	}
	emit(country, content);
};

// Función REDUCE
// - Por cada "emit" recibe un diccionario donde el primer valor es la 
// cuenta, el segundo el usuario y el resto las palabras.
// - Por tanto, agrupamos los diccionarios en una variable record y vamos 
// sumando las ocurrencias de las palabras y los usuarios.

var r = function(key, values) { 
  reducedVal = { count: 0, words: {}, users: {} };
//por cada emit llega  { count: 1, user:{userN: 1}, words: {palabra1: 1, palabra2: 1, ...}} }
  values.forEach(function(content) { 
  	// COUNT
    reducedVal.count+=content.count;
    // print(key + "cuenta" + content.count['count'])

    // USERS
    for (user in content.users) {
		// print(key + " usuario " + user + content.users[user])
		if (user in reducedVal.users) {
			reducedVal.users[user] = reducedVal.users[user] + content.users[user] 
	  	} else {
	    	reducedVal.users[user] = content.users[user] 
	  	}
  	}

    // WORDS
    for (word in content.words) {
		// print(key + " palabra " + word + content.words[word])
		if (word in reducedVal.words) {
		  reducedVal.words[word] = reducedVal.words[word] + content.words[word] 
		} else {
		  reducedVal.words[word] = content.words[word] 
		}
	}
  });
  return reducedVal;
};

// Función FINALIZE:
// - Convertimos los diccionarios en arrays, los ordenamos y cogemos
// los 5 primeros usuarios y las 10 primeras palabras.

var f = function(key, reducedVal) {
  finalicedVal= { count: reducedVal.count, words: [], users: [] };
  //USERS:
  for(var usr in reducedVal.users) finalicedVal.users.push(usr);
  finalicedVal.users = finalicedVal.users.sort(function(a,b) {return reducedVal.users[b]-reducedVal.users[a]}).slice(0,5);
  // WORDS  
  for(var wrd in reducedVal.words) finalicedVal.words.push(wrd);
  finalicedVal.words = finalicedVal.words.sort(function(a,b) {return reducedVal.words[b]-reducedVal.words[a]}).slice(0,10);
   
  return finalicedVal
};

db.runCommand( {
                 mapReduce: "tweets",
                 map: m,
                 reduce: r,
                 finalize: f,
                 out: {replace : "result"}
               } );


// To show results: db.result.find().pretty() 
//
// While testing, to reduce the execution time you can filter tweets for country.
// For example you could just calculate the results for Andorra:
//
// 1) Create an Index on country_code:
//
//    db.tweets.createIndex({"place.country_code": 1})
//
// 2) Add the filter to the map reduce:
//
//    db.runCommand( {
//                     mapReduce: "tweets",
//                     map: m,
//                     reduce: r,
//                     finalize: f,
//                     query: {"place.country_code":"AD"},
//                     out: {replace : "result"}
//                   } );

   
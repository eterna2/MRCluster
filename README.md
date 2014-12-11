MapReduce Cluster (MRCluster)
==============

A single node multi-core pseudo-MapReduce implementation on NodeJS. Input files are automatically broken into blocks and distributed to the Mappers and Reducers. 

Examples of implementations can be found in the README.

### Installation
```
npm install mrcluster
```

### Features List
---
* `.file`: input file or arrays of files for the MR task.

* `.lineDelimiter`: delimiter for linebreaks in the data.

* `.blockSize`: size in Mb for each block.

* `.sample`: number of sample chunks to run (to test ur codes).

* `.cache`: pre-load and cache a javascript `Object` into the `Mapper` and `Reducer`.

* `.mapOnly`: Perform mapping only.

* `.numMappers`: number of Mappers to use.

* `.numReducers`: number of Reducers to use.

* `.partition`: Custom function to control how the Mapper outputs are distributed to the Reducers. The function takes in a key, and returns a integer corresponding to the respective Reducer. 

* `.map`: Map function. Takes in a line and return a key-value pair array (1-1 mapping) or a hashtable of key-value pairs (1-n mapping).

* `.mapCSV`: Alternative Map function. Takes in an array and return a key-value pair array (1-1 mapping) or a hashtable of key-value pairs (1-n mapping).

* `.combine`: Combine function applied after the Map task in the Mapper. Takes in 2 values with same key, and return a value for the key.

* `.reduce`: Reduce function. Takes in 2 values with same key, and return a value for the key.

* `.drain`: Drain function. Takes in a hashtable of keys and values. Return a new hashtable of keys and values. Used to free up memory in the Reducer. 

* `.post_reduce`: Aggregate function after all the Reduce tasks are completed for each Reducer. Takes in an hashtable of key and values. Return a value to the master node.

* `.aggregate`: Aggregate function at the master node. Aggregates all the values returned by all the Reducers. Takes in an array of values (same as number of Reducers).


*v0.0.21*
* Fixed bug when `.combine` is not defined.  
* Added `.mapCSV` function. `.mapCSV` is functionally equivalent to `.map` except the input is an array instead of a line. This array is extracted from a single line of csv using the method described [here](http://stackoverflow.com/questions/8493195/how-can-i-parse-a-csv-string-with-javascript).
* Added `.cache` function. `.cache` function allows user to pre-load a javascript `Object` (but not `function`) into all the `Mapper` and `Reducer`.
* Added `.partition` function. Custom function to control how the Mapper outputs are distributed to the Reducers.

*v0.0.20*
* Replaced `.numBlocks` with `.blockSize` function to allow user to define the size of each block in Mb. Default is `64 Mb`.
* Enhanced `map` function to allow 1-n mapping - mapping 1 line of data into multiple key-value pairs in the form of a hashtable.

*v0.0.19*
* Added `.combine` function to allow user to define the `.reduce` function to run at the `Mapper`.
* Added `.drain` function to allow user to clear the memory of the `Reducer` after each reduce task.
* Added example on how to rehash "long" user ids into unique integers.

## Quick Start
---
#### Create a new instance
```javascript
var mrcluster = require("mrcluster").init();
```

#### Chaining
The module is written to be chainable. All settings are set via function call chains. 
```javascript
mrcluster
	.file("mockdata_from_mockaroo.csv")
	.lineDelimiter('\n')
	.numBlocks(9);
```

#### Overview
```javascript
var mrcluster
	.file("mockdata_from_mockaroo.csv")
	.map(function(line){ some map function returning a key-value pair })
	.reduce(function(a,b){ some reduce function returning a value })
	.post_reduce(function(hashtable){ some function to return aggregated results back to master node })
	.aggregate(function(array){ some aggregation function for all the results returned by the reducers })
	.start();
```

## Options
---
##### .file
Specify the csv file or files to read in. 
```javascript
mrcluster.file("mockdata_from_mockaroo.csv");
```

If an array of files are given, the files will be broken into their respective blocks and will pushed to the Mappers in a FIFO manner.
```javascript
mrcluster.file(["file1.csv","file2.csv","file3.csv"]);
```

##### .lineDelimiter (optional)
Specify the delimiter to indicate a new line. Default is `\n`.  
```javascript
mrcluster.lineDelimiter('\n');
```

##### .blockSize (optional)
Specify the size of each block (in Mb) to break the file into. Default is `64 Mb`.
As each NodeJs process (aka each `Mapper` / `Reducer`) is limited to ~1 Gb RAM (x64), you might want to break up the file into sufficiently small blocks. 
```javascript
mrcluster.blockSize(64);	// each block will be ~64 Mb
```

##### .sample (optional)
Specify the number of Blocks to sample. The min number of samples must be >= number of `Mappers`. Default is `-1` (Do not sample - run everything).  
This function is useful to have a quick test of your codes before actually running through the entire dataset.
```javascript
mrcluster.sample(1);
```

##### .cache (optional)
Pre-load an Object or variable to all `Mappers` and `Reducers`. E.g. An array of weights. This Object can be called in any of the callbacks (e.g. `.map`, `.reduce`) via the variable `ctx._cache`. The `ctx._cache` variable for each `Mappers` and `Reducers` is mutable and persistent. Each `ctx._cache` variable starts off identical but is independent from each other in the subsequent operations.

```javascript
mrcluster
	.cache([1,2,3])
	.drain(function(hashtable){
		for (var i in hashtable)
		{
			ctx._cache[i] += hashtable[i] * 0.01;
		}
		return {};
	})
	.post_reduce(function(hashtable){
		return ctx._cache;
	});

```

##### .mapOnly (optional)
Specify whether to run only Mappers. Default is `False`.  
Note that you still need to specify your `Reduce` function as the `Reduce` step is also performed in the `Mapper`. 
```javascript
mrcluster.mapOnly(true)
```

##### .numMappers (optional)
Specify the number of mappers to create. Default is `1`.
```javascript
mrcluster.numMappers(2);
```

##### .numReducers (optional)
Specify the number of reducers to create. Default is `3`.
The underlying codes will hash all key-values pairs produced by the mappers into the respective reducers. Hence, each chunks of key-values pairs in each reducer is independent of each other. This reduces memory usage when doing the reduce operation. 

```javascript
mrcluster.numReducers(3);
```

##### .partition (optional)
Specify a custom hash function to distribute the Mapper outputs to the respective Reducers. Takes in `numReducers` as 1st input, and custom hash function as 2nd input.
The custom hash function takes in a `key` and returns an integer (representing which Reducer to send this key pair to). Note that the number of Reducers must match the output of the hash function. Custom hash function allows you to perform some ordering functions in map-reduce, but you have to take care that the hash function is able to evenly distribute the loads among the Reducers. 

For example, if most key-value pairs have the keys starting with "A", then the hash function in the example below will allocate all the key-value pairs to the same reducer (as the hash function distribute the key-value pairs by the 1st character of the key). 

```javascript
mrcluster.partition(3,
	function(key){
		// distribute key-value pairs based on the 1st character of the key
		return key.charCodeAt(0)%3;
	}
);
```

##### .map
First input specifies the mapping function to be applied on each line of data. 
Second input (optional) is a flag to specify whether to write the content of each Mapper to disk. This is often used with the `mapOnly` options when you are only doing `Map` tasks (e.g. remapping data).
The function should take in a `String` representing a line of data, and returns an `Array[2]` representing the resultant key-value pair.
```javascript
mrcluster    
	.map(function (line) {
        return [line.split(',')[0], 1];
    },
	true)
```
Alternatively, the function can also return a `Hashtable` of key-value pairs, aka, instead of mapping a line of data into a single key-value pair, the map function can also map a line of data into multiple key-value pairs represented in a hashtable.
```javascript
mrcluster    
	.map(function (line) {
		var hashtable = {};
		hashtable[key1] = line;
		hashtable[key2] = line;
		hashtable[key3] = line;
        return hashtable;
    },
	true)
```
 
##### .mapCSV (replaces `.map`) 
This is a CSV replacement for the `.map` function, where the input variable is an array instead of a line. This array is automatically extracted from the line using the method described [here](http://stackoverflow.com/questions/8493195/how-can-i-parse-a-csv-string-with-javascript). The main advantage is that quotes and double quotes in the csv are automatically handled, and converted into an array. However, this come at the cost of extra computation time as `regex` is used to extract the values. For simple CSV, it may be a better alternative to do a simple line split in the `.map` function.
```javascript
mrcluster    
	.mapCSV(function (array) {
        return [array[0], 1];
    },
	true)
```


##### .combine (optional)
The `combine` function is essentially the `reduce` operation to perform at the mapper, as some reduce jobs can be done at the mapper instead of the reducer.
By default the `combine` function will be the same as the `reduce` function. However, you can use this function call to specify a different `.reduce` function at the mapper.
```javascript
mrcluster    
	.combine(function (a,b) {
        return a + b;
    })
```

##### .reduce
First input specifies the reduce function to be applied. The second input (optional) specifies whether to write the result of each Reduce jobs to disk. 
This function is applied once in the `Mapper` and once in the `Reducer`. It is applied at the end of the `Mapper` execution, just before returning the mapped results to the master node.  
The function should take 2 variables representing the the values for the two key-value pairs. And returns a value representing the resultant value for the two key-value pairs.
E.g. The following codes demonstrate the summing of the values for 2 key-value pairs - ['A',1] + ['A',1] = ['A',2]
```javascript
mrcluster    
	.reduce(function (a,b) {
        return a + b;
    })
```

##### .drain (optional)
In the MR task, each reducer actually hold in memory the hashtable of key-value pairs it has received so far. For some reduce tasks (e.g. concat, or append tasks), the size of the value in the key-value pair increases after each reduce task which might lead to out of memory problems. 
The `.drain` function can be used to free up memory in some of these situation. The `.drain` function takes in a `hashtable` of the current key-value pairs held in memory by the reducer, and returns the new `hashtable`.

For example, you wish to rehash long user ids into unqiue integers. You can set the key as the user id and the value as remaining data at the Mapper. Your `.combine` function can concat the data by keys. 
The memory usage for the reducers will monotonically increase with each reduce pass if you keep appending new data by keys. So you can specify a `.drain` function where you write the current data to file, and return a new hashtable with the keys but without the data. Hence, you can continue appending new data in your `.reduce` function.   
```javascript
mrcluster  
	.drain(function(list){
		var id = 0, lines = "", obj = {};
		for (var key in list)
		{
			obj[key] = [];
			list[key].forEach(function(data){
				lines += (id*7+ctx.id)+','+data+"\n";	// ensure the id generated is unique across all the 7 reducers  
			});
			id++;
		}
		fs.appendFile('res_'+ctx.id+'.csv',lines);
		return obj;
	})
```

##### .post_reduce (optional)
Specify the function to be applied at the end of each `Reducer` execution. 
The function should take in an `hashtable` holding all the key-values produced by the `Reducer`. And can return any value to the master node for further collation (e.g. sum).
```javascript
mrcluster    
    .post_reduce(function (obj) {
        var res = Object.keys(obj).map(function (key) {
            return obj[key];
        });
		console.log(obj)
        return res.reduce(function (a, b) {
            return a+b;
        });
    })
```

##### .aggregate (optional)
Specify the function to be applied at the end of all tasks. 
The function should take in an `Array` (representing the hash bins) holding all the returned Values produced by the `.post_reduce` function (e.g. You can do a summation of all the returned sums of all the `Reducers`).  
```javascript
mrcluster    
    .aggregate(function (hash_array) {
        console.log("Total: " + hash_array.reduce(function (a, b) {
            return a + b;
        }))
    })
```

## Examples
---
### Counting Unique Ids
A simple count of number of unique domains in the email list.
```javascript
var mrcluster = require("mrcluster");

mrcluster.init()
    .file("mockdata_from_mockaroo.csv")	
	// line delimiter is \n
    .lineDelimiter('\n')
	// each block is 1 Mb
	.blockSize(1)	
	// 2 mappers
	.numMappers(2)
	// 3 reducers
    .numReducers(3)	
	// function to map a line of data to a key-value pair
    .map(function (line) {
		// tokenize line
		// select 2nd col and tokenize it again
		// get the domain or return NA if null
		// return a key-value pair of format [domain,1]
        return [line.split(',')[1].split('@')[1] || 'NA', 1];
    })
	// simple reduce function which return a value of 1
    .reduce(function (a, b) {
        return 1;
    })
	// sum the values of all key-value pairs in the Reducer
    .post_reduce(function (obj) {
        var res = Object.keys(obj).map(function (key) {
            return obj[key];
        });
		console.log(obj)
        return res.reduce(function (a, b) {
            return a+b;
        });
    })
	// sum the results returned by all the Reducers
    .aggregate(function (hash_array) {
        console.log("Total: " + hash_array.reduce(function (a, b) {
            return a + b;
        }))
    })
	// start MapReduce job
    .start();
```


### Finding similar users
Finding users share same domain for their emails.
```javascript
var mrcluster = require("mrcluster");

mrcluster.init()
    .file("mockdata_from_mockaroo.csv")
    .lineDelimiter('\n')
	.blockSize(1)
	.numMappers(2)
    .numReducers(3)
    .map(function (line) {
		var a = line.split(',')[1].split('@');
        return [a[1] || 'NA', [a[0]]];
    })
    .reduce(function (a, b) {
        return a.concat(b);
    })
    .post_reduce(function (obj) {
		var lines = "";
        Object.keys(obj).forEach(function (key) {
			var tmp = {};
			obj[key].forEach(function(d){tmp[d]=true;});
            lines += key+','+Object.keys(tmp).join(',')+'\n';
        });
		fs.appendFile('results.csv',lines);	// output to file
        return 0; // trivial return
    })
    .start();
```


### Rehashing long user ids
Assuming you have very long user ids (e.g. md5 hashes), and you wish to replace these ids with unique integers. You can achieve this by setting the key to be the id, and the value to be the data for the `mapper`. Then concat the data by keys in the `combiner` and `reducer`. 

As you concat data, memory usage is monotonically increasing. So you will want to free up memory by writing out data that you have already grouped by id. And as the keys in each `reducer` is independent to other `reducers`, you can replace the key with an integer with base of the number of reducers (needs to be prime). 

E.g. Assuming 7 reducers, 
1st key in reducer 1 = 1, 
2nd key in reducer 1 = 8,
n key in reducer m = (n-1)*(number of reducers) + m

```javascript
var mrcluster = require("mrcluster");

mrcluster.init()
    .file("mockdata_from_mockaroo.csv")
    .lineDelimiter('\n')
    .blockSize(1)
    .numMappers(2)
    .numReducers(7)
    .map(function (line) {
		var d = line.split(',');
		var id = d.shift();
        return [id, [d.join(',')]];
    })
    .reduce(function (a, b) {
        return a.concat(b);
    })
	.drain(function(list){
		var id = 0, lines = "", obj = {};
		for (var key in list)
		{
			obj[key] = [];
			list[key].forEach(function(d){
				// ctx.id is the id of the reducer
				lines += (id*7+ctx.id)+','+d+"\n";		
			});
			id++;
		}
		fs.appendFile('res_'+ctx.id+'.csv',lines);
		return obj;
	})
    .start();
```
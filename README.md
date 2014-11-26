MapReduce Cluster (MRCluster)
==============

A single node multi-core pseudo-MapReduce implementation on NodeJS.

### Installation
```
npm install mrcluster
```

### Features List
---
* `file`: input file or arrays of files for the MR task.

* `lineDelimiter`: delimiter for linebreaks in the data.

* `numBlocks`: number of chunks to create for each file.

* `sample`: number of sample chunks to run (to test ur codes).

* `numMappers`: number of Mappers to use.

* `map`: Map function. Takes in a line and return a key-value pair array.

* `combine`: Combine function applied after the Map task in the Mapper. Takes in 2 values with same key, and return a value for the key.

* `mapOnly`: Perform mapping only.

* `numReducers`: number of Reducers to use.

* `reduce`: Reduce function. Takes in 2 values with same key, and return a value for the key.

* `drain`: Drain function. Takes in a hashtable of keys and values. Return a new hashtable of keys and values. Used to free up memory in the Reducer. 

* `post_reduce`: Aggregate function after all the Reduce tasks are completed for each Reducer. Takes in an hashtable of key and values. Return a value to the master node.

* `aggregate`: Aggregate function at the master node. Aggregates all the values returned by all the Reducers. Takes in an array of values (same as number of Reducers).



*v0.0.19*
* Added `combine` function to allow user to define the `reduce` function to run at the `Mapper`.
* Added `drain` function to allow user to clear the memory of the `Reducer` after each reduce task.
* Added example on how to rehash "long" user ids into unique integers.

---
### Usage
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

#### Starting the MapReduce operation
```javascript
var mrcluster
	.file("mockdata_from_mockaroo.csv")
	.start();
```


##### Settings - file
Specify the csv file or files to read in. 
```javascript
mrcluster.file("mockdata_from_mockaroo.csv");
```

If an array of files are defined, each `Mapper` will parse 1 file as a single block.
```javascript
mrcluster.file(["file1.csv","file2.csv","file3.csv"]);
```

##### Settings - lineDelimiter (optional)
Specify the delimiter to indicate a new line. Default is `\n`.  
```javascript
mrcluster.lineDelimiter('\n');
```

##### Settings - numBlocks (optional)
Specify the number of blocks to split the file into. Default is `2`.
As each NodeJs process (aka each `Mapper` / `Reducer`) is limited to ~1 Gb RAM (x64), you might want to break up the file into sufficiently small blocks. 
```javascript
mrcluster.numBlocks(9);
```

##### Settings - sample (optional)
Specify the number of Blocks to sample. The min number of samples must be >= number of `Mappers`. Default is `-1` (Do not sample - run everything).  
This function is useful to have a quick test of your codes before actually running through the entire dataset.
```javascript
mrcluster.sample(1);
```

##### Settings - numMappers (optional)
Specify the number of mappers to create. Default is `2`.
```javascript
mrcluster.numMappers(2);
```

##### Settings - map
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

##### Settings - combine (optional)
The `combine` function is essentially the `reduce` operation to perform at the mapper, as some reduce jobs can be done at the mapper instead of the reducer.
By default the `combine` function will be the same as the `reduce` function. However, you can use this function call to specify a different `reduce` function at the mapper.
```javascript
mrcluster    
	.combine(function (a,b) {
        return a + b;
    })
```

##### Settings - mapOnly (optional)
Specify whether to run only Mappers. Default is `False`.  
Note that you still need to specify your `Reduce` function as the `Reduce` step is also performed in the `Mapper`. 
```javascript
mrcluster.mapOnly(true)
```

##### Settings - numReducers (optional)
Specify the number of reducers to create. Default is `3`.
The underlying codes will hash all key-values pairs produced by the mappers into the respective reducers. Hence, each chunks of key-values pairs in each reducer is independent of each other. This reduces memory usage when doing the reduce operation. 

```javascript
mrcluster.numReducers(3);
```

##### Settings - reduce
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

##### Settings - drain (optional)
In the MR task, each reducer actually hold in memory the hashtable of key-value pairs it has received so far. For some reduce tasks (e.g. concat, or append tasks), the size of the value in the key-value pair increases after each reduce task which might lead to out of memory problems. 
The `drain` function can be used to free up memory in some of these situation. The `drain` function takes in a `hashtable` of the current key-value pairs held in memory by the reducer, and returns the new `hashtable`.

For example, you wish to rehash long user ids into unqiue integers. You can set the key as the user id and the value as remaining data at the Mapper. Your `combine` function can concat the data by keys. 
The memory usage for the reducers will monotonically increase with each reduce pass if you keep appending new data by keys. So you can specify a `drain` function where you write the current data to file, and return a new hashtable with the keys but without the data. Hence, you can continue appending new data in your `reduce` function.   
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

##### Settings - post_reduce (optional)
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

##### Settings - aggregate (optional)
Specify the function to be applied at the end of all tasks. 
The function should take in an `Array` (representing the hash bins) holding all the returned Values produced by the `post_reduce` function (e.g. You can do a summation of all the returned sums of all the `Reducers`).  
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
    .lineDelimiter('\n')
	.numBlocks(9)
	.numMappers(3)
	// function to map a line of data to a key-value pair
    .map(function (line) {
		// tokenize line
		// select 2nd col and tokenize it again
		// get the domain or return NA if null
		// return a key-value pair of format [domain,1]
        return [line.split(',')[1].split('@')[1] || 'NA', 1];
    })
	// all the domain keys produced will be hashed into 3 bins, which will be handled by 3 Reducers
    .hash(3)	
	// simple reduce function which return a value of 1 (aka return [domainX,1] when [domainX,1] meet [domainX,1])
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
	.numBlocks(9)
	.numMappers(3)
    .map(function (line) {
		var a = line.split(',')[1].split('@');
        return [a[1] || 'NA', [a[0]]];
    })
    .hash(3)
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
Finding users share same domain for their emails.
```javascript
var mrcluster = require("mrcluster");

mrcluster.init()
    .file("mockdata_from_mockaroo.csv")
    .lineDelimiter('\n')
    .numBlocks(10)
    .numMappers(2)
    .map(function (line) {
		var d = line.split(',');
		var id = d.shift();
        return [id, [d.join(',')]];
    })
    .hash(7)
    .combine(function (a, b) {
        return a.concat(b);
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
				lines += (id*7+ctx.id)+','+d+"\n";		// ctx.id is the id of the reducer
			});
			id++;
		}
		fs.appendFile('res_'+ctx.id+'.csv',lines);
		return obj;
	})
    .start();
```
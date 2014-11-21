Node-MapReduce
==============

A multi-core pseudo-MapReduce implementation on NodeJS

### Installation
```
npm install mrcluster
```

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

##### Settings - lineDelimiter
Specify the delimiter to indicate a new line. Default is `\n`.  
```javascript
mrcluster.lineDelimiter('\n');
```

##### Settings - numBlocks
Specify the number of blocks to split the file into. Default is `2`.
As each NodeJs process (aka each `Mapper` / `Reducer`) is limited to ~1 Gb RAM (x64), you might want to break up the file into sufficiently small blocks. 
```javascript
mrcluster.numBlocks(9);
```

##### Settings - sample
Specify the number of Blocks to sample. The min number of samples must be >= number of `Mappers`. Default is `-1` (Do not sample - run everything).  
This function is useful to have a quick test of your codes before actually running through the entire dataset.
```javascript
mrcluster.sample(1);
```

##### Settings - numMappers
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

##### Settings - mapOnly
Specify whether to run only Mappers. Default is `False`.  
Note that you still need to specify your `Reduce` function as the `Reduce` step is also performed in the `Mapper`. 
```javascript
mrcluster.mapOnly(true)
```

##### Settings - hash
Specify the number of hashes the hash function will generate. Default is `3`. The primary reason for the hash bin is to allocate which `Reducer` to handle which key-value pairs. The keys are hashed and then send to the corresponding `Reducer`.
The number of `Reducers` are currently fixed to be the same as the max number of hashes - each `Reducer` is assigned to one hash bin.
```javascript
mrcluster.hash(3);
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

##### Settings - post_reduce
Specify the function to be applied at the end of each `Reducer` execution. 
The function should take in an `Associative Array` holding all the key-values produced by the `Reducer`. And can return any value to the master node for further collation (e.g. sum).
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

##### Settings - aggregate
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

## Example 1 - Counting Unique Ids
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

## Example 2 - Finding similar users
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
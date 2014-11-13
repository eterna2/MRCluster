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
Specify the csv file to read in. 
```javascript
mrcluster.file("mockdata_from_mockaroo.csv");
```

##### Settings - lineDelimiter
Specify the delimiter to indicate a new line. Default is `\n`.  
```javascript
mrcluster.lineDelimiter('\n');
```

##### Settings - numBlocks
Specify the number of blocks to split the file into. Default is `2`.
```javascript
mrcluster.numBlocks(9);
```

##### Settings - sample
Specify the number of Blocks to sample. The min number of samples must be >= number of `Mappers`. Default is `-1` (Do not sample - run everything).  
```javascript
mrcluster.sample(1);
```

##### Settings - write2disk
Specify whether to write all the `Mapper` and `Reducer` outputs to file. Default is `False`.  
```javascript
mrcluster.write2disk(true);
```

##### Settings - numMappers
Specify the number of mappers to create. Default is `2`.
```javascript
mrcluster.numMappers(2);
```

##### Settings - map
Specify the mapping function to be applied on each line of data. 
The function should take in a `String` representing a line of data, and returns an `Array[2]` representing the resultant key-value pair.
```javascript
mrcluster    
	.map(function (line) {
        return [line.split(',')[0], 1];
    })
```

##### Settings - hash
Specify the number of hashes the hash function will generate. Default is `3`. 
The number of `Reducers` are currently fixed to be the same as the max number of hashes - each `Reducer` is assigned to one hash bin.
```javascript
mrcluster.hash(3);
```

##### Settings - reduce
Specify the reduce function to be applied. 
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
Specify the function to be applied at the end of the `Reducer` execution. 
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

#### Example 1
A simple count of number of unique domains in the email list.
```javascript
var mrcluster = require("mrcluster");

mrcluster.init()
    .file("mockdata_from_mockaroo.csv")
    .lineDelimiter('\n')
	.numBlocks(9)
	.numMappers(3)
    .map(function (line) {
        return [line.split(',')[1].split('@')[1] || 'NA', 1];
    })
    .hash(3)
    .reduce(function (a, b) {
        return 1;
    })
    .post_reduce(function (obj) {
        var res = Object.keys(obj).map(function (key) {
            return obj[key];
        });
		console.log(obj)
        return res.reduce(function (a, b) {
            return a+b;
        });
    })
    .aggregate(function (hash_array) {
        console.log("Total: " + hash_array.reduce(function (a, b) {
            return a + b;
        }))
    })
    .start();
```
Node-MapReduce
==============

A multi-core pseudo-MapReduce implementation on NodeJS

### Installation
```
npm install mrcluster
```

### Usage
```javascript
var MapReduce = require("mrcluster");

MapReduce.file()

```

### Example 1
A simple count of number of unique countries.
```javascript
var MapReduce = require("mrcluster");

MapReduce.init({
    
	filename: "us-500.csv",
	
	linebreak: '\r',
    
	numBlocks: 25,
    
	numMappers: 3,
    
	numReducers: 3,
    
	mapper: function (piece) {
        return [piece.split(',')[0], 1];
    },
    
	reducer: function (a, b) {
        return b;
    },
    
	post_reducer: function (chunk) {

        var count = 0;
        for (var key in chunk)++count;


        console.log(count);
        return count;
    },
    
	aggregate_reducer: function (answer) {
        console.log(answer.reduce(function (a, b) {
            return a + b;
        }));
    },
    
	hash: function (value) {
        value += "";
        var hash = 0,
            i, chr, len;
        if (value.length == 0) return hash;
        for (i = 0, len = value.length; i < len; i++) {
            chr = value.charCodeAt(i);
            hash = ((hash << 5) - hash) + chr;
            hash |= 0; // Convert to 32bit integer
        }
        return Math.abs(hash % 3);
    }
})
```
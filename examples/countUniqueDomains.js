var MapReduce = require("mrcluster");

MapReduce.init()
    .file("mockdata_from_mockaroo.csv")
    .lineDelimiter('\n')
	.blockSize(1)
	.numMappers(3)
    .numReducers(3)
    .map(function (line) {
        return [line.split(',')[1].split('@')[1] || 'NA', 1];
    })
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
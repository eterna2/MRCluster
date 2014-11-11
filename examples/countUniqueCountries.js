var MapReduce = require("mrcluster");

MapReduce
	.file("us-500.csv")
	.start();

const ctx = this;
const fs = require('fs');

process.on('message', function (msg) {
    if (msg.compile) {
        eval(msg.compile);
        var res = post_reducer(ctx.reduceResults);
        process.nextTick(function () {
            process.send({
                compileDone: true,
                result: res
            })
        })
        return;
    }
	if (msg.linebreak) ctx.linebreak = msg.linebreak;
    if (msg.mapperFunction) {
        eval(msg.mapperFunction);
        ctx._mapperFunction = mapper;
    }
    if (msg.reducerFunction) {
        eval(msg.reducerFunction);
        ctx._reducerFunction = reducer;

    }
    if (msg.hashFunction) {
        eval(msg.hashFunction);
        ctx._hashFunction = hash;
    }
    if (msg.startMap) initMapTask(msg.numHash, msg.file, msg.start, msg.end);
    else if (msg.startReduce) initReduceTask(msg.chunk);
    else if (msg.kill) process.exit(0);
});

function initMapTask(numHash, file, start, end) {

    var results = new Array(numHash), lastLine = "";
    var stream = new fs.ReadStream(file, {
            start: start,
            end: end,
            encoding: "utf8"
        })
		.on('data', parseData)
		.on('end', done)
        .on('error', function (err) { throw err; })

	var linebreak = ctx.linebreak,
		mapperFunction = ctx._mapperFunction,
		hashFunction = ctx._hashFunction,
		reducerFunction = ctx._reducerFunction;

	function done()
	{
		if (lastLine.length > 0) doMapTask(lastLine);
		//console.log('last = '+lastLine);
		process.send({
                    mapDone: true,
                    chunk: results
                });
		//console.log(results);
	}	
	
	function parseData(chunk)
	{
		var lines = chunk.split(linebreak);
		lines[0] += lastLine;
		lastLine = lines.pop();
		
		lines.forEach(doMapTask);
		
	}
	
	function doMapTask(d)
	{
		if (d.length <= 0) return;
		var valuepair = mapperFunction(d);
		
		var hash = hashFunction(valuepair[0]);
		results[hash] = results[hash] || {};
		results[hash][valuepair[0]] = reducerFunction(results[hash][valuepair[0]], valuepair[1]);
	}
}



function initReduceTask(chunk) {

    var reduce = ctx._reducerFunction;
    var res = ctx.reduceResults = ctx.reduceResults || {};

    for (var key in chunk) {
        res[key] = reduce(res[key], chunk[key]);
    }

    process.nextTick(function () {
        process.send({
            reduceDone: true
        });
    })

}
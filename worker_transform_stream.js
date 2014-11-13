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
	if (msg.write2disk) ctx.write2disk = msg.write2disk;
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
		if (ctx._backdoor) 
		{
			ctx._backdoor();		
		}
		if (ctx.write2disk == "csv" || ctx.write2disk == "CSV") {};
		else if (ctx.write2disk) fs.appendFile('Mapper_pid'+process.pid+'.txt',JSON.stringify(results)+"\r\n");
		//console.log(results);
	}	
	
	function parseData(chunk)
	{
		var lines = chunk.split(linebreak);

		lines[0] = lastLine + lines[0];
		lastLine = lines.pop();

		lines.forEach(doMapTask);
		
	}
	
	function doMapTask(d)
	{
		if (d.length <= 0) return;
		var valuepair = mapperFunction(d);
		
		var hash = hashFunction(valuepair[0]);
		results[hash] = results[hash] || {};
		results[hash][valuepair[0]] = (!results[hash][valuepair[0]]) ? valuepair[1] : reducerFunction(results[hash][valuepair[0]], valuepair[1]);
	}
}



function initReduceTask(chunk) {

    var reduce = ctx._reducerFunction;
    var res = ctx.reduceResults = ctx.reduceResults || {};
	
	
    for (var key in chunk) {
        res[key] = (!res[key]) ? chunk[key] : reduce(res[key], chunk[key]);
    }

	if (ctx.write2disk == "csv" || ctx.write2disk == "CSV") 
	{
		var array = [];
		for (var key in res)
		{
			array.push(res[key]);
		}
		fs.writeFile('Reducer_pid'+process.pid+'.txt',array.join('\n'));
		
	}
	else if (ctx.write2disk) fs.writeFile('Reducer_pid'+process.pid+'.txt',JSON.stringify(res));

	if (ctx._backdoor) 
	{
		ctx._backdoor();		
	}
	
    process.nextTick(function () {
        process.send({
            reduceDone: true
        });
    })

}
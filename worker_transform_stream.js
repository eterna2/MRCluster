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
	if (msg.map2disk) ctx.map2disk = msg.map2disk;
	if (msg.reduce2disk) ctx.reduce2disk = msg.reduce2disk;
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

	var options = (start<0 || end<0)?{ encoding: "utf8"}:{ start:start, end:end, encoding: "utf8"};
    var results = new Array(numHash), lastLine = "";
    var stream = new fs.ReadStream(file, options)
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

		if (ctx.map2disk) 
		{
			
			results.forEach(function(d,i){
				
				for (var key in d)
				{
					fs.appendFile('Mapper_'+encodeURIComponent(key)+'.txt',d[key]+"\n");
				}
			})
		}
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

	if (ctx.reduce2disk) 
	{
		var array = [];
		for (var key in res)
		{
			array.push(res[key]);
		}
		fs.writeFileSync('Reducer_pid'+process.pid+'.txt',array.join('\n'));
		//array = null;
	}

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
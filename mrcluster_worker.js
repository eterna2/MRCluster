const ctx = this;
const fs = require('graceful-fs');
var _fn = {};

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
	if (msg.id) ctx.id = msg.id;
	if (msg.map2disk) ctx.map2disk = msg.map2disk;
	if (msg.reduce2disk) ctx.reduce2disk = msg.reduce2disk;
	if (msg.linebreak) ctx.linebreak = msg.linebreak;
	if (msg.csv) ctx._csv = msg.csv;
	if (msg.cache) ctx._cache = msg.cache;
	if (msg.stopwords) ctx._stopwords = msg.stopwords;
    if (msg.mapperFunction) {
        eval(msg.mapperFunction);
        ctx._mapperFunction = mapper;
    }
    if (msg.combinerFunction) {
        eval(msg.combinerFunction);
        ctx._combinerFunction = reducer;

    }
    if (msg.reducerFunction) {
        eval(msg.reducerFunction);
        ctx._reducerFunction = reducer;

    }
    if (msg.drainFunction) {
        eval(msg.drainFunction);
        ctx._drainFunction = drain;
    }
    if (msg.hashFunction) {
        eval(msg.hashFunction);
        ctx._hashFunction = hash;
    }
	if (msg.fn)
	{
		ctx._fn = ctx._fn || {};
		msg.fn.forEach(function(d){
			eval("_fn['"+d[0]+"']="+d[1]);
		})
	}
    if (msg.startMap) initMapTask(msg.numHash, msg.file, msg.start, msg.end);
    else if (msg.startReduce) initReduceTask(msg.chunk);
    else if (msg.kill) process.exit(0);
});

function initMapTask(numHash, file, start, end) {

	ctx._file = file;
	var options = (start<0 || end<0)?{ encoding: "utf8"}:{ start:start, end:end, encoding: "utf8"};
    var results = new Array(numHash), lastLine = "";
    var stream = new fs.ReadStream(file, options)
		.on('data', parseData)
		.on('end', done)
        .on('error', function (err) { throw err; })

	var linebreak = ctx.linebreak,
		mapperFunction = (!ctx._csv) ? ctx._mapperFunction : function(d){ return ctx._mapperFunction(CSVtoArray(d))},
		hashFunction = ctx._hashFunction,
		reducerFunction = ctx._combinerFunction;

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
		if (Array.isArray(valuepair)) mapValuePair(valuepair);
		else
		{
			var valuepairs = Object.keys(valuepair).map(function(d){return [d,valuepair[d]];});
			valuepairs.forEach(mapValuePair);
		}

	}
	
	function mapValuePair(valuepair)
	{
		var hash = hashFunction(valuepair[0]);
		results[hash] = results[hash] || {};
		results[hash][valuepair[0]] = (!results[hash][valuepair[0]]) ? valuepair[1] : reducerFunction(results[hash][valuepair[0]], valuepair[1]);
	}
}

function initReduceTask(chunk) {

    var reduce = ctx._reducerFunction;
    var res = ctx.reduceResults = ctx.reduceResults || {};
	
	
    for (var key in chunk) {
		if (ctx._stopwords.indexOf(key)>=0) continue;
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

	if (ctx._drainFunction) {
		ctx.reduceResults = ctx._drainFunction(res);
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


// Source: ridgerunner @ http://stackoverflow.com/questions/8493195/how-can-i-parse-a-csv-string-with-javascript
// Return array of string values, or NULL if CSV string not well formed.
function CSVtoArray(text) {
    var re_valid = /^\s*(?:'[^'\\]*(?:\\[\S\s][^'\\]*)*'|"[^"\\]*(?:\\[\S\s][^"\\]*)*"|[^,'"\s\\]*(?:\s+[^,'"\s\\]+)*)\s*(?:,\s*(?:'[^'\\]*(?:\\[\S\s][^'\\]*)*'|"[^"\\]*(?:\\[\S\s][^"\\]*)*"|[^,'"\s\\]*(?:\s+[^,'"\s\\]+)*)\s*)*$/;
    var re_value = /(?!\s*$)\s*(?:'([^'\\]*(?:\\[\S\s][^'\\]*)*)'|"([^"\\]*(?:\\[\S\s][^"\\]*)*)"|([^,'"\s\\]*(?:\s+[^,'"\s\\]+)*))\s*(?:,|$)/g;
    // Return NULL if input string is not well formed CSV string.
    if (!re_valid.test(text)) return null;
    var a = [];                     // Initialize array to receive values.
    text.replace(re_value, // "Walk" the string using replace with callback.
        function(m0, m1, m2, m3) {
            // Remove backslash from \' in single quoted values.
            if      (m1 !== undefined) a.push(m1.replace(/\\'/g, "'"));
            // Remove backslash from \" in double quoted values.
            else if (m2 !== undefined) a.push(m2.replace(/\\"/g, '"'));
            else if (m3 !== undefined) a.push(m3);
            return ''; // Return empty string.
        });
    // Handle special case of empty last value.
    if (/,\s*$/.test(text)) a.push('');
    return a;
};
const fs = require('graceful-fs');

const _mapper = "var mapper=function(line){return [line.split(',')[0], 1];};";
const _reducer = "var reducer=function(a, b){return b;};";
const _post_reducer = "var post_reducer=function(chunk){var count = 0;for (var key in chunk)++count;console.log(count);return count;};";
const _hash = "var hash=function(value){ value=value.toString(); var hash=0, i, chr, len; if (value.length==0) return hash; for (i = 0, len = value.length; i < len; i++) { chr = value.charCodeAt(i);hash = ((hash << 5) - hash) + chr; hash |= 0;  };  return Math.abs(hash%3);};";

function genHashFunction(numHash)
{
	return "var hash=function(value){ value=value.toString(); var hash=0, i, chr, len; if (value.length==0) return hash; for (i = 0, len = value.length; i < len; i++) { chr = value.charCodeAt(i);hash = ((hash << 5) - hash) + chr; hash |= 0;  };  return Math.abs(hash%"+Math.max(1,parseInt(numHash))+");};";
}

function MapReduce() {
    var ctx = this;
	
	ctx._numBlocks = 2;
	ctx._linebreak = '\n';
	ctx._numMappers = 2;
	ctx._numReducers = 3;
	ctx._mapper = _mapper;
	ctx._reducer = _reducer;
	ctx._hash = _hash;
	ctx._post_reducer = _post_reducer;
	ctx._aggregate_reducer = _aggregate_reducer;
	ctx._sample = -1;
	
	ctx.sample = function(sample)
	{
		ctx._sample = parseInt(sample);
		return ctx;
	};
	
	ctx.file = function(file)
	{
		if (Array.isArray(file)) ctx._files = file;
		else ctx._file = file;
		return ctx;
	};

	ctx.numBlocks = function(numBlocks)
	{
		ctx._numBlocks = numBlocks;
		return ctx;
	};

	ctx.lineDelimiter = function(lineDelimiter)
	{
		ctx._linebreak = lineDelimiter;
		return ctx;
	};

	ctx.numMappers = function(numMappers)
	{
		ctx._numMappers = numMappers;
		return ctx;
	}

	ctx.map = function(func,map2disk)
	{
		ctx._map2disk = map2disk;
		ctx._mapper = "var mapper="+func.toString();
		return ctx;
	};

	ctx.mapOnly = function(mapOnly)
	{
		ctx._mapOnly = mapOnly;
		return ctx;
	}
	
	ctx.reduce = function(func,reduce2disk)
	{
		ctx._reduce2disk = reduce2disk;
		ctx._reducer = "var reducer="+func.toString();
		return ctx;
	};
	
	ctx.customhash = function(numReducers, func)
	{
		ctx._numReducers = numReducers;
		ctx._hash = "var hash="+func.toString();
		return ctx;
	};

	ctx.hash = function(numHash)
	{
		ctx._numReducers = numHash;
		ctx._hash = genHashFunction(numHash);
		return ctx;
	};

	
	ctx.post_reduce = function(func)
	{
		ctx._post_reducer = "var post_reducer="+func.toString();
		return ctx;
	};

	ctx.aggregate = function(func)
	{
		ctx._aggregate_reducer = func || _aggregate_reducer;
		return ctx;
	};

    ctx.start = function () {
        ctx._cluster = require('cluster');
		ctx._bin = new Array(ctx._numReducers);
		ctx._answer = new Array(ctx._numReducers);
		ctx._sortHash = _sortHash;
		
        if (ctx._cluster.isMaster) {
            ctx._cluster.setupMaster({
                exec: __dirname+"/worker_transform_stream.js"
                //silent : true
            })
            
			ctx._activeWorkers = 0;
			
			ctx._cluster
				.on('exit', function (worker, code, signal) {
                    console.log('worker#'+worker.id+ " exited");
                })
                .on('disconnect', function (worker) {
                    console.log('worker#'+worker.id+ " disconnected");
                })
				.on('online', function(worker){ 
					worker.send({
							mapperFunction: ctx._mapper,
							reducerFunction: ctx._reducer,
							hashFunction: ctx._hash,
							linebreak: ctx._linebreak,
							map2disk: ctx._map2disk,
							reduce2disk: ctx._reduce2disk
					});
					ctx._activeWorkers++;  
					if (ctx._activeWorkers >= numWorkers) 
					{
						if (ctx._file) analyzeFile(ctx._file, ctx._numBlocks, ctx._run, ctx._linebreak);
						else if (ctx._files) 
						{
							ctx._files.forEach(function(d){
								analyzeFile(d, ctx._numBlocks, ctx._concatJobs, ctx._linebreak);
							})
							ctx._run(ctx._jobs);
						}
					}
					console.log('worker#'+worker.id+ " online");

				});

			var numWorkers = ctx._numReducers + ctx._numMappers;
            for (var i=0; i<numWorkers; i++)
			{
                ctx._cluster.fork().on('message', messageHandler);
			}          

        }
		return ctx;
    };

	ctx._concatJobs = function(jobs)
	{
		ctx._jobs = ctx._jobs || [];
		ctx._jobs = ctx._jobs.concat(jobs);
	}
	
    ctx._run = function(jobs) {
		if (ctx._sample > 0) jobs = jobs.slice(0,Math.max(ctx._sample,ctx._numMappers));
		ctx._startTime = process.hrtime();
		ctx._jobs = jobs;
		ctx._jobsLeft = jobs.length;
		ctx._reducJobsLeft = 0;
		var numReducers = ctx._numReducers,
			numMappers = ctx._numMappers;
		ctx._reducerState = new Array(numReducers);
		for (var i=0; i<numMappers; i++)
		{
			_startMapper(ctx,numReducers+i);
		}
		
		return ctx;
    };
	
    ctx.broadcast = function (callback) {
        for (var id in ctx._cluster.workers) {
            callback(ctx._cluster.workers[id]);
        }
		return ctx;
    };

    ctx.killAll = function () {
        ctx.broadcast(function (worker) {
            worker.kill();
        })
		return ctx;
    };

    function messageHandler(msg) {
		if (msg.compileDone)
		{
			--ctx._answersLeft;
			ctx._answer[this.id-1] = msg.result;
			if (ctx._answersLeft == 0) 
			{
				ctx._aggregate_reducer(ctx._answer)
				var dt = process.hrtime(ctx._startTime);	
				console.info("Execution time: %ds %dms", dt[0], dt[1]/1000000);
				ctx.killAll();
			}
			
			return;
		}
        if (msg.mapDone) 
		{
			--ctx._jobsLeft;
			console.log("Map Jobs Remaining: "+ctx._jobsLeft)
			_startMapper(ctx,this.id);

			if (ctx._mapOnly && ctx._jobsLeft == 0) 
			{
				var dt = process.hrtime(ctx._startTime);	
				console.info("Execution time: %ds %dms", dt[0], dt[1]/1000000);
				ctx.killAll();
				return;
			}
			else if (ctx._mapOnly) return;
			ctx._sortHash(ctx._bin, msg.chunk);
        }
		else if (msg.reduceDone) 
		{
			console.log("Reduce Jobs Remaining: "+ctx._reducJobsLeft)
			ctx._reducerState[this.id-1] = false;
			_startReducer(ctx,this.id-1,ctx._bin[this.id-1].pop());
        }


		var _bin = ctx._bin, reduceJobsLeft = 0;
		for (var hash in _bin)
		{
			if (!ctx._reducerState[hash]) _startReducer(ctx,hash,_bin[hash].pop());
			reduceJobsLeft += _bin[hash].length;
		}
		ctx._reducJobsLeft = reduceJobsLeft;
		var reducerDone = ctx._reducerState.every(function(d){return !d;});
		if (reducerDone && ctx._jobsLeft == 0 && reduceJobsLeft == 0) _compileResult(ctx);
    };

    return ctx;
};

function _startMapper(ctx,id)
{
	var job = ctx._jobs.pop();
	if (!job) return false;

	ctx._cluster.workers[id].send({
		startMap: true,
		numHash: ctx._numReducers,
		file: job.file,
		start: job.start,
		end: job.end
	});
};

function _startReducer(ctx,hash,chunk)
{
	if (!chunk) return false;

	hash = parseInt(hash);

	var worker = ctx._cluster.workers[hash+1];
	ctx._reducerState[hash] = true;

	worker.send({
		startReduce: true,
		chunk: chunk
	});
};

function _sortHash(_bin, chunk) {
    chunk.forEach(function (d, i) {
        _bin[i] = _bin[i] || [];
		_bin[i].push(d);
    });
    chunk = null;
};

function _compileResult(ctx) {

	ctx._answersLeft = ctx._numReducers;
	ctx._answer = new Array(ctx._numReducers);
	ctx._reducerState.forEach(function(d,i){ctx._cluster.workers[i+1].send({compile: ctx._post_reducer})})
};

function analyzeFile(filename, numBlocks, callback, linebreak) {

    var fd = fs.openSync(filename, 'r'),
        stats = fs.statSync(filename),
        filesize = stats["size"];
	//console.log(filesize)
    var step = Math.floor(filesize / numBlocks),
		intervals = [];

    for (var i = 0; i < numBlocks; i++) {
		var bufferSize = Math.min(1000,parseInt(step))
		//console.log('bufferSize'+bufferSize)
        var obj = { step: i * step },
            buffer = new Buffer(bufferSize);
        intervals.push(obj)
        fs.readSync(fd, buffer, 0, bufferSize, obj.step)
		var nearestLinebreak = buffer.toString().indexOf(linebreak);
		if (i>0) obj.step += buffer.toString().indexOf(linebreak) + 1;
		//console.log("current="+obj.step+" index="+nearestLinebreak+":"+buffer.toString().substr(nearestLinebreak,15))
    }
	

	var jobs = intervals.map(
		function (d, i, array) {
			return {
				file: filename,
				start: d.step,
				end: (i + 1 >= numBlocks) ? (filesize - 1) : (array[i + 1].step - 1)
			};
		})
	
	console.log("file broken into "+numBlocks+" blocks");
	
	fs.closeSync(fd);
	callback(jobs);


};


function _aggregate_reducer(answer) {
	console.log(answer.reduce(function (a, b) {
		return a + b;
	}));
};




// expose module methods
exports.init = MapReduce;
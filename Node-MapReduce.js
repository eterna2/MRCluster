const fs = require('fs');

function MapReduce(_options) {
    var ctx = this;
	
	ctx._numBlocks = 1;
	ctx._linebreak = '\n';
	ctx._numMappers = 1;
	ctx._numReducers = 3;
	ctx._mapper = _mapper.toString();
	ctx._reducer = _reducer.toString();
	ctx._hash = _hash.toString();
	ctx._post_reduce = _post_reduce.toString();
	ctx._aggregate_reducer = _aggregate_reducer.toString();
	
	ctx.file = function(file)
	{
		ctx._file = file;
	};

	ctx.numBlocks = function(numBlocks)
	{
		ctx._numBlocks = numBlocks;
	};

	ctx.lineDelimiter = function(lineDelimiter)
	{
		ctx._linebreak = lineDelimiter;
	};

	ctx.map = function(numMappers, func)
	{
		ctx._numMappers = numMappers;
		ctx._mapper = "var mapper = "+func.toString();
	};

	ctx.reduce = function(func)
	{
		ctx._reducer = "var reducer = "+func.toString();
	};
	
	ctx.hash = function(numReducers, func)
	{
		ctx._numReducers = numReducers;
		ctx._hash = "var hash = "+func.toString();
	};

	ctx.post_reduce = function(func)
	{
		ctx._post_reduce = "var post_reducer = "+func.toString();
	};

	ctx.aggregate = function(func)
	{
		ctx._aggregate_reducer = func || _aggregate_reducer;
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
							linebreak: ctx._linebreak
					});
					ctx._activeWorkers++;  
					if (ctx._activeWorkers >= numWorkers) analyzeFile(ctx._file, ctx._numBlocks, ctx._run, ctx._linebreak);
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

    ctx._run = function callback(jobs) {
		ctx._startTime = process.hrtime();
		ctx._jobs = jobs;
		ctx._jobsLeft = jobs.length;
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
			_startMapper(ctx,this.id);
			ctx._sortHash(ctx._bin, msg.chunk);
        }
		else if (msg.reduceDone) 
		{
			ctx._reducerState[this.id-1] = false;
			_startReducer(ctx,this.id-1,ctx._bin[this.id-1].pop());
        }
		
		var _bin = ctx._bin, reduceJobsLeft = 0;
		for (var hash in _bin)
		{
			if (!ctx._reducerState[hash]) _startReducer(ctx,hash,_bin[hash].pop());
			reduceJobsLeft += _bin[hash].length;
		}

		var reducerDone = ctx._reducerState.every(function(d){return !d;});
		if (reducerDone && ctx._jobsLeft == 0 && reduceJobsLeft == 0) _compileResult(ctx);
    };

    return ctx.init(_options);
};

function _startMapper(ctx,id)
{
	var job = ctx._jobs.pop();
	if (!job) return false;

	ctx._cluster.workers[id].send({
		startMap: true,
		numHash: ctx._numReducers,
		file: ctx._file,
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

	ctx._answersLeft = ctx.options.numReducers;
	ctx._answer = new Array(ctx.options.numReducers);
	ctx._reducerState.forEach(function(d,i){ctx._cluster.workers[i+1].send({compile: ctx._post_reduce})})
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
				start: d.step,
				end: (i + 1 >= numBlocks) ? (filesize - 1) : (array[i + 1].step - 1)
			};
		})
	//console.log(jobs)
	callback(jobs);


};

function _mapper(line) {
	return [line.split(',')[0], 1];
};

function _reducer(a, b) {
    return b;
};

function _post_reducer(chunk) {
	var count = 0;
	for (var key in chunk)++count;
	console.log(count);
	return count;
};

function _aggregate_reducer(answer) {
	console.log(answer.reduce(function (a, b) {
		return a + b;
	}));
};


function _hash(value) {
    value += "";
    var hash = 0,
        i, chr, len;
    if (value.length == 0) return hash;
    for (i = 0, len = value.length; i < len; i++) {
        chr = value.charCodeAt(i);
        hash = ((hash << 5) - hash) + chr;
        hash |= 0; // Convert to 32bit integer
    }
    return Math.abs(hash%3);
};

// expose module methods
exports.init = MapReduce;
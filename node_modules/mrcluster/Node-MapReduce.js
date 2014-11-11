const fs = require('fs');

function MapReduce(_options) {
    var ctx = this;
	
    ctx.init = function (options) {
        ctx.options = options || {};
        ctx.cluster = require('cluster');
        
		ctx._mapper = "var mapper = "+(options.mapper).toString();
        ctx._reducer = "var reducer = "+(options.reducer).toString();
        ctx._hash = "var hash = "+(options.hash).toString();
        ctx._compile = "var post_reducer = "+(options.post_reducer).toString();
		ctx.aggregate_reducer = options.aggregate_reducer || _aggregate_reducer;

		ctx.bin = new Array(options.numReducers);
		ctx.answer = new Array(options.numReducers);
		ctx.sortHash = _sortHash;
		ctx.options = _options;
		ctx.jobsLeft;

		
        if (ctx.cluster.isMaster) {
            ctx.cluster.setupMaster({
                exec: "worker_transform_stream.js"
                //silent : true
            })
            
			ctx.activeWorkers = 0;
			
			ctx.cluster
				.on('exit', function (worker, code, signal) {
                    if (ctx.options.exit) ctx.options.disconnect(ctx, worker, code, signal);
                })
                .on('disconnect', function (worker) {
                    if (ctx.options.disconnect) ctx.options.disconnect(ctx, worker);
                })
				.on('online', function(worker){ 
					worker.send({
							mapperFunction: ctx._mapper,
							reducerFunction: ctx._reducer,
							hashFunction: ctx._hash,
							linebreak: options.linebreak || '\n'
					});
					ctx.activeWorkers++;  
					if (ctx.activeWorkers >= options.numWorkers) analyzeFile(options.filename, options.numBlocks, ctx.start, options.linebreak);
					console.log(worker.id+" online");
				});

			options.numWorkers = options.numReducers + options.numMappers;
            ctx.workers = new Array(options.numWorkers);
			var n = options.numWorkers;
            while (n > 0) {
                var mapper = ctx.cluster.fork().on('message', messageHandler);
                --n;
            }
          

        }
		return ctx;
    }

    ctx.start = function callback(file, jobs) {
		ctx.startTime = process.hrtime();
		ctx.jobs = jobs;
		ctx.jobsLeft = jobs.length;
		ctx.file = file;
		var numReducers = ctx.options.numReducers;
		ctx.reducerState = new Array(numReducers);
		for (var i=0,len=ctx.options.numMappers; i<len; i++)
		{
			_startMapper(ctx,numReducers+i);
		}
		
		return ctx;
    }

    ctx.broadcast = function (callback) {
        for (var id in ctx.cluster.workers) {
            callback(ctx.cluster.workers[id]);
        }
		return ctx;
    }

    ctx.killAll = function () {
        ctx.broadcast(function (worker) {
            worker.kill();
        })
		return ctx;
    }

    function messageHandler(msg) {
		if (msg.compileDone)
		{
			--ctx.answersLeft;
			//console.log("answers left = "+ctx.answersLeft)
			ctx.answer[this.id-1] = msg.result;
			if (ctx.answersLeft == 0) 
			{
				ctx.aggregate_reducer(ctx.answer)
				var dt = process.hrtime(ctx.startTime);	
				console.info("Execution time: %ds %dms", dt[0], dt[1]/1000000);
				ctx.killAll();
			}
			
			return;
		}
        if (msg.mapDone) 
		{
			//console.log("Mapper "+this.id+" done");
			--ctx.jobsLeft;
			_startMapper(ctx,this.id);
			ctx.sortHash(ctx.bin, msg.chunk);
        }
		else if (msg.reduceDone) 
		{
			//console.log("Reducer "+this.id+" done");
			ctx.reducerState[this.id-1] = false;
			_startReducer(ctx,this.id-1,ctx.bin[this.id-1].pop());
        }
		
		var bin = ctx.bin, reduceJobsLeft = 0;
		for (var hash in bin)
		{
			if (!ctx.reducerState[hash]) _startReducer(ctx,hash,bin[hash].pop());
			reduceJobsLeft += bin[hash].length;
		}
		//else compileResult(ctx.answer, hash, chunk);
		//console.log("reduceJobs = "+reduceJobsLeft)
		var reducerDone = ctx.reducerState.every(function(d){return !d;});
		if (reducerDone && ctx.jobsLeft == 0 && reduceJobsLeft == 0) _compileResult(ctx);
    }

    return ctx.init(_options);
}

function _startMapper(ctx,id)
{
	var job = ctx.jobs.pop();
	if (!job) return false;

	//console.log("start mapper "+id);
	ctx.cluster.workers[id].send({
		startMap: true,
		numHash: ctx.options.numReducers,
		file: ctx.file,
		start: job.start,
		end: job.end
	});
}

function _startReducer(ctx,hash,chunk)
{
	if (!chunk) return false;
	hash = parseInt(hash);
	//console.log(ctx.cluster.workers[1])
	var worker = ctx.cluster.workers[parseInt(hash)+1];
	ctx.reducerState[hash] = true;
	//console.log("start reducer "+hash);

	worker.send({
		startReduce: true,
		chunk: chunk
	});
}

function _sortHash(bin, chunk) {
    chunk.forEach(function (d, i) {
        bin[i] = bin[i] || [];
		bin[i].push(d);
    });
    chunk = null;
}

function _compileResult(ctx) {

	ctx.answersLeft = ctx.options.numReducers;
	ctx.answer = new Array(ctx.options.numReducers);
	ctx.reducerState.forEach(function(d,i){ctx.cluster.workers[i+1].send({compile: ctx._compile})})
}


function _aggregate_reducer(answer) {
	console.log(answer.reduce(function(a,b){return a+b;}))
}

function hash32(value) {
    value += "";
    var hash = 0,
        i, chr, len;
    if (value.length == 0) return hash;
    for (i = 0, len = value.length; i < len; i++) {
        chr = value.charCodeAt(i);
        hash = ((hash << 5) - hash) + chr;
        hash |= 0; // Convert to 32bit integer
    }
    return hash;
};


function analyzeFile(filename, numBlocks, callback, linebreak) {
	linebreak = linebreak || '\n';
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
	callback(filename, jobs);


}

// expose module methods
exports.init = MapReduce;
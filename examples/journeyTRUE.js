var mrcluster = require("./mrcluster");

function countBits(i)
{
     i = i - ((i >>> 1) & 0x55555555);
     i = (i & 0x33333333) + ((i >>> 2) & 0x33333333);
     return (((i + (i >>> 4)) & 0x0F0F0F0F) * 0x01010101) >>> 24;
}

function genHashFunction(numHash)
{
	return (function(value)
	{
		value=value.toString(); 
		var hash=0, i, chr, len; if (value.length==0) return hash; 
		for (i = 0, len = value.length; i < len; i++) 
		{ 
			chr = value.charCodeAt(i);hash = ((hash << 5) - hash) + chr; hash |= 0;  
		};  
		return Math.abs(hash%Math.max(1,parseInt(numHash)));
	})
}

mrcluster.init()
    .file("e:/RIDE_20140623_hashed.csv")
	//.sample(6)
    .lineDelimiter('\n')
    .blockSize(32)
    .numMappers(3)
	.numReducers(7)
	.fn("genHashFunction",genHashFunction)
	/*.partition(7,function(key){ 
		function countBits(i)
		{
			 i = i - ((i >>> 1) & 0x55555555);
			 i = (i & 0x33333333) + ((i >>> 2) & 0x33333333);
			 return (((i + (i >>> 4)) & 0x0F0F0F0F) * 0x01010101) >>> 24;
		}
		return  countBits(((key>>25)))%7; 
		})*/
	//.mapOnly(true)
    .map(function (line) {
		var fields = line.split(','),
			//id = parseInt(fields[0]),
			journey = parseInt(fields[1]),
			//type = fields[2],
			//travel = fields[3],
			start = parseInt(fields[7]),
			end = parseInt(fields[8]),
			date = fields[9],
			time = fields[10],
			dist = parseFloat(fields[11]),
			ridetime = parseFloat(fields[12]);
		var t = Date.parse(date+"T"+time+"+08:00");
		return [journey+"",[start,end,t,dist,t,1,ridetime]];
    })
    .reduce(function (a, b) {
		var start=a[0], 
			end = b[1],
			t1 = a[2], 
			dist = a[3]+b[3],
			t2 = b[4], 
			trips = a[5]+b[5]
			ridetime = a[6]+b[6];
		if (a[2] > b[2])
		{
			t1 = b[2];
			start = b[0];
		}
		if (a[4] > b[4])
		{
			t2 = a[4];
			end = a[1];
		}
		return [start,end,t1,dist,t2,trips,ridetime];
	})
	.drain(function(hashtable){
		var memUsage = process.memoryUsage().heapUsed/1048576;

		if (memUsage > 350)
		{	
			var len = Object.keys(hashtable).length;
			ctx._subhash = ctx._subhash || parseInt(Math.max(len/10000, 5));
			
			var subhash = _fn.genHashFunction(ctx._subhash)
			console.log("Reducer#"+ctx.id+": Spilling to DISK.")
			
			var tmp = new Array(ctx._subhash);
			var reduce = ctx._reducerFunction;
			
			for (var key in hashtable)
			{
				var h = subhash(key);
				tmp[h] = tmp[h] || {};
				tmp[h][key]=hashtable[key];
			}
			var i = 0;
			while (tmp.length > 0)
			{
				var d = tmp.pop() || {};
				var filename = __dirname+"/_reducer"+ctx.id+"_"+i+".tmp";
				if (ctx._buffered && fs.existsSync(filename)) 
				{
					var h_tmp = JSON.parse(fs.readFileSync(filename,{encoding:"utf8"}));
					for (var key in h_tmp)
					{
						d[key] = (!d[key]) ? h_tmp[key] : reduce(d[key], h_tmp[key]);
					}
				}
				fs.writeFileSync(__dirname+"/_reducer"+ctx.id+"_"+i+".tmp",JSON.stringify(d));
				i++;
				if (global.gc) global.gc();
			}

			ctx._buffered = true;
			tmp = null;
			hashtable = null;
			
			return {};
		}
		return hashtable;
	})
	//k, from_stn, to_stn, start_dt, start_time, total_dist, total_time, num_trips
    .post_reduce(function (hashtable) {
		if (ctx._buffered) 
		{
			
			var tmp = new Array(ctx._subhash);
			var reduce = ctx._reducerFunction;
			var subhash = _fn.genHashFunction(ctx._subhash)
			
			for (var key in hashtable)
			{
				var h = subhash(key);
				tmp[h] = tmp[h] || {};
				tmp[h][key]=hashtable[key];
				hashtable[key] = null;
			}
						
			var i = ctx._subhash-1;
			while (tmp.length >0)
			{
				var d = tmp.pop();
				var filename = __dirname+"/_reducer"+ctx.id+"_"+i+".tmp";
				if (fs.existsSync(filename)) 
				{
					//console.log("Reducer#"+ctx.id+": reading from "+filename)
					var h_tmp = JSON.parse(fs.readFileSync(filename,{encoding:"utf8"}));
					for (var key in h_tmp)
					{
						d[key] = (!d[key]) ? h_tmp[key] : reduce(d[key], h_tmp[key]);
					}
					
					fs.unlinkSync(filename);
					console.log("Reducer#"+ctx.id+": "+(100*(1-tmp.length/ctx._subhash)).toFixed(0)+"% Done");
				}
				var lines = "";
				for (var key in d)
				{
					var p = d[key];
					p[4] = p[6];
					//p[2] = (new Date(p[2])).toLocaleString();
					p.pop();
					lines += key+","+p.join(',')+"\n";
				}
				fs.appendFileSync("journey.csv",lines);
				i--;
				if (global.gc) global.gc();

			}
			
		}
		
		else 
		{
			var lines = "";
			
			for (var key in hashtable)
			{
				var p = hashtable[key];
				p[4] = p[6];
				//p[2] = (new Date(p[2])).toLocaleString();
				p.pop();
				lines += key+","+p.join(',')+"\n";
			}
			fs.appendFileSync("journey.csv",lines);
		}
		return 1;
    })
    .start();
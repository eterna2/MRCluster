var mrcluster = require("mrcluster");

mrcluster.init()
    .file("mockdata2_from_mockaroo.csv")
    .lineDelimiter('\n')
    .numBlocks(10)
    .numMappers(2)
    .map(function (line) {
		var d = line.split(',');
		var id = d.shift();
        return [id, [d.join(',')]];
    })
    .hash(7)
    .combine(function (a, b) {
        return a.concat(b);
    })
    .reduce(function (a, b) {
        return a.concat(b);
    })
	.drain(function(list){
		var id = 0, lines = "", obj = {};
		for (var key in list)
		{
			obj[key] = [];
			list[key].forEach(function(d){
				lines += (id*7+ctx.id)+','+key+','+d+"\n";		// ctx.id is the id of the reducer
			});
			id++;
		}
		fs.appendFile('res_'+ctx.id+'.csv',lines);
		return obj;
	})
    .start();
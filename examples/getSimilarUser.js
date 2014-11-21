var mrcluster = require("mrcluster");

mrcluster.init()
    .file("mockdata_from_mockaroo.csv")
    .lineDelimiter('\n')
	.numBlocks(9)
	.numMappers(3)
    .map(function (line) {
		var a = line.split(',')[1].split('@');
        return [a[1] || 'NA', [a[0]]];
    })
    .hash(3)
    .reduce(function (a, b) {
        return a.concat(b);
    })
    .post_reduce(function (obj) {
		var lines = "";
        Object.keys(obj).forEach(function (key) {
			var tmp = {};
			obj[key].forEach(function(d){tmp[d]=true;});
            lines += key+','+Object.keys(tmp).join(',')+'\n';
        });
		fs.appendFile('results.csv',lines);
        return 0; // trivial return
    })
    .start();

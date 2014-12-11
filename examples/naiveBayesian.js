var fs = require('graceful-fs'),
	MapReduce = require("./mrcluster.js");

var text = "Myocyte cell loss and myocyte cellular hyperplasia in the hypertrophied aging rat heart. To determine the effects of age on the myocardium  the functional and structural characteristics of the heart were studied in rats at 4  12  20  and 29 months of age. Mean arterial pressure  left ventricular pressure and its first derivative  dP dt   and heart rate were comparable in rat groups up to 20 months. During the interval from 20 to 29 months  elevated left ventricular end-diastolic pressure and decreased dP dt indicated that a significant impairment of ventricular function occurred with senescence. In the period between 4 and 12 months  a reduction of nearly 19  in the total number of myocytes was measured in both ventricles. In the subsequent ages  similar decreases in myocyte cell number were found in the left ventricle  whereas in the right ventricle  the initial loss was fully reversed by 20 months. Moreover  from 20 to 29 months  a 59  increase in the aggregate number of myocytes occurred in the right ventricular myocardium. In the left ventricle  a 3  increment was also seen  but this small change was not statistically significant. These estimations of myocyte cellular hyperplasia  however  were complicated by the fact that cell loss continued to take place with age. The volume fraction of collagen in the tissue  in fact  progressively increased from 8  and 7  at 4 months to 16  and 22  at 29 months in the left and right ventricles  respectively. In conclusion  myocyte cellular hyperplasia tends to regenerate the ventricular mass being lost with age in the adult mammalian rat heart. Anversa P  Palackal T  Sonnenblick EH  Olivetti G  Meggs LG  Capasso JM."

var stopwords = fs.readFileSync('stopwords.csv',{encoding:"utf8"})
					.trim()
					.split(',')
					.map(function(d){return "\m"+d+"$"})
					
				
					
MapReduce.init()
    .file("ohsumed-allcats.csv")
    .lineDelimiter('\n')
	.cache({stopwords:stopwords, bag:parse2Bag(text)})
    .map(function (line) {
		// group, text
		var values = line.split(',');
		// multiple key-value pairs to map into
		var hashtable = {}, key = values[0];
		// regex for stopwords
		var regex = new RegExp("[^a-zA-Z ]",'g')
		// parse text into bag of words
		var words = values[1]
						.toLowerCase()
						.replace(regex,'')	// keep only alphanumeric
						.split(' ')
						.filter(function(d){return (d.length > 1)&&(ctx._cache.stopwords.indexOf(d)<0);})
		
		hashtable[key]=hashtable[key]||1;
		hashtable[key+",_word"]=words.length;
		words.forEach(function(d){
			hashtable[key+","+d]=hashtable[key+","+d]||0;
			++hashtable[key+","+d];
		});
        return hashtable;
    })
    .reduce(function (a, b) {
        return a+b;
    })
    .post_reduce(function (hashtable) {
		return hashtable;
    })
	.aggregate(function(array){
		var bayesian = {};
		array.forEach(function(d){
			for (var k in d)
			{
				var grp = k.split(',');
				bayesian[grp[0]] = bayesian[grp[0]] || {};
				if (grp.length == 1) 
				{
					bayesian[grp[0]]["_total"] = d[k];
				}
				else
				{
					bayesian[grp[0]][grp[1]] = d[k];
				}
			}
		})
		var prob = predict(text,bayesian);
		console.log(prob)
	})
    .start();
	
	

function parse2Bag(text)
{
	var regex = new RegExp("[^a-zA-Z ]",'g')
	// parse text into bag of words
	var words = text
					.toLowerCase()
					.replace(regex,'')	// keep only alphanumeric
					.split(' ')
					.filter(function(d){return (d.length > 1)&&(stopwords.indexOf(d)<0);})
	
	return words;
}	

function predict(text,bayesian)
{
	var bagOfwords = parse2Bag(text);
	var laplacian = bagOfwords.length;
	var totalDoc = 0;
	for (var grp in bayesian)
	{
		totalDoc += bayesian[grp]._total;
	}
	var prob = [];
	for (var grp in bayesian)
	{
		var num_C = bayesian[grp]._total+laplacian, 
			num_word = bayesian[grp]._word,
			p_C = Math.log(num_C/totalDoc),
			pX_C = bagOfwords.map(function(word){
						var count = bayesian[grp][word] || 1;
						return Math.log(count/num_word);
					})
					.reduce(function(a,b){return a+b;})

		prob.push([grp,pX_C+p_C]);

	}
	
	prob.sort(function(a,b){
		return b[1]-a[1];
	})
	
	return prob[0][0];
}
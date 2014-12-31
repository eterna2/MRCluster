function Bernstein(str) {
	var hash = 5381, i = str.length;
	while (i)	hash = (hash * 33) ^ str.charCodeAt(--i);
	return hash >>> 0;
}

function Kernighan(str) {
	var hash = 0, i = str.length;
	while (i)	hash = (hash * 31) ^ str.charCodeAt(--i);
	return hash >>> 0;
}

    
function sdbm(str) {
	var hash = 0, i = str.length;
	while (i) hash = str.charCodeAt(--i) + (hash << 6) + (hash << 16) - hash;
	return hash >>> 0;
}

function loselose(str) {
	var hash = 0, i = str.length;
	while (i) hash += str.charCodeAt(--i);
	return hash >>> 0;
}

function fnv1a(str) {
	var hash = 0x811C9DC5, i = str.length;

	while (i)
	{
		hash ^= str.charCodeAt(--i);
		hash += (hash << 1) + (hash << 4) + (hash << 7) + (hash << 8) + (hash << 24);
	}
	return hash >>> 0;
}

function _murmur3_multiply(k,c)
{
	return ((((k & 0xffff) * c) + ((((k >>> 16) * c) & 0xffff) << 16))) & 0xffffffff;
}

function _murmur3_hash1(k)
{
	//k = k >>> 0;
	k = (k*0xcc9e2d51);
	k = (k << 15) | (k >>> 17);
	k = (k*0x1b873593);
	return k>>>0;
}

function murmur3(str) {
	var c1 = 0xcc9e2d51,
		c2 = 0x1b873593;

	var k,
		hash = 1, 
		remainder = str.length & 3, 
		c = str.length - remainder,
		i = 0;
 
	
	while (i<c)
	{
		k =
          ((str.charCodeAt(i) & 0xff)) |
          ((str.charCodeAt(++i) & 0xff) << 8) |
          ((str.charCodeAt(++i) & 0xff) << 16) |
          ((str.charCodeAt(++i) & 0xff) << 24);

		++i;
		
		console.log(i+" "+k)
		k = _murmur3_hash1(k);
		console.log(i+" "+k)
		hash ^= k;
		hash = (hash << 13) | (hash >>> 19) * 5 + 0xe6546b64;
	}
	k = 0;
	
    switch (remainder) {
		case 3: k ^= (str.charCodeAt(i+2) & 0xff) << 16;
		case 2: k ^= (str.charCodeAt(i+1) & 0xff) << 8;
		case 1: k ^= (str.charCodeAt(i) & 0xff);

		hash ^= _murmur3_hash1(k);
    }
	
	hash ^= str.length;
	hash ^= (hash >>> 16);
	hash = (((hash & 0xffff) * 0x85ebca6b) + ((((hash >>> 16) * 0x85ebca6b) & 0xffff) << 16)) & 0xffffffff;
    hash ^= hash >>> 13;
    hash = ((((hash & 0xffff) * 0xc2b2ae35) + ((((hash >>> 16) * 0xc2b2ae35) & 0xffff) << 16))) & 0xffffffff;
	hash ^= (hash >>> 16);

//hash = _murmur3_multiply(hash,0x85ebca6b);
	//hash ^= (hash >>> 13);
	//hash = _murmur3_multiply(hash,0xc2b2ae35);
	//hash ^= (hash >>> 16);

	return hash >>> 0;
}

function MurmurHashV3(key) {
    var remainder, bytes, h1, h1b, c1, c1b, c2, c2b, k1, i;

    remainder = key.length & 3; // key.length % 4
    bytes = key.length - remainder;
    h1 = 1;
    c1 = 0xcc9e2d51;
    c2 = 0x1b873593;
    i = 0;

    while (i < bytes) {
        k1 =
          ((key.charCodeAt(i) & 0xff)) |
          ((key.charCodeAt(++i) & 0xff) << 8) |
          ((key.charCodeAt(++i) & 0xff) << 16) |
          ((key.charCodeAt(++i) & 0xff) << 24);
      ++i;
		console.log(i+" "+k1)

      k1 = ((((k1 & 0xffff) * c1) + ((((k1 >>> 16) * c1) & 0xffff) << 16))) & 0xffffffff;
      k1 = (k1 << 15) | (k1 >>> 17);
      k1 = ((((k1 & 0xffff) * c2) + ((((k1 >>> 16) * c2) & 0xffff) << 16))) & 0xffffffff;
		console.log(i+" "+k1)

      h1 ^= k1;
      h1 = (h1 << 13) | (h1 >>> 19);
      h1b = ((((h1 & 0xffff) * 5) + ((((h1 >>> 16) * 5) & 0xffff) << 16))) & 0xffffffff;
      h1 = (((h1b & 0xffff) + 0x6b64) + ((((h1b >>> 16) + 0xe654) & 0xffff) << 16));
		
    }

    k1 = 0;

    switch (remainder) {
      case 3: k1 ^= (key.charCodeAt(i + 2) & 0xff) << 16;
      case 2: k1 ^= (key.charCodeAt(i + 1) & 0xff) << 8;
      case 1: k1 ^= (key.charCodeAt(i) & 0xff);

      k1 = (((k1 & 0xffff) * c1) + ((((k1 >>> 16) * c1) & 0xffff) << 16)) & 0xffffffff;
      k1 = (k1 << 15) | (k1 >>> 17);
      k1 = (((k1 & 0xffff) * c2) + ((((k1 >>> 16) * c2) & 0xffff) << 16)) & 0xffffffff;
      h1 ^= k1;
    }

    h1 ^= key.length;

    h1 ^= h1 >>> 16;
    h1 = (((h1 & 0xffff) * 0x85ebca6b) + ((((h1 >>> 16) * 0x85ebca6b) & 0xffff) << 16)) & 0xffffffff;
    h1 ^= h1 >>> 13;
    h1 = ((((h1 & 0xffff) * 0xc2b2ae35) + ((((h1 >>> 16) * 0xc2b2ae35) & 0xffff) << 16))) & 0xffffffff;
    h1 ^= h1 >>> 16;

    return h1 >>> 0;
  }
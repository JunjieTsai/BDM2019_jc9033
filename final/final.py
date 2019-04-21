from pyspark import SparkContext

def createIndex(shapefile):
    import rtree
    import geopandas as gpd
    zones = gpd.read_file(shapefile)
    index = rtree.Rtree()
    for idx,geometry in enumerate(zones.geometry):
        index.insert(idx, geometry.bounds)
    return (index, zones)

def findZone(p, index, zones):
    match = index.intersection((p.x, p.y, p.x, p.y))
    for idx in match:
        if zones.geometry[idx].contains(p):
            return idx
    return None

def FindDrugKeywords(tweet, drug_keywords):
    import string
    tweet = tweet.lower().encode('ascii', 'ignore').decode('ascii') # lowercase # remove emoji
    translator = str.maketrans(string.punctuation, ' '*len(string.punctuation)) # remove punctuations
    tweet_WOpunctuation = tweet.translate(translator) # remove punctuations
    tweet_refined = " " + ' '.join(tweet_WOpunctuation.split()) + " " # recombine
    return list(filter(lambda x: x in tweet_refined, drug_keywords))

def mapper1(pid, records):
    import csv
    import shapely.geometry as geom    

    drug_illegal = list(map(lambda x: " " + x.strip().lower() + " ", open("drug_illegal.txt", "r").readlines()))
    drug_sched2 = list(map(lambda x: " " + x.strip().lower() + " ", open("drug_sched2.txt", "r").readlines()))
    drug_keywords = drug_illegal + drug_sched2

    index, zones = createIndex('500cities_tracts.geojson')
    
    reader = csv.reader(records, delimiter='|')
    counts = {}
    
    for row in reader:        
        if FindDrugKeywords(row[5], drug_keywords):
            try:
                point = geom.Point(float(row[2]), float(row[1]))
                zone = findZone(point, index, zones)
            except:
                continue
            if zone:
                yield(zones['plctract10'][zone],1/zones['plctrpop10'][zone])

if __name__ == "__main__":
    sc = SparkContext()
    rdd = sc.textFile('hdfs:///tmp/bdm/tweets-100m.csv')
    counts = rdd.mapPartitionsWithIndex(mapper1)\
                .reduceByKey(lambda x,y: x+y)\
                .sortByKey()\
                .collect()
    print(counts[:10])          

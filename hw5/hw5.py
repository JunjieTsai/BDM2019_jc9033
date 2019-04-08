from pyspark import SparkContext

def createIndex(shapefile):
    import rtree
    import fiona.crs
    import geopandas as gpd
    zones = gpd.read_file(shapefile).to_crs(fiona.crs.from_epsg(2263))
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

def processTrips(pid, records):
    import csv
    import pyproj
    import shapely.geometry as geom
    
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)    
    index, zones = createIndex('neighborhoods.geojson')
    
    if pid==0:
        next(records)
    reader = csv.reader(records)
    counts = {}
    
    for row in reader:
        try:
            p = geom.Point(proj(float(row[5]), float(row[6])))
            d = geom.Point(proj(float(row[9]), float(row[10])))
            zone = findZone(p, index, zones)
            zone2 = findZone(d, index, zones)
            zone2 = zones['borough'][zone2]
        except:
            continue
        if zone and zone2:
            yield((zone,zone2),1)
            
def IndexToName(x):
    import geopandas as gpd
    zones = gpd.read_file('neighborhoods.geojson')
    return zones['neighborhood'][x]

if __name__ == "__main__":
    sc = SparkContext()
    
    rdd = sc.textFile('hdfs:///tmp/bdm/yellow_tripdata_2011-05.csv')
    counts = rdd.mapPartitionsWithIndex(processTrips)\
            .reduceByKey(lambda x,y: x+y) \
            .map(lambda x: (x[0][1], (x[1], x[0][0])))\
            .groupByKey()\
            .map(lambda x: (x[0], sorted(x[1], reverse=True)[:3]))\
            .map(lambda x: (x[0], [IndexToName(x[1][i][1]) for i in range(3)]))\
            .collect()
    
    for i in range(len(counts)):
        print(counts[i][1][0], ",",counts[i][1][1], ", and", counts[i][1][2], "are the top 3 origin neighborhood for trips ending up in", counts[i][0])

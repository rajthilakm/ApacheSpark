from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields=line.split(',')
    stationId=fields[0]
    entryType=fields[2]
    temperature=float(fields[3])* 0.1* 1.8 + 32.0
    return (stationId,entryType,temperature)
    
lines=sc.textFile("/Users/raj/Documents/Spark/1800.csv")
parsedLines = lines.map(parseLine)

minTemps=parsedLines.filter(lambda x:"TMIN" in x[1])
stationTemps=minTemps.map(lambda x:(x[0],x[2]))
minTemps=stationTemps.reduceByKey(lambda x,y :min(x,y))


results = minTemps.collect() 

for i in results:
    print i[0] +"\t{:.2f}F".format(i[1])

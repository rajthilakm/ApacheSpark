from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MaxWeather")
sc = SparkContext(conf = conf)

def parseLine(lines):
    lines=lines.split(',')
    stationID = lines[0]
    metricType=lines[2]
    temperature=float(lines[3])*0.1*1.8+32
    return(stationID,metricType,temperature)
    
#def parseLine(line):
#    fields=line.split(',')
#    stationId=fields[0]
#    entryType=fields[2]
#    temperature=float(fields[3])* 0.1* 1.8 + 32.0
#    return (stationId,entryType,temperature)
        
    
lines=sc.textFile("/Users/raj/Documents/Spark/1800.csv")   
parsedLines= lines.map(parseLine)

maxTemp=parsedLines.filter(lambda x : "TMAX" in x[1])
stationTemps=maxTemp.map(lambda x:(x[0],x[2]))
maxTemp=stationTemps.reduceByKey(lambda x,y: max(x,y))

results = maxTemp.collect()

for i in results:
    print i[0] +"\t{:.2f} deg F".format(i[1])
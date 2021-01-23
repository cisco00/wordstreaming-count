
var accesslogs = sc.textFile("/data/spark/project/access/access.log.2.gz")

accesslogs.take(10)

def containsIP(line:String):Boolean = return line matches "^([0-9\\.]+).*$"
    

var ipaccesslogs = accesslogs.filter(containsIP)

def extractIP(line:String):(String) = {
    // Here we are using the regular expression for matching the strings with a certain pattern.
    val pattern = "^([0-9\\.]+) .*$".r
    val pattern(ip:String) = line
    return (ip.toString)
}
var ips = ipaccesslogs.map(line => (extractIP(line),1));


var ipcounts = ips.reduceByKey((a,b) => (a+b))
var ipcountsOrdered = ipcounts.sortBy(f => f._2, false);
ipcountsOrdered.take(10)



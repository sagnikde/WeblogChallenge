from pyspark import SparkContext
import re
import os
from datetime import datetime

def getipanddate(rawdata):
    allparts = rawdata.split(' ')
    dateandtime = datetime.strptime(allparts[0], "%Y-%m-%dT%H:%M:%S.%fZ")
    ipandport = (allparts[2]).split(':')
    # I am throwing out the parts which are not really necessary for this assignment. Takes up precious RAM on my laptop.
    return (ipandport[0],dateandtime)#,rawdata)

def breakbysessionwindow(timewindow,ipandtimes):

    ip = ipandtimes[0]
    times = list(ipandtimes[1])
    # This is done as the Spark runtime will jumble up the times of visit
    times.sort()
    visitsfromip = len(times)

    # Stores the sessions
    sessions = []
    # Stores start of a session
    timestart = times[0]
    for eachvisit in range(visitsfromip-1):
        #If two consecutive visits exceed the time window then store the ip address , starting time and the length of the session.
        if((times[eachvisit+1]-times[eachvisit]).total_seconds()>timewindow):
            session = (ip,timestart,(times[eachvisit+1]-timestart).total_seconds())
            sessions.append(session)
            # Signals the start of a new session
            timestart = times[eachvisit+1]
            
    # As you have noticed the last item may be a new session but is not checked in the loop
    # so I take care of it here.
    if(timestart==times[visitsfromip-1]):
        session = (ip,timestart,0)
        sessions.append(session)
    return sessions
    
    
            
    
    



def findsessionstatistics():
    sc = SparkContext("local[*]", "SessionAnalysis")
    filepath = os.path.dirname(os.path.abspath(__file__))
    print(os.path.dirname(os.path.abspath(__file__)))
    rdd = sc.textFile("file:///"+filepath+"/../SessionData/2015_07_22_mktplace_shop_web_log_sample.log")
    sampleddata = rdd.sample(True,0.0001,10)
    parseddata = sampleddata.map(getipanddate)
    aggregatebyip = parseddata.groupByKey()
    ipsessions = aggregatebyip.flatMap(lambda x: breakbysessionwindow(15*60,x))
   
    output = ipsessions.collect()                             
    for sample in output:
        print(sample)
    


findsessionstatistics()

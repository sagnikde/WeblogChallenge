from pyspark import SparkContext
from pyspark.sql.types import Row
import shutil
import os
import sys
from datetime import datetime

# Parses the RAW data
def getipanddate(rawdata):
    allparts = rawdata.split(' ')
    dateandtime = datetime.strptime(allparts[0], "%Y-%m-%dT%H:%M:%S.%fZ")
    ipandport = (allparts[2]).split(':')
    # I am throwing out the parts which are not really necessary for this assignment. Takes up precious RAM on my laptop.
    return (ipandport[0],dateandtime)#,rawdata)


# Breaks up the visits according to the time window (it is in seconds)
def breakbysessionwindow(timewindow,ipandtimes):

    ip = ipandtimes[0]
    times = list(ipandtimes[1])
    # This is done as the Spark runtime may jumble up the times of visit during group operation.
    times.sort()
    visitsfromip = len(times)

    # Stores the sessions.
    sessions = []
    # Stores start of a session.
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

# Checks if a visit from an IP address is within a time frame
def insidesessioninterval(sessionstart,sessionlength,ipdatetime):
    timediff = (ipdatetime-sessionstart).total_seconds()
    return (timediff<=sessionlength and timediff>0)



# This function basically iterates through all the ip visits and retains only the unique ip visits            
def filteriphits(sessionandhits):

    session = sessionandhits[0]
    visits = sessionandhits[1]

    sessionip = session[0]
    sessionstart = session[1]
    sessionlength = session[2]
    # Python dictionary to maintain uniqueness
    ipdict = {}
    ipdict[session[0]]=""
    for ipvisit in visits:
        ipadd = ipvisit[0]
        ipdatetime = ipvisit[1]
         
        if(insidesessioninterval(sessionstart,sessionlength,ipdatetime)):
            ipdict[ipadd]=""

    # Find the number of unique keys (IP addresses)
    iphits = len(list(ipdict.keys()))
    return (sessionip,sessionstart,sessionlength,iphits)



# This is another method I tried but Spark cannot reference RDD inside a tranformation.
def filterredundantdata(ipvisits,session):
    sessionip = session[0]
    sessionstart = session[1]
    sessionlength = session[2]
    result = ipvisits.filter(lambda x : insidesessioninterval(sessionstart,sessionlength,x[1])).map(lambda x : x[0])
    uniqueresults = result.distinct().count()
    return (sessionip,sessionstart,sessionlength,uniqueresults)
    
    



def findsessionstatistics(sessionwindowminutes):
    sc = SparkContext("local[*]", "SessionAnalysis")
    filepath = os.path.dirname(os.path.abspath(__file__))

    rdd = sc.textFile("file:///"+filepath+"/../SessionData/2015_07_22_mktplace_shop_web_log_sample.log").cache()

    # Let's take a small sample and run the tests, the actual data set will take too much time for part 3 (unique visits)
    sampleddata = rdd.sample(True,0.002,5)
    #sampleddata = sc.parallelize(rdd.take(1000))


    #-------------------------------------------------------------------------------------#
    # Self explanatory, I will parse the data, group by ip address and find the sessions.
    parseddata = sampleddata.map(getipanddate)
    #parseddata = rdd.map(getipanddate)
    groupbyip = parseddata.groupByKey()
    ipsessions = groupbyip.flatMap(lambda x: breakbysessionwindow(sessionwindowminutes*60,x))


    #-------------------------------------------------------------------------------------#
    # Average time
    
    sessioncount = ipsessions.map(lambda x : (1,x[2]))
    totalsessionsandtimes = sessioncount.reduce(lambda a,b : (a[0]+b[0],a[1]+b[1]))
    averagetime = totalsessionsandtimes[1]/totalsessionsandtimes[0]



    #-------------------------------------------------------------------------------------#
    # Unique hits during an IP session
    # 
    # Create pairs for each session with the parsed data 
    sessionpair = ipsessions.cartesian(parseddata)   
    # Filter all the pairs where sessions from different ip addresses don't coincide and retain the ones which do.
    sessioniphits =  sessionpair.groupByKey().map(filteriphits)

    # The cartesian product operation takes up a lot of memory so I tried to filter the IP addresses inside a map operation.
    # This however is not supported by Spark (a transformation cannot reference an RDD) so I commented it out.
    
    #sessioniphits = ipsessions.map(lambda x : filterredundantdata(parseddata,x))
    
    # I personally believe that there must be a smarter method than taking the expensive Cartesian product and then throwing most of it away.
    # However at this moment I cannot find a better solution



    #-------------------------------------------------------------------------------------#
    # Most engaged users
    # I'll find the top ten engaged users

    sessiontimes = ipsessions.sortBy((lambda session : session[2]),False).map(lambda rawdata : (rawdata[0],str(rawdata[1]),rawdata[2])).take(10)
    
    

    #-------------------------------------------------------------------------------------#
    # Outputs
    formattedipsession = ipsessions.map(lambda rawdata : (rawdata[0],str(rawdata[1]),rawdata[2]))
    formattedipsession.saveAsTextFile("file:///"+filepath+"/../output/ipsessions/ipsessions.log")

    formattedsessioniphits = sessioniphits.map(lambda rawdata : (rawdata[0],str(rawdata[1]),rawdata[2]))
    formattedsessioniphits.saveAsTextFile("file:///"+filepath+"/../output/uniquevisits/uniquevisits.log")
    
    print("#-------------------------------------------------------------------------------------#")
    print("Average session time is : "+str(averagetime)+" seconds")
    output = sessiontimes
    print("#-------------------------------------------------------------------------------------#")
    print("Top 10 ip with long user sessions: ")
    for sample in output:
        print(sample)
    


findsessionstatistics(int(sys.argv[1]))

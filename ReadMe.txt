

I have done the analysis but it is using Spark and Python. I'll provide an explanation: 
When I got the email from the recruiter I did not have access to the server where I had set up the data analysis environment. So my initial idea was to use Pig for the analysis as I can just download the VM and start the assignment without fiddling with environment variables and installation procedures but limited ram on my PC posed to be a problem.
I found that I can install pyspark and Spark without needing a VM or anything and it ran without any hiccups.

I am using Spark 2.2 and Python version 3.6. Due to limited RAM on my laptop, I am only sampling a small amount of data from the input file.

SessionData folder should contain the input and the output folder will contain files related to the output.

The average session times and the longest session users are shown in standard output i.e. Terminal 

All the sessions and the unique visits from other ip addresses while a session is in progress is saved on files.

To run the python script with a session window of 15 mins the command is:

python AnalyzeSession.py 15


So IP addresses will not indicate unique users but the browser string will narrow down each user behind the IP addresses significantly. Another trick is to query for the installed fonts on the user's device along with screen size when one visits a particular website.







# slow-py
Python scripts to automate a CouchDB slow control database and interface with CSV made of lab instrument datapoints.

**First Step: Create Your Database**

- Take create_database.py
- Change the parameter in line 61 to the name of your new database
- Run create_database.py

You've done it!


**Second Step: Understand Input CSV File Specifications**

- each line of the CSV is one datapoint and each comma separated value is an attribute of the datapoint
- the first line of CSV must be the names of all the attributes being recorded (ex. timestamp, temperature, pressure)
- within the CSV, each comma separated value corresponds to the name specified in the first line
- attributes can be left blank, there just must be a space left for them in the csv (ex. 1,2,3,,4,5)
- one of the headers must be called "timestamp"
- upper limit of 4300 arguments (number of lines multiplied by number of values in the first line of the CSV)
- if the upper limit is respected, the script can handle any combination of number of lines and names of attributes
- no attribute name can be '_id'
- there must be consistency within each CSV file, but the next CSV file can have datapoints with completely different attributes as long as the "timestamp" specification is respected
- timestamp has been defined as Java regulated timestamp (ex. yyyy-mm-dd hh:mm:ss)

*Example CSV File*

![CSV_EXAMPLE](https://user-images.githubusercontent.com/47134315/86925837-585e7400-c0ff-11ea-827c-b58e111a6ee1.png)

*Example Resulting Document*

![Document](https://user-images.githubusercontent.com/47134315/86926411-05d18780-c100-11ea-9fa9-1f1bb069fda2.png)

**Third Step: Make the Code Your Own** 

The very last line slow_control.py (pictured below) has two string parameters. The first is the database name and the second is the name of the CSV file that data will be coming from. If you're using the original slow_control specifications, don't change it! But if you're storing data in a different database, change the first parameter to the name of the database you created in Step 1. Change the second parameter to the name of the CSV file you formatted in Step 2. 

![LastLine](https://user-images.githubusercontent.com/47134315/89677931-49b0db80-d8bc-11ea-95b0-ee00a25fbf2e.png)

You're ready to record data!



**Retrieving Data**

In the CouchDB database we have
- documents that hold data (what you just set up)
- documents that tell us where to find data

Every time that slow_control.py enters the contents of a CSV into the database, it also adds documents to make it easier to find it later.
For example, if you're entering temperature data, there will be a design document created that returns timestamps and temperatures. 
If you call the temperature design document with a startkey and endkey, you can look at the temperatures during that range of time.

This is how it's possible to graph the data or simply return all the values for data in this time range.

Code for both of these is COMING SOON (being finalized as we speak)


**Numpy Documentation of Code**

Instructions TBD



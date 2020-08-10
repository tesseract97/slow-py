# slow-py
Python scripts to automate a CouchDB slow control database and interface with CSV made of lab instrument datapoints.

**First Step: Create Your Database**

- Take create_database.py
- Change the parameter in line 61 to the name of your new database
- Run create_database.py

You've done it!


**Second Step: Understand Input CSV File Specifications**

- Store the CSV in the same directory as slow_control.py
- Write names of attributes in the first line of the CSV as headers. Every other line should contain values of attributes
- Don't exceed 4300 arguments in a single CSV (total number of values and headers)

- One of the attributes must be named "timestamp"
- The values for "timestamp must follow this format: yyyy-mm-dd hh:mm:ss
- Fields can be left blank, there just must be a space left for them in the csv (ex. 1,2,3,,4,5)
- CSVs with different headers/ number of columns/ number of rows can be entered into the same database as long as they all have "timestamp"


*Example CSV File*

![CSV_EXAMPLE](https://user-images.githubusercontent.com/47134315/86925837-585e7400-c0ff-11ea-827c-b58e111a6ee1.png)

*Example Resulting Document*

![Document](https://user-images.githubusercontent.com/47134315/86926411-05d18780-c100-11ea-9fa9-1f1bb069fda2.png)

**Third Step: Make the Code Your Own** 

- Take slow_control.py
- If you're sending data to slowcontroldb, make no changes
- If you're sending data to a different database, change the first parameter in line 382 to the name of your database
- Change the second parameter in line 382 to the name of your CSV file.
- Run slow_control.py


![LastLine](https://user-images.githubusercontent.com/47134315/89677931-49b0db80-d8bc-11ea-95b0-ee00a25fbf2e.png)

You're recording data!

**Numpy Documentation of Code**

For documentation of code:
- Open \_build
- Open html
- Open output of index.html in Browser

You're looking at it!


**Retrieving Data**

In the CouchDB database we have
- documents that hold data (what you just set up)
- documents that tell us where to find data

Every time that slow_control.py enters the contents of a CSV into the database, it also adds documents to make it easier to find it later.
For example, if you're entering temperature data, there will be a design document created that returns timestamps and temperatures. 
If you call the temperature design document with a startkey and endkey, you can look at the temperatures during that range of time.

This is how it's possible to graph the data or simply return all the values for data in this time range.

Code for both of these is COMING SOON (being finalized as we speak)






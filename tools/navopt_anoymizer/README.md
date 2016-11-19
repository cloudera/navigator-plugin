Navigator Optimizer Anonymizer
========

**IMPORTANT:** This is a BETA version of the Anonymizer tool, which is still undergoing testing before its official release. Data redaction is not 100% so do not use Anonymizer in production settings or on production data. Currently, Anonymizer is offered for testing purposes only.

Anonymizer is a command-line tool designed to be used in conjunction with [Cloudera Navigator Optimizer] (http://www.cloudera.com/products/cloudera-navigator-optimizer.html), which is a service you can use to profile and analyze the query text in SQL workloads. Use Anonymizer to protect sensitive information in the SQL workloads you analyze by using AES-128 encryption. Run Anonymizer on SQL files (.csv or semicolon-separated .sql files) to:

* Mask all literals in the SQL queries
* Encrypt table and column names

## Prerequisites

* Make sure that you have a recent version of the Java JDK installed. Anonymizer  has been tested with JDK 1.7 and 1.8. After installing the JDK open a terminal window and run the following command to verify it is installed correctly:
<pre><code>java -version</code></pre>
If this command returns information about the installed Java JDK, you have installed it correctly. For example, the following information is returned when the JDK 1.8 is installed:
<pre><code>java version "1.8.0\_101"
Java(TM) SE Runtime Environment (build 1.8.0_101-b13)
Java HotSpot(TM) 64-Bit Server VM (build 25.101-b13, mixed mode)</code></pre>

* Download the <code>navopt-workload-anonymizer-0.1-SNAPSHOT.jar</code> from [https://github.com/cloudera/navigator-sdk/blob/master/tools/navopt_anoymizer/navopt-workload-anonymizer-0.1-SNAPSHOT.jar] (https://github.com/cloudera/navigator-sdk/blob/master/tools/navopt_anoymizer/navopt-workload-anonymizer-0.1-SNAPSHOT.jar) 

## Using Anonymizer

### To anonymize .sql workload files:

1. Open a terminal window, navigate to the directory where the Anonymizer JAR file is located, and run the following command:
<pre><code>java -jar navopt-workload-anonymizer-0.1-SNAPSHOT.jar -i \<path\_to\_workload_file\> -sql</code></pre>
Where <code>-jar</code> specifies the path to the Anonymizer JAR file, <code>-i</code> specifies the path to the .sql file that contains the workload, and <code>-sql</code> specifies that the input workload file is a .sql file (not a .csv file).
2. Respond to the tool prompts for database vendor and password. If the password you enter is not strong enough, the tool writes the password requirements to the terminal window.
3. The tool processes the .sql file and when finished writes the locations of the anonymized file, the key file that contains credentials generated from the password you supplied, and the error output file to the terminal window.
 
By default, Anonymizer names the .sql output files as follows:

| Input Filename 	| Anonymized Output Filename 	| Key File Name       	| Error File Name 	|
|--------------------------	|-----------------------------	|---------------------	|-----------------	|
| **xyz.sql**                  	| anon**xyz.sql**                 	| anon**xyz.sql**.passkey 	| anon**xyz.sql**.err 	|


### To de-anonymize .sql workload files:

1. Open a terminal window, navigate to the directory where the Anonymizer JAR file is located, and run the following command:
<pre><code>java -jar navopt-workload-anonymizer-0.1-SNAPSHOT.jar -i \<path\_to\_anonymized\_workload\_file\> -sql -k \<path\_to\_passkey\_file\> -d</code></pre> Where <code>-k</code> specifies the path to the <code>.passkey</code> file created when you originally anonymized the .sql workload.

2. Respond to the tool prompts for database vendor and password that you originally entered when the file was anonymized.
3. The tool processes the anonymized file and when finished writes the locations of the de-anonymized file, the key file, and the error output file to the terminal window.

### To encrypt .csv workload files:
Before you can anonymize .csv workload files, identify which column contains the query text. Start counting from the left at 1. For example, in the following .csv file, the query text is in column 3:
<pre><code>"SQL\_ID","CPU\_TIME","SQL\_FULLTEXT","USER","APP","REPORT"
43958,100,"select emps.id from emps where emps.name = 'Joe' group by emps.mgr, emps.id;","Alice Johnson","Finance","Manager Report"
235,900,"select emps.name from emps where emps.num = 007 group by emps.state, emps.name;","Bill Huntington","HR","Employee Locations"
abc1,40,"select Part.partkey, Part.name, Part.type from Part where Part.yyprice > 2095;","Carrol Robinson","Operations","Inventory"
f58,440,"select Part.partkey, Part.name, Part.mfgr FROM Part WHERE Part.name LIKE '%red';","David Stanley","Operations","Inventory"
345,67000,"select count(\*) as loans from account a where a.account\_state\_id in (5,9);","Anne McCaffrey","Finance","Liability Report"
2341,999,"select orders.key, orders.id from orders where orders.price < 9999;","David Stanley","Sales","Revenue Report"
5ef3,678,"select mgr.name from mgr where mgr.reports > 10 group by mgr.state;","David Stanley","HR","Manager Report"
7676,34,"select vp.salary from vp where vp.grade = 'Alpha' group by vp.state;","Alice Johnson","Finance","Revenue Report"
346,67053,"select count(*) as loans from account a where a.account\_state\_id in (5,9);","Alice Johnson","Finance","Liability Report"</code></pre>

**Note:** The Anonymizer uses a .csv parser that complies with [RFC-4180] (https://www.ietf.org/rfc/rfc4180.txt).


1. Open a terminal window, navigate to the directory where the Anonymizer JAR file is located, and run the following command:
<pre><code>java -jar navopt-workload-anonymizer-0.1-SNAPSHOT.jar -i \<path\_to\_workload_file\> -q \<column\_number\></code></pre>
Where <code>-jar</code> specifies the path to the Anonymizer JAR file, <code>-i</code> specifies the path to the .csv file that contains the workload, and <code>-q</code> specifies the column that contains the query text in the .csv file. If your .csv file has more than one header row, use the <code>-h</code> parameter to specify how many header rows the file contains. When you do not specify the <code>-h</code> parameter, Anonymizer assumes the default values of 1.
2. Respond to the tool prompts for database vendor and password. If the password you enter is not strong enough, the tool writes the password requirements to the terminal window.
3. The tool processes the .csv file and when finished writes the locations of the anonymized file, the key file that contains credentials generated from the password you supplied, and the error output file to the terminal window.
 
By default, Anonymizer names the .csv output files as follows:

| Input Filename 	| Anonymized Output Filename 	| Key File Name       	| Error File Name 	|
|--------------------------	|-----------------------------	|---------------------	|-----------------	|
| **xyz.csv**                  	| anon**xyz.csv**                 	| anon**xyz.csv**.passkey 	| anon**xyz.csv**.err 	|


### To de-anonymize .csv workload files:
1. Open a terminal window, navigate to the directory where the Anonymizer JAR file is located, and run the following command:
<pre><code>java -jar navopt-workload-anonymizer-0.1-SNAPSHOT.jar -i \<path\_to\_anonymized\_workload\_file\> -q \<column\_number\> -k \<path\_to\_passkey\_file\> -d</code></pre> Where <code>-k</code> specifies the path to the <code>.passkey</code> file created when you originally anonymized the .csv workload.

2. Respond to the tool prompts for database vendor and password that you originally entered when the file was anonymized.
3. The tool processes the anonymized file and when finished writes the locations of the de-anonymized file, the key file, and the error output file to the terminal window.


## Anonymization Scenarios

The Anonymizer can perform two types of anonymization:

  * [Literal stripping](#literalstrip)
  * [Schema obfuscation](#schemaobfus)
 
<a name="literalstrip" />
### Literal stripping
</a> 
Security standards such as PCI-DSS (Payment Card Industry Data Security Standard) and laws such as HIPAA mandate that PII (Personally Identifiable Information) and PHI (Protected Health Information) are treated with the highest level of confidentiality. PII/PHI is defined as information that can unambiguously identify an individual. In the case of HIPAA, for example, this could be names, addresses, or social security numbers. This kind of information infrequently appears in SQL queries. The second highest level of confidentiality protects what is called a limited data set. This is information such as birth date, geographical region, or hospital admission dates, that could be used to identify an individual within a population. This information can sometimes be contained within the literals (also known as _constant values_ or _fixed data values_) of SQL queries. The Anonymizer tool, which is run on the client side, irreversibly strips literals from SQL queries before sending them to the Navigator Optimizer cloud service. Navigator Optimizer cloud service only sees these "de-identified" queries.

**Important:** After Anonymizer has masked out the literal values, the values cannot be unmasked. It is an irreversible process.

#### Example use case
A health-care organization wants to make sure all PHI data is scrubbed from the SQL queries before being sent to the Navigator Optimizer cloud service. To accomplish this, they use Anonymizer to strip all literals from their workloads before uploading them to Navigator Optimizer. Anonymizer gives peace of mind that all sensitive data is removed from workload files before uploading. 

<a name="schemaobfus" />
### Schema obfuscation
</a>
Sometimes database schemas contain sensitive information that is treated as IP (intellectual property). For example, database schemas can reveal how a company organizes its data, or column and table names can reveal what data a company collects. Before queries are sent to the Navigator Optimizer cloud service, Anonymizer masks all column names and table names in SQL queries on the client side and secures them with a secret password only known to the user. The anonymized SQL queries retain their structure (SQL keywords are in plaintext) but all table names and column names are encrypted using AES-128 encryption standards. Navigator Optimizer provides workload analysis based on query patterns only. All private or sensitive information is encrypted and protected by a password. If the user forgets their password or loses their key, no recovery of the data is possible. There is no “password-reset” feature. Schema change recommendations are computed on obfuscated table and column names, and the user views these recommendations by supplying a password, which is never sent to Navigator Optimizer cloud service. Decryption happens securely in the browser on the client. 

#### Example use case:
A company uses a secret proprietary algorithm that uses public Facebook profiles to make decisions about its customers. A column name titled “facebook-id” would reveal that the company is looking at Facebook information. Anonymizer ensures that this information remains confidential by encrypting all column and table names on the client side before uploading queries to Navigator Optimizer cloud service.  


## How Anonymizer Is Used with Navigator Optimizer

1. A passcode is supplied to Anonymizer when it is used to encrypt SQL queries.  Anonymizer encrypts all sensitive information on the client. There is no data transmission over a network. The encrypted SQL queries retain their structure (SQL keywords are in plaintext) but all table names and column names are encrypted and literals are entirely dropped. Anonymizer generates a .passkey file that is necessary to recover encrypted schema information. 
2. The encrypted workload file is uploaded to Navigator Optimizer cloud service where it is analyzed. Analysis is possible because Navigator Optimizer only uses the structure of queries to make recommendations, not the data itself. Literals are not sent to the cloud service because they are stripped out by Anonymizer on the client before uploading. Consequently, literals can not be recovered after Anonymizer is run on a workload file unless you choose the <code>-l</code> or the <code>--skip\_mask\_literals</code> option when you anonymize the file. This option prevents Anonymizer from stripping out literals from the SQL queries. 
3. After Navigator Optimizer analyzes the workload and provides recommendations, in the client-side browser, you can decrypt the data to see column names and table names. No decryption is possible without the passcode and <code>.passkey</code> file, and they are never sent to the Navigator Optimizer service. All decryption occurs in the Javascript on the client-side browser. Click on the eye icon in the top right corner of the Navigator Optimizer application window to supply the decryption password and <code>.passkey</code> file.


## CLI Reference

The following command-line options are available:

| Option                                   | Required? | Description                                                                                                                                                                      |
|------------------------------------------|-----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| <code>-a</code>, <code>--anonymize</code>                          |           | Encrypts the queries in the file. If both the <code>-a</code> and the <code>-d</code> options are absent, Anonymizer defaults the operation to encryption.                                                 |
| <code>-d</code>, <code>--deanonymize</code>                        |     X     | Decrypts the queries in the file. Must be specified to decrypt a file.                                                                                                           |
| <code>-h</code>, <code>--header\_row \<number\_of\_header\_rows\></code> |           | Applies to .csv files only. Specifies the number of header rows to be ignored from processing. When this option is absent, Anonymizer defaults to ignoring only one header row.  |
| <code>-i</code>, <code>--input \<path\_to\_input\_file\></code>         |     X     | Specifies the path to the input file that contains the queries.                                                                                                                  |
| <code>-k</code>, <code>--key \<path\_to\_.passkey\_file\></code>         |     X     | Specifies the path to the <code>.passkey</code> file. Must be specified to decrypt a file.                                                                                                    |
| <code>-l</code>, <code>--skip\_mask\_literals</code>                 |           | Prevents Anonymizer from stripping out the literals from queries.                                                                                                                |
| <code>-o</code>, <code>--output \<path\_to\_output\_file\></code>       |           | Specifies the path and name for the output file when Anonymizer encrypts or decrypts a workload file.                                                                            |
| <code>-q</code>, <code>--query\_col\_number \<column\_position\></code> |     X     | Applies to .csv files only. Specifies the position of the column that contains queries in the .csv file. Column positions are counted from the left, starting with 1.            |
| <code>-sql</code>                                    |           | Specifies if the input file is an SQL file that must be split on semi-colons (;).                                                                                                |
| <code>-t</code>, <code>--skip\_anonymize\_table\_columns</code>       |           | Prevents Anonymizer from encrypting the table and column names in the queries.                                                                                                   |


## Limitations

1. Hive and Impala queries are currently not supported. 
2. Cannot anonymize alias when using the older aliasing syntax from Teradata. For example, Anonymizer cannot anonymize <code>aliasName</code> in the following query structure:
   <pre><code>SELECT sample_column (NAMED aliasName) from t1
   </code></pre>

## FAQs

#### How secure is Anonymizer encryption?
Anonymizer uses AES-128, one of the most secure encryption algorithms available today. As of today, no practicable attack against AES exists. Therefore, AES remains the preferred encryption standard for governments, banks, and high security systems around the world.

#### What safeguards are present to ensure the integrity of the Navigator Optimizer cloud service?
Navigator Optimizer is working towards industry-standard SOC-2, HIPAA HITECH, ISO27001 certification. Navigator Optimizer's processes and practices are periodically audited by independent auditors, and all code check-ins go through rigorous code and security reviews. Strict change-management processes are adhered to.

#### What safeguards are present to ensure the security of the Navigator Optimizer cloud environment?
Navigator Optimizer uses security best practices in line with SOC-2, HIPAA HITECH, and ISO27001 standards. For example, all data in databases are encrypted, and the cloud environment is routinely scanned for vulnerabilities. 


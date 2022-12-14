<h1 align="left">Solution to 'exa-data-eng-assessment'</h1>

<h2 align="left">Project Description</h2>
<p align="left">
    This is a solution to the EMIS-group DE test found here: https://github.com/emisgroup/exa-data-eng-assessment.
    The solution uses Python to create a pipeline for unpacking json files and transferring them into a tabular format within a MySQL database.   Both of these are held within Docker containers.  
</p>



<h2 align="left">Quick Start</h2>
<h3 align="left">Installation</h3>
<p>
<ol>
  <li>Simply clone this Repo to your machine</li>
  <li>Open a terminal and navigate to its location</li>
  <li>Start by running the command below (this takes a few minutes):</li>
      
      docker-compose up --build
</ol>
    
</p>


<h3 align="left">Running The Pipeline</h3>
<p align="left">
    Open a terminal within the python_pipeline container
    and run:
    
    python3 main.py
</p>

<h3 align="left">Exploring the Database</h3>
<ul>
    <li>Open a terminal within the mysql container</li>
    <li>Enter:</li> 
        
        mysql -u root -p
</ul>
<ul>
    <li>Type in the password:</li>
    
        password
</ul>
<ul>
    <li>Any MySQL Commands now work, for example:
        
        USE emis_test_db;
        SHOW TABLES;
        select * from Patient;
</ul>

<h3 align="left">Expected Output</h3>

![alt text](https://github.com/rlamprell/emis_test/blob/main/images/emis-test-expected-output.PNG?raw=true)



<h2 align="left">Architecture</h2>


![alt text](https://github.com/rlamprell/emis_test/blob/main/images/emis-test-docker-diagram.PNG?raw=true)
![alt text](https://github.com/rlamprell/emis_test/blob/main/images/emis-test-main-dataflow.PNG?raw=true)



<h2 align="left">Next Steps & Alternative Approaches</h2>
<ul>
    <li>Use Postgres instead of MySQL.  The latter doesn't properly support UTF-8 which has lead to hotfix at the 11th hour.</li>
    <li>Remove the dependency to feed the explode_nested_arrays() method an arbitrary list of names.</li>
    <li>Expand the db_connection methods and make them more general.</li>
    <li>Resolve aborted connection bug when transferring tables - doesn't affect the data exported, might be a docker config issue.</li>
    <li>Some data-modeling around the tables would be good.  Some of the field names are bit abstract, long and not very informative.  Normalisation of the tables using Kimball or something similar might be beneficial to both the aforementioned issue and the overall readbility and flexibility of the data.</li> 
    <li>Further testing and a more robust test library would make changes easier.  Write more unit and integration tests.</li>
    <li>Change of approach on the file extraction.  If the number of files is too large or there's a particuarlly large file in the batch there's a potential for the pipeline to fallover due to memory issues.  Perhaps it should be more peacemeal with a checkpointing system.  .</li>
    <li>Using a Kubernetes Cluster may make solution more flexible.</li>
    <li>A streaming solution such as Kafka may prove better in production - depends on the data delivery.</li>
    <li>Using a modern-data-stack approach might also be better - certainly more flexible and scalable than this solution.  We could use GCP composer to host an Airflow instance that orchestrates our dag - saved in GCS.  This would also give us access to BigQuery which we could use as a datalake to store the unstructured data, which could then be manipulated via a transformation engine such as dbt or Dataform.  And if we needed to access anything external we could attach a Cloud Nat to our VPC.</li>
</ul>


<h2 align="left">Notes</h2>
<ul>
    <li>Built on Windows 10.</li>
    <li>Will run not on M1 macs, as there is no  mysql:5.7 image for armx64.</li> 
</ul>


<h3 align="left">Languages and Tools:</h3>
<p align="left"> 
    <a href="https://www.tensorflow.org" target="_blank" rel="noreferrer"> 
        <img src="https://skillicons.dev/icons?i=python,mysql,docker,git" />
    </a> 
</p>

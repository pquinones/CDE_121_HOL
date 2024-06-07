### Job Observability & Data Governance



#### Lab 5: Monitoring Jobs with Cloudera Observability

Cloudera Observability is CDP’s single pane of glass observability solution, continually discovering and collecting performance telemetry across data, applications, and infrastructure components running in CDP deployments on private and public clouds. With advanced, intelligent analytics and correlations, it provides insights and recommendations to address tricky issues, optimize costs, and improve performance.

##### Monitor Jobs in CDP Observability

Navigate to CDP Observability




##### CDE Job Run Observability

Navigate to the Job Runs page in your Virtual Cluster and notice a new Job Run is being logged automatically for you.

![alt text](../../img/part1-cdesparkjob-4.png)

Once the run completes, open the run details by clicking on the Run ID integer in the Job Runs page.

![alt text](../../img/part1-cdesparkjob-5.png)

Open the logs tab and validate output from the Job Run in the Driver -> Stdout tab.

![alt text](../../img/part1-cdesparkjob-6.png)



##### Create CDE Python Resource

Navigate to the Resources tab and create a Python Resource. Make sure to select the Virtual Cluster assigned to you if you are creating a Resource from the CDE Home Page, and to name the Python Resource after your username e.g. "fraud-prevention-py-user100" if you are "user100".

Upload the "requirements.txt" file located in the "cde_spark_jobs" folder. This can take up to a few minutes.

Please familiarize yourself with the contents of the "requirements.txt" file and notice that it contains a few Python libraries such as Pandas and PyArrow.

Then, move on to the next section even while the environment build is still in progress.

![alt text](../../img/part1-cdepythonresource-1.png)

![alt text](../../img/part1-cdepythonresource-2.png)

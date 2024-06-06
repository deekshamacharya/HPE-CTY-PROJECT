# To install YCSB (Yahoo! Cloud Serving Benchmark) for ArangoDB 3 on Ubuntu, you can follow these steps:

**1.Install YCSB:**<br>
        >First, you need to download YCSB from the official GitHub repository: YCSB GitHub.<br>
        >You can either clone the repository using Git or download the zip file and extract it.<br>
        _command:_<br>
        git clone https://github.com/brianfrankcooper/YCSB.git
        
**2.Set Up ArangoDB Connection:**<br>
        >After downloading YCSB, navigate to the arangodb directory within the YCSB folder.<br>
        >Edit the arangodb.properties file to configure the connection settings. You can set the following properties:<br>
    arangodb.url=http://localhost:8529<br>
    arangodb.user=root<br>
    arangodb.password=xxx<br>

**3.Install Java:**<br>
       >Ensure you have Java installed on your Ubuntu machine. You can install it using the following command:<br>
    sudo apt update
    sudo apt install default-jdk

**4.Compile YCSB:**<br>
       >After configuring the connection settings, go back to the root directory of the YCSB folder.
    Compile YCSB using Maven:<br>
    
    mvn clean package

**5.Run YCSB Workloads:**<br>
      >Once YCSB is compiled successfully, you can run the workloads against your ArangoDB instance.<br>
      For example, to run a workload named "workloada", you can use the following command:
      
python2 ./bin/ycsb load arangodb -s -P workloads/workloada -p arangodb.ip=localhost -p arangodb.port=8529<br>
      For example, to run a workload named "workloada", you can use the following command:
    
python2 ./bin/ycsb run arangodb -s -P workloads/workloada -p arangodb.ip=localhost -p arangodb.port=8529

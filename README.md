# Cluster
Utility for monitoring/submission/removal of Condor and Slurm jobs.

## Install
```
git clone git@github.com:LARG/cluster.git
cd cluster
pip install --user .
```

## Job Submission
To submit a job, simply prepend cluster:
```
cluster ./my_executable -my_arg
```
This will start the job and return its PID. Cluster will auto-detect if the local machine is a Condor or Slurm submit node, and if so, will submit the job to Condor/Slurm. Otherwise it will run the job locally. Cluster supports several options for specifying job requirements and logging. To see the full list of options run `cluster` without any arguments.

## Job Monitoring
```cluster-ls``` lists your active jobs.  
```cluster-ls -a``` lists active jobs of all users.  

## Job Removal
```cluster-rm PID``` Removes job with specified PID.  
```clister-rm -a``` Removes all of your jobs.
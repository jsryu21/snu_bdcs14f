snu_bdcs14f
===========

## Objectives
The algorithm assigned to me is ALS-CF and the goal is Linear Regression.

## The status of program
* Even with a late submission, I could not have done with my work.
* Data Loading / Group Communication had some restrictions made things more difficult. I will mention the issue below.
* Build process goes with no failure, but it crashes during the computation.

### What I have done :
* Load the data with Data loading API. The available format is Yahoo! music dataset.
* Establish Group Communication channel
* Build the control flow and communication

### What I have not done :
* Distribute the dataset (all nodes have the whole dataset)
* Update model matrix (U and M) for each iteration
* Optimize Communcation overhead
* Optimize the data structure

## How to run the program
The important command line options are
* input : The path where input data files exist
* num_feat : Number of features to guess`
* lambda : The coefficient term for regularization
* split : number of partitions to split the dataset
* max_iter : maximum iteration

For example, assuming that you have your dataset in `/tmp/dataset.txt`, you can run the program with this command

```shell
java -cp target/bdcs-assignment-1.0-SNAPSHOT-shaded.jar edu.snu.cms.bdcs.assignment.ALS -input “/tmp/dataset.txt” -split 4 -max_iter 1000
```


## API Issues
* The current GroupCommunication does not have Scatter/Gather. In ALS, two operations are critical to distribute/collect the dataset and feature matrices. I tried to use a trick to assign task id and distribute the workload based on the total number and ids of the tasks, but I could not make the timeline.
* DataLoading API loads the data with sequential order. It makes sense in the other algorithms to distribute data set because they are independent. But in ALS, the data set should be clustered to compute the feature matrix, because they use entire rate for each user(or item) for each iteration.

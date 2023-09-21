# Exercise 6: fault-tolerance

Go to the checkpoint location and analyze the content:
```
cd /tmp/wfc/workshop/part02/checkpoint/

less commits/*
less offsets/*
```

Questions:

1. From your understanding, what is the logic behind the fault-tolerance? How the engine knows where to start processing if the job stops.
2. How to implement a reprocessing scenario from an arbitrary point in the past? What are your ideas?



<details>
<summary>Answers - Question 1</summary>
There is a versioned pair of `commits` and `offsets`. Whenever given micro-batch completes, it writes corresponding versioned files in these 2 directories. 
When you restart the job, Apache Spark verifies the last written version for both directories and:

* if both have the same number, the job starts in the next offset (N)
* if offset is higher than the commit, the job starts from the next to last offset (N-1)
* if commit is higher than offset, well, it can't happen unless you alter the checkpoint location on purpose
</details>

<details>
<summary>Answers - Question 2</summary>
Assuming the checkpoint data is still there - 10 most recent micro-batches are kept by default - you can:

* alter the last offset and rollback it to any point in the past
* explicitly set the starting timestamp in the job; but it requires changing the checkpoint location which in case of a stateful processing can be problematic
* remove the checkpoint metadata files you want to reprocess
 
**Before making any operation on the checkpoint location, create a backup**
</details>

# 🥳 Congrats!

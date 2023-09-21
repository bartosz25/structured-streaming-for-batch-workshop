# Exercise 2: Spark UI

Keep the previous pipeline running. Open the Spark UI at [http://localhost:4040](http://localhost:4040).

1. Where to find the information about the batch duration and data processing rate?
2. Stop the job and modify the trigger to `.trigger(Trigger.ProcessingTime("1 minute"))` (Scala) or `.trigger(processingTime='1 minute')` (Python)
3. Restart the job and open the Spark UI in the new tab or window. Compare the batch duration and data processing rate.
4. Do you see any difference?


<details>
<summary>Answers - Question 1</summary>
Under _Structured Streaming_ where the Streaming query statistics are displayed.
</details>

<details>
<summary>Answers - Question 4</summary>
Several aspects here:

* more rows are processed at once, the query waits 1 minute and takes more data at once
* it doesn't impact the execution time, though; our sink is not data-bounded; we print data which is stored in memory
  so there is no I/O impact because of the increased data volume; typically, it won't be the case if you use a I/O-bounded data sink
* there is less jobs executed in the Jobs section; it might be easier to follow
</details>

# Well done! 
⏭️ [start the next exercises](/exercise3.md)

# MonotonicFunction
This app is calculating whether the input users amount as the function of time is monotonically increasing.

You should first [https://medium.com/tinghaochen/how-to-install-pyspark-locally-94501eefe421](install pyspark).

In order to run the app you should just run the main file with the following parameters:
- The input directory:
    This directory should include all the json files you wish to read.
- The output directory:
    Where the results will be written to.
    
The output is whether each website is monotonically increasing.

If I had more time:
- I would try to think of a way to better implement the sorting. In my implementation the sorting is for all the events together. Because the events should be sorted only in the resulotion of each website then there might be a way to improve it.
- I would use more computing power than the local machine. In my implementation I use all the cores of the local machine (`local[*]`), where I could use Spark's cluster mode instead. 
- I might use caching to improve the calculation, using the RDD cache/persist method.

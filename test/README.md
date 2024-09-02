# Run Experiments
* You can run experiments with the example workloads in the ./loads directory. 
  For example: `ycsb_test test/loads/ycsb-load.dat test/loads/ycsb-run-a.dat 3 20 30`
* For other datasets, you can use the Yahoo! Cloud Serving Benchmark (YCSB) to generate 
  standard workloads. And then, use our `ycsb_build` to build workloads for `ycsb_test`
* The first parameter of `ycsb_build` is the workload generated by YCSB, the second parameter
  is your specified dataset, where each line is a unique key.
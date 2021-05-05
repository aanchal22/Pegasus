Commands for test run:

1) cd into the directory "peg"
2) run command ""
3) view accuracy by running the command "hdfs dfs -cat /user/ak8257/Pegasus/accuracy/*.csv"

Commands for generating a small dataset. (WARNING: It will overwrite the previous data generated for our test run. Please ensure to run this only after running a sample test run to reproduce results.)

1) cd into directory "data-generator"
2) run "sh generate_clean_data"
3) run the test run commands again.

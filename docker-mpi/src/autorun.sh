#!/bin/bash
echo "Start"
echo "Start test" > logfile.txt
for i in {3..24..1}
do
  echo "Run with $i processors" | tee -a logfile.txt
  { mpirun -np $i --allow-run-as-root ./Count.out ./InputFile 2>&1; } | tee -a logfile.txt
done
echo "End"

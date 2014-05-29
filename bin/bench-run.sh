#!/bin/bash

if [ $# -lt 1 ]; then
    echo "Must specify at least one yml config file for a scenario to test"
    echo "Example: $0 sample-config.yml"
    exit 1
else
    for file in $@ ; do
        if ! [[ -f $file ]]; then
            echo "Input scenaro $file does not exist."
            exit 1
        fi
    done
fi

# Usage: benchmark scenario1 scenario2 scenario3 ...
#
# where scenarioN is the name of a yml config file for Socialite.
#
# This is what it will do:
# 1. Create a new output directory for all of the data
# 2. Cycle through each of the scenarios, running each of the benchmarks
# 3. Process the output files and generate a plot for each benchmark
#    There will be 1 series in each benchmark for each of the scnearios passed in

benchmark="run"

#  --totalusers TOTALUSERS --concurrentusers CONCURRENTUSERS --threads THREADS --seconds SECONDS [file]

#
# Create the output directory. It'll be in ./output/<new_directory_name>

outdir=./output/$(date +"%b-%d-%y-%H:%M")
echo "Sending output to ${outdir}"

# Run the benchmark for each scenario passed in and process the results

scenarios=""
for scenario in $@ ; do
    scenario_base=`basename $scenario`
    scenario_name=${scenario_base%%.*}
    scenario_outdir=$outdir/$benchmark/$scenario_name
    echo "Starting scenario $scenario_name"
    mkdir -p $scenario_outdir
    java -jar ./target/socialite-0.0.1-SNAPSHOT.jar $benchmark --totalusers 100000 --concurrentusers --out $scenario_outdir $scenario

    # Format the output in a form that gnuplot likes
    cut -d , -f 2 $scenario_outdir/count.csv > $scenario_outdir/count.dat
    cut -d , -f 11 $scenario_outdir/latency.csv > $scenario_outdir/latency.dat
    paste $scenario_outdir/count.dat $scenario_outdir/latency.dat > $outdir/$benchmark/$scenario_name.dat
    scenarios="$scenarios $scenario_name"
done

for out in $outputs ; do
    echo "Generated output for $out";
done

cat << EOF | gnuplot > $outdir/$benchmark/chart.png
set terminal png
set autoscale
unset log
unset label
set xtic auto
set ytic auto
set title "Send message latency by follower count"
set xlabel "# of followers"
set ylabel "99% latency (ms)"
cd '$outdir/$benchmark'
filenames = "${scenarios}"
plot for [file in filenames] file.".dat" using 1:2 with line title sprintf("%s",file)
EOF

echo "Output graph into $outdir/$benchmark/chart.png"


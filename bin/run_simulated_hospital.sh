#!/bin/bash

######################################################
#
# A simple helper script to run simhospital docker image, which creates the HL7v2 files.
# These files will then be uploaded to S3 bucket. So simulate a stream, the simhospital
# is start/stop iterative cycle, so that the sample can be uploaded to S3.
#
# To run the script, the following pre-requisite should be made
# - docker installed
# - aws cli configured
# - s3 bucket
#
# Note: This script is just for development testing only. 
#
######################################################

# Seconds to sleep between iterations
SLEEP_INTERVAL=180

# Maximum number of iterations
ITERATION_MAX=100

# The S3 bucket to upload the data files
S3_BUCKET="$TARGET_S3_BUCKET"

#######
## Execution
#######

# Create the data direction
data_dir="$(pwd)/target/data"
echo "Data Dir : ${data_dir}"
mkdir -p ${data_dir}

# Run the simulator
run_sim () {
    msg_fl_name=$1
    echo "Run simulation output to ${data_dir}/${msg_fl_name} ..."
    docker run --name simhospi --detach \
        --rm -it -p 8000:8000 \
        -v ${data_dir}:/out \
        eu.gcr.io/simhospital-images/simhospital:latest health/simulator \
        -output=file -output_file=/out/${msg_fl_name} \
        -pathways_per_hour=5000

    echo "${data_dir}/${msg_fl_name}"
}

# Iteratively loop on a start/stop of simhospital
COUNTER=0
while true ; do
    let COUNTER++
    if [ "$COUNTER" -ge "$ITERATION_MAX"  ]; then
        break
    fi

    echo "============== Iteration : ${COUNTER} =================="
    message_file_suffix=$(date '+%Y%m%d%H%M%S')
    msg_fl_name="hl7v2_msg_${message_file_suffix}.txt"
    hl7_fl_path="${data_dir}/${msg_fl_name}"

    run_sim ${msg_fl_name}
    sleep ${SLEEP_INTERVAL}
    docker stop simhospi

    echo "Uploading ${hl7_fl_path} to s3 ${S3_BUCKET} ..."
    aws s3 cp "${hl7_fl_path}" "${S3_BUCKET}" 
done

echo "Finished!!"
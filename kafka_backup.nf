#!/usr/bin/env nextflow
nextflow.enable.dsl=2
import groovy.json.JsonSlurper

process kafka_filtool {
    label 'filtool'
    container "${params.fold_singularity_image}"
    errorStrategy 'ignore'

    beforeScript """
    bash ${baseDir}/beforeScript_Processing.sh ${baseDir} ${process_uuid} ${submit_time} ${execution_order} ${params.jsonconfig} ${params.kafka.server} ${params.kafka.schema_registry_url} \${NXF_SCRATCH} ${params.kafka.message_create} ${params.kafka.producer_script}
    """
  afterScript """
    # Determine the current directory and the exit status file path
    exit_code=\$?
    echo "exit code is \$exit_code"
    work_dir=\$(pwd)
    echo "Work directory in afterScript is \$work_dir"
    echo "CHDIR is \${NXF_CHDIR}"
    exit_code_file="\${NXF_CHDIR}/.exitcode"
    echo "Exit code file is \$exit_code_file"
    echo "Files in the directory are:" 
    \$(ls -v \${NXF_CHDIR}/.*)

    # Ensure the exit status file exists and is readable
    if [[ -f "\$exit_code_file" ]]; then
        exit_status=\$(cat "\$exit_code_file")
    else
        echo "Warning: Exit status file not found."
        exit_status="Unknown1"
    fi

    # Log the completion time and exit status
    end_time=\$(date -u +"%Y-%m-%dT%H:%M:%S")
    echo "Process ended at \$end_time"
    echo "Process ended at \$end_time in directory \${work_dir} with status \$exit_status"
"""

    
    input:
    val(process_uuid)
    val(submit_time)
    val execution_order
    

    script:
    """
    #!/bin/bash
    trap 'echo "Script failed at line \$LINENO with status \$?"; exit 1' ERR

    
    raw_data=\$(ls -v ${params.raw_data})
    # Get the first file from the input_data string!!
    first_file=\$(echo \${raw_data} | awk '{print \$1}')
    # Extract the file extension from the first file
    file_extension="\$(basename "\${first_file}" | sed 's/.*\\.//')"

    if [[ \${file_extension} == "sf" ]]; then
        

        echo "filtool -psrfits --scloffs -t ${params.filtool.threads} --nbits ${params.filtool.nbits} --mean ${params.filtool.mean} --std ${params.filtool.std} --td ${params.filtool.time_decimation_factor} --fd ${params.filtool.freq_decimation_factor} --telescope ${params.filtool.telescope} -z ${params.filtool.rfi_filter} --cont -o ${params.target}_${params.utc}_${params.beam} -s ${params.target} -f \${raw_data}"
        exit 2
    else

        echo "filtool -t ${params.filtool.threads} --nbits ${params.filtool.nbits} --mean ${params.filtool.mean} --std ${params.filtool.std} --td ${params.filtool.time_decimation_factor} --fd ${params.filtool.freq_decimation_factor} --telescope ${params.filtool.telescope} -z ${params.filtool.rfi_filter} --cont -o ${params.target}_${params.utc}_${params.beam} -s ${params.target} -f \${raw_data}"
    fi
    sleep 60
    
    """
    
}





process filtool {
    label 'filtool'
    container "${params.fold_singularity_image}"
    //scratch "${params.tmp_dir}"

    input:
    tuple path(fil_file), val(POINTING), val(BAND), val(UTC_OBS), val(BEAM)
    val rfi_filter
    val threads
    val telescope

    output:
    tuple path("${POINTING}_${BAND}_${UTC_OBS}_${BEAM}_01.fil"), val(POINTING), val(BAND), val(UTC_OBS), val(BEAM)

    script:
    """
    filtool -t ${threads} --telescope ${telescope} -z ${rfi_filter} --cont -o ${POINTING}_${BAND}_${UTC_OBS}_${BEAM} -f ${fil_file} -s ${POINTING} 
    """
}

// process rfifind{
//     label 'short'
//     container "${params.presto_singularity_image}"
//     publishDir "RESULTS/${POINTING}/${UTC_OBS}/${BAND}/${BEAM}/02_RFIFIND/", pattern: "*.rfifind*", mode: 'copy'

//     input:
//     tuple path(fil_file), val(POINTING), val(BAND), val(UTC_OBS), val(BEAM)
//     val threads, val time, val timesig, val freqsig, val intfrac, val chanfrac

//     output:
//     tuple path("${POINTING}_${BAND}_${UTC_OBS}_${BEAM}_rfifind.mask"), val(POINTING), val(BAND), val(UTC_OBS), val(BEAM)

//     script:
//     """
//     rfifind -nooffsets -noscales -time ${time} -timesig ${timesig} -freqsig ${freqsig} -intfrac ${intfrac} -chanfrac ${chanfrac} -o ${POINTING}_${BAND}_${UTC_OBS}_${BEAM}_rfifind -ncpus ${threads} ${fil_file}
//     """


// }



process peasoup {
    label 'peasoup'
    container "${params.search_singularity_image}"
    // This will only publish the XML files
    publishDir "RESULTS/${POINTING}/${UTC_OBS}/${BAND}/${BEAM}/03_SEARCH/", pattern: "**/*.xml", mode: 'copy'



    input:
    tuple path(fil_file), val(POINTING), val(BAND), val(UTC_OBS), val(BEAM)
    path(dm_file) 
    val(fft_size)
    val(total_cands_limit)
    val(min_snr)
    val(acc_start)
    val(acc_end)
    val(ram_limit_gb)
    val(nh)
    val(ngpus)


    output:
    tuple path("**/*.xml"), path(fil_file), val(POINTING), val(BAND), val(UTC_OBS), val(BEAM)

    script:
    """
    peasoup -i ${fil_file} --fft_size ${fft_size} --limit ${total_cands_limit} -m ${min_snr} --acc_start ${acc_start} --acc_end ${acc_end} --dm_file ${dm_file} --ram_limit_gb ${ram_limit_gb} -n ${nh} -t ${ngpus} 
    """
}


process pulsarx {
    label 'pulsarx'
    container "${params.fold_singularity_image}"
    publishDir "RESULTS/${POINTING}/${UTC_OBS}/${BAND}/${BEAM}/04_FOLDING/", pattern: "*.ar", mode: 'copy'
    publishDir "RESULTS/${POINTING}/${UTC_OBS}/${BAND}/${BEAM}/04_FOLDING/", pattern: "*.png", mode: 'copy'




    input:
    tuple path(peasoup_xml_out), path(input_file), val(POINTING), val(BAND), val(UTC_OBS), val(BEAM)
    path pulsarx_fold_template

    output:
    path("*.ar")
    path("*.png")

    script:
    """
    python3 ${params.fold_script} -i ${peasoup_xml_out} -t pulsarx -p ${pulsarx_fold_template} -b ${BEAM} -threads 4
    """

}

process prepfold {
    label 'prepfold'
    container "${params.presto_singularity_image}"
    publishDir "RESULTS/${POINTING}/${UTC_OBS}/${BAND}/${BEAM}/05_FOLDING/", pattern: '*.pfd*', mode: 'copy'

    input:
    tuple path(peasoup_xml_out), path(input_file), val(POINTING), val(BAND), val(UTC_OBS), val(BEAM)

    output:
    path("*.pfd*")

    script:
    """
    python ${params.fold_script} -i ${peasoup_xml_out} -t presto -b ${BEAM} -pthreads 4
    """



}

process readfile_parser {
    container "${params.presto_singularity_image}"
    input:
    tuple path(fil_file), val(POINTING), val(BAND), val(UTC_OBS), val(BEAM)

    output:
    stdout 

    script:
    """
    bash ${params.readfile_parser} -f ${fil_file}
    """
}
def fieldNames = ['Telescope', 'Pointing', 'START_UTC', 'MJD', 'RA_STR_J2000', 'DEC_STR_J2000', 'RA_DEG_J2000', 'DEC_DEG_J2000', 'TSAMP', 'CENTRAL_FREQ', 'LOW_CHAN_MHz', 'HIGH_CHAN_MHz', 'CHAN_BW_MHz', 'NUM_CHAN', 'BW_MHz', 'NBIT', 'NSAMPLES', 'TOBS']

process create_and_send_kafka_message_processing {
    container "${params.kafka_singularity_image}"
    executor = 'local'

    input:
    val process_name
    val execution_order
    val sql_table
    val kafka_server
    val schema_registry
    val avro_schema

    output:
    env(process_id)
    env(submit_time)
    path("submit_time.txt")

    shell:
    """
    #!/bin/bash
    set -e  # Exit immediately if a command exits with a non-zero status.
    set -o pipefail  # The return value of a pipeline is the status of the last command to exit with a non-zero status.
    
    
    # Run the script to generate processing message
    python "${params.kafka.message_create}" -p "${process_name}" -e "${execution_order}" -j "${params.jsonconfig}" -t "${sql_table}" --process_status "SUBMITTED"

    # Produce the message to the Kafka topic 'processing'
    python "${params.kafka.producer_script}" --bootstrap_servers "${kafka_server}" --schema_registry_url "${schema_registry}" --schema_file "${baseDir}/avro_schema/Processing.avsc" --topic "processing" --value_file "processing_message.csv"

    # Produce the message to the Kafka topic 'processing_dp_inputs'
    python "${params.kafka.producer_script}" --bootstrap_servers "${kafka_server}" --schema_registry_url "${schema_registry}" --schema_file "${baseDir}/avro_schema/ProcessingDpInputs.avsc" --topic "processing_dp_inputs" --value_file "process_dp_inputs_message.csv"

    # Extract the process_id from the CSV
    process_id=\$(awk -F',' 'NR==2 {print \$1}' processing_message.csv)
    submit_time=\$(awk -F',' 'NR==2 {print \$5}' processing_message.csv)
    echo \${process_id}
    echo \${submit_time} > submit_time.txt
    """

}
 

workflow kafka_workflow{
    
    def execution_order = 1
  
   (process_uuid, submit_time, submit_file) = create_and_send_kafka_message_processing("filtool", execution_order, "processing", params.kafka.server, params.kafka.schema_registry_url, params.kafka.schema_file)
    // process_uuid.view()
    // submit_time.view()
    submit_file.view()
    //println "Process ID: ${process_uuid}"
   // println "Submit Time: ${submit_time}"
    kafka_filtool(process_uuid, submit_time, execution_order)
    
    //execution_order += 1
    //uuid_string.view()
    
}




 workflow { 

    if (params.use_kafka == 1) {
        println("Using kafka workflow")
        kafka_workflow()
    }
    else{
        println "Using normal workflow"
        // filtool_output = filtool(fil_files_channel, "zdot", "12", params.telescope)
        // peasoup_results = peasoup(filtool_output, params.dm_file, params.fft_size, params.total_cands_limit, params.min_snr, params.acc_start, params.acc_end, params.ram_limit_gb, params.nh, params.ngpus)
        // pulsarx_output = pulsarx(peasoup_results, params.pulsarx_fold_template)
        // prepfold_output = prepfold(peasoup_results)

    }


 }


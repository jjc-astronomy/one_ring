#!/usr/bin/env nextflow
nextflow.enable.dsl=2
import groovy.json.JsonSlurper
import java.util.UUID

// Define a function to generate UUID
def generateUUID() {
    UUID.randomUUID().toString()
}




process kafka_filtool {
    label 'filtool'
    container "${params.fold_singularity_image}"
    errorStrategy 'ignore'
    
    input:
    val(process_uuid) //For the database
    val(input_dp_id) //List
    val(input_dp) //List
    val(pipeline_id)
    val(hardware_id)
    val(filtool_id) //Argument ID
    val(execution_order)
    val(program_name) //For the database
    val(output_filename)
    val(output_file_type_id) //For the database
    val(beam_id)
    val(hardware_id)

    output:
    tuple path(output_filename), env(output_dir), env(tsamp), env(tobs), env(nsamples), env(freq_start_mhz), env(freq_end_mhz), env(tstart), env(tstart_utc), env(foff), env(nchans), env(nbits)

    // path(output_filename)
    // env(output_dir)
    // env(tsamp)
    // env(tobs)
    // env(nsamples)
    // env(freq_start_mhz)
    // env(freq_end_mhz)
    // env(tstart)
    // env(tstart_utc) 

    script:

    """
    #!/bin/bash
    raw_data=\$(ls -v ${input_dp})
    # Get the first file from the input_data string!!
    first_file=\$(echo \${raw_data} | awk '{print \$1}')
    # Extract the file extension from the first file
    file_extension="\$(basename "\${first_file}" | sed 's/.*\\.//')"

    if [[ \${file_extension} == "sf" ]]; then

        filtool --psrfits --scloffs -t ${params.filtool.threads} --nbits ${params.filtool.nbits} --mean ${params.filtool.mean} --std ${params.filtool.std} --td ${params.filtool.time_decimation_factor} --fd ${params.filtool.freq_decimation_factor} --telescope ${params.filtool.telescope} -z ${params.filtool.rfi_filter} --cont -o ${params.target}_${params.utc}_${params.beam} -s ${params.target} -f \${raw_data}
       
    else

        filtool -t ${params.filtool.threads} --nbits ${params.filtool.nbits} --mean ${params.filtool.mean} --std ${params.filtool.std} --td ${params.filtool.time_decimation_factor} --fd ${params.filtool.freq_decimation_factor} --telescope ${params.filtool.telescope} -z ${params.filtool.rfi_filter} --cont -o ${params.target}_${params.utc}_${params.beam} -s ${params.target} -f \${raw_data}
    fi
    output_dir=\$(pwd)
    # Get the metadata from the output file and store it in the environment variables
    while IFS='=' read -r key value
        do
            declare "\$key=\$value"
        done < <(python3 ${params.get_metadata} -f ${output_filename})

    #Temporary hard fix because filtool inverts the frequency band incorrectly.
    filedit -f \${freq_end_mhz} -F \${foff} ${output_filename}
    """
    
}


process kafka_peasoup {
    label 'peasoup'
    container "${params.search_singularity_image}"
    // This will only publish the XML files
    publishDir "RESULTS/${POINTING}/${UTC_OBS}/${BAND}/${BEAM}/03_SEARCH/", pattern: "**/*.xml", mode: 'copy'



    input:
    tuple val(process_uuid), val(input_dp_id), val(input_dp) //List
    val(pipeline_id)
    val(hardware_id)
    val(peasoup_id) //Argument ID
    val(execution_order)
    val(program_name) //For the database
    val(output_filename)
    val(output_file_type_id) //For the database
    val(beam_id)
    val(hardware_id)
    path(dm_file)
    


    output:
    path(output_filename) 
    env(fft_size)

    script:
    """
    output_dir=\$(dirname ${output_filename})
    mkdir -p \${output_dir}
    fft_size=${params.peasoup.fft_size}
    peasoup -i ${input_dp} --fft_size \${fft_size} --limit ${params.peasoup.total_cands_limit} -m ${params.peasoup.min_snr} --acc_start ${params.peasoup.acc_start} --acc_end ${params.peasoup.acc_end} --dm_file ${dm_file} --ram_limit_gb ${params.peasoup.ram_limit_gb} -n ${params.peasoup.nh} -t ${params.peasoup.ngpus} -o \${output_dir}
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

    
    
    
   
workflow {
    // Define the parameters
    def execution_order = 1
    def filtool_id, filtool_output_file_id, peasoup_id, peasoup_output_file_id

    println "Pipeline ID: ${params.pipeline_id}"
    println "Hardware ID: ${params.hardware_id}"
    println "Beam ID: ${params.beam_id}"
    println "Programs: ${params.programs.program_name}"
    // Iterate over the list of programs
    params.programs.each { program ->
        if (program.program_name == 'filtool') {
            filtool_id = program.program_id
            filtool_output_file_id = program.output_file_id
        } else if (program.program_name == 'peasoup') {
            peasoup_id = program.program_id
            peasoup_output_file_id = program.output_file_id
        }
    }
    println "Filtool ID: $filtool_id"
    println "Filtool Output File ID: $filtool_output_file_id"
    println "Peasoup ID: $peasoup_id"
    println "Peasoup Output File ID: $peasoup_output_file_id"
   

}
  


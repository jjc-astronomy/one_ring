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
    val(output_dp)
    val(beam_id)

    output:
    tuple path(output_dp), env(output_dp_id), env(tsamp), env(tobs), env(nsamples), env(freq_start_mhz), env(freq_end_mhz), env(tstart), env(tstart_utc), env(foff), env(nchans), env(nbits)



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
    #output_dir=\$(pwd)
    # Get the metadata from the output file and store it in the environment variables
    while IFS='=' read -r key value
        do
            declare "\$key=\$value"
        done < <(python3 ${params.get_metadata} -f ${output_dp})

    #Temporary hard fix because filtool inverts the frequency band incorrectly.
    if [[ \${foff} -ge 0 ]]; then
        # Make foff negative 
        \${foff}=-\${foff}
    fi

    filedit -f \${freq_end_mhz} -F \${foff} ${output_dp}

    # Generate a UUID for the output file
    output_dp_id=\$(uuidgen)
    """
    
}


process kafka_peasoup {
    label 'peasoup'
    container "${params.search_singularity_image}"
    // This will only publish the XML files
    publishDir "RESULTS/${params.target}/${params.utc}/${params.beam}/01_SEARCH/", pattern: "**/*.xml", mode: 'copy'



    input:
    tuple val(process_uuid), val(input_dp_id), val(input_dp) //List
    val(pipeline_id)
    val(hardware_id)
    val(peasoup_id) //Argument ID
    val(execution_order)
    val(program_name) //For the database
    val(output_dp)
    val(beam_id)
    val(hardware_id)
    path(dm_file)
    


    output:
    tuple path(output_dp), env(output_dp_id), env(fft_size)

    script:
    """
    output_dir=\$(dirname ${output_dp})
    mkdir -p \${output_dir}
    fft_size=${params.peasoup.fft_size}
    peasoup -i ${input_dp} --fft_size \${fft_size} --limit ${params.peasoup.total_cands_limit} -m ${params.peasoup.min_snr} --acc_start ${params.peasoup.acc_start} --acc_end ${params.peasoup.acc_end} --dm_file ${dm_file} --ram_limit_gb ${params.peasoup.ram_limit_gb} -n ${params.peasoup.nh} -t ${params.peasoup.ngpus} -o \${output_dir}
    output_dp_id=\$(uuidgen)
    """
}


process kafka_pulsarx {
    label 'pulsarx'
    container "${params.fold_singularity_image}"
    publishDir "RESULTS/${params.target}/${params.utc}/${params.beam}/02_FOLDING/", pattern: "*.{ar,png}", mode: 'copy'

    input:
    tuple val(process_uuid), val(input_dp_id), val(input_dp) // List
    val(pipeline_id)
    val(hardware_id)
    val(beam_id)
    path(pulsarx_fold_template)
    val(pulsarx_id)
    val(execution_order)
    val(program_name)

    output:
    //path(*.ar, *.png) are kept for downstream processes, and env (output_archives, outputplots) for the database.
    tuple path("*.ar"), path("*.png"), env(output_dp), env(output_dp_id)
    //tuple path("*.ar"), path("*.png"), env(output_archives), env(ar_uuids), env(output_plots), env(png_uuids)

    script:
    """
    #!/bin/bash
    python3 ${params.folding.fold_script} -i ${input_dp} -t pulsarx -p ${pulsarx_fold_template} -b ${params.beam} -threads ${params.pulsarx.threads} -utc ${params.utc} -rfi ${params.pulsarx.rfi_filter} -clfd ${params.pulsarx.clfd_q_value}
    
     # Collect output files and generate UUIDs
    output_dp=\$(ls -v *.ar *.png | xargs -I {} realpath {})
    output_dp_id=""
    for file in \$output_dp; do
        uuid=\$(uuidgen)
        output_dp_id="\$output_dp_id \$uuid"
    done
    

    """
}


process kafka_prepfold {
    label 'prepfold'
    container "${params.presto_singularity_image}"
    publishDir "RESULTS/${params.target}/${params.utc}/${params.beam}/03_FOLDING/", pattern: "*.pfd*", mode: 'copy'

    input:
    tuple val(process_uuid), val(input_dp_id), val(input_dp) // List
    val(pipeline_id)
    val(hardware_id)
    val(beam_id)
    val(prepfold_id)
    val(execution_order)
    val(program_name)

    output:
    tuple path("*.pfd"), path("*.bestprof"), path("*.ps"), path("*.png"), env(output_dp), env(output_dp_id)

    script:
    """
    #!/bin/bash
    python ${params.folding.fold_script} -i ${input_dp} -t presto -b ${params.beam} -pthreads ${params.prepfold.ncpus}
    
    # Collect output files and generate UUIDs
    output_dp=\$(ls -v *.pfd *.bestprof *.ps *.png | xargs -I {} realpath {})
    output_dp_id=""
    for file in \$output_dp; do
        uuid=\$(uuidgen)
        output_dp_id="\$output_dp_id \$uuid"
    done
    
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
    def filtool_id, filtool_output_file_id, peasoup_id, peasoup_output_file_id, pulsarx_id, pulsarx_output_file_id, prepfold_id, prepfold_output_file_id
    
    //Extract all the database IDs for each program from the JSON file
    params.programs.each { program ->
        if (program.program_name == 'filtool') {
            filtool_id = program.program_id
            filtool_input_dp_ids = program.data_products.dp_id.join(' ')
            filtool_input_dp_files = program.data_products.filename.join(' ')

        } else if (program.program_name == 'peasoup') {
            peasoup_id = program.program_id
        }
          else if (program.program_name == 'pulsarx'){
            pulsarx_id = program.program_id
          }
          else if (program.program_name == 'prepfold'){
            prepfold_id = program.program_id
          }
    }
   
    
    filtool_output_filename = "${params.target}_${params.utc}_${params.beam}_01.fil"
    filtool_process_uuid = generateUUID()

    filtool_channel = kafka_filtool(filtool_process_uuid, filtool_input_dp_ids, filtool_input_dp_files, params.pipeline_id, params.hardware_id, filtool_id, execution_order, "filtool", filtool_output_filename, params.beam_id)
    
    filtool_output = filtool_channel.map { item ->
        def (filtool_cleaned_file, filtool_cleaned_file_uuid, tsamp, tobs, nsamples, startMHz, endMHz, tstart, tstartUTC, foff, nchans, nbits) = item
        //filtool_cleaned_file_uuid = generateUUID()
        peasoup_process_uuid = generateUUID()
        return tuple(peasoup_process_uuid, filtool_cleaned_file_uuid, filtool_cleaned_file)
    }
    //Start Peasoup
    execution_order += 1
    def peasoup_output_filename = "peasoup_results/overview.xml"
    //Search all DM range files with the rfi cleaned observation.
    
    peasoup_channel = kafka_peasoup(filtool_output, params.pipeline_id, params.hardware_id, peasoup_id, execution_order, "peasoup", peasoup_output_filename, params.beam_id, params.hardware_id, params.peasoup.dm_file)

    peasoup_results = peasoup_channel.multiMap { item ->
        def (peasoup_output_file, peasoup_output_file_uuid, fft_size) = item
        //def peasoup_output_file_uuid = generateUUID()
        def pulsarx_process_uuid = generateUUID()
        def prepfold_process_uuid = generateUUID()

        pulsarx: tuple(pulsarx_process_uuid, peasoup_output_file_uuid, peasoup_output_file) // Tuple for PulsarX
        prepfold: tuple(prepfold_process_uuid, peasoup_output_file_uuid, peasoup_output_file) // Tuple for Prepfold
    }
    //Start PulsarX
    execution_order += 1
    pulsarx_output = kafka_pulsarx(peasoup_results.pulsarx, params.pipeline_id, params.hardware_id, params.beam_id, params.pulsarx.fold_template, pulsarx_id, execution_order, "pulsarx")
    //Start Prepfold
    prepfold_output = kafka_prepfold(peasoup_results.prepfold, params.pipeline_id, params.hardware_id, params.beam_id, prepfold_id, execution_order, "prepfold")




}
  


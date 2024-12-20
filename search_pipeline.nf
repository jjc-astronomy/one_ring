nextflow.enable.dsl=2

// Define a function to generate UUID
def generateUUID() {
    UUID.randomUUID().toString()
}

workflow {
    // Extract 'filtool' object from pipeline config JSON
    def filtool_prog = params.programs.find { it.program_name == 'filtool' }

    // Build a channel that emits one map per data product:
    // Each emitted value is a single Groovy map with all fields needed for the process.
    filtool_input = Channel.from( filtool_prog.data_products )
        .map { dp ->
            return [
                process_uuid : generateUUID(),
                program_args : filtool_prog.arguments,
                program_id   : filtool_prog.program_id,
                target_name  : dp.target_name,
                hardware_id  : dp.hardware_id,
                pointing_id  : dp.pointing_id,
                beam_name    : dp.beam_name,
                beam_id      : dp.beam_id,
                coherent_dm  : dp.coherent_dm,
                filenames    : dp.filenames,
                dp_id        : dp.dp_id.join(' '),
                processing_dp_id : dp.processing_dp_id.join(' '),
                output_dp        : "${dp.target_name}_${dp.beam_name}_01.fil" 

            ]
        }
        
    
    // Run the filtool process on each emitted value in parallel
    filtool(filtool_input)
}

process filtool {
    label 'filtool'
    container "${params.apptainer_images.pulsarx}"
    errorStrategy 'ignore'

    input:
    tuple val(process_uuid), val(program_args), val(filtool_id), val(target_name), val(hardware_id), val(pointing_id), val(beam_name), val(beam_id), val(coherent_dm), val(input_dp), val(input_dp_id), val(process_input_dp_id), val(output_dp)

    output:
    tuple path(output_dp), env(output_dp_id), env(tsamp), env(tobs), env(nsamples), env(freq_start_mhz), env(freq_end_mhz), env(tstart), env(tstart_utc), env(foff), env(nchans), env(nbits)


    script:
    """
    #!/bin/bash
    
    filtool -t ${program_args.threads} --nbits ${program_args.nbits} --mean ${program_args.mean} --std ${program_args.std} --td ${program_args.time_decimation_factor} --fd ${program_args.freq_decimation_factor} --telescope ${program_args.telescope} -z ${program_args.rfi_filter} -o ${target_name}_${beam_name} -s ${target_name} -f ${input_dp} ${program_args.extra_args}

    # Get the metadata from the output file and store it in the environment variables
    while IFS='=' read -r key value
        do
            declare "\$key=\$value"
        done < <(python ${program_args.get_metadata} -f ${output_dp})

    # Generate a UUID for the output file
    output_dp_id=\$(uuidgen)

    """
}

nextflow.enable.dsl=2

process filtool {
    label 'filtool'
    container "${params.apptainer_images.pulsarx}"
    errorStrategy 'ignore'

    input:
    tuple val(program_name),
          val(pipeline_id), 
          val(hardware_id), 
          val(program_args), 
          val(filtool_id), 
          val(target_name), 
          val(utc_start), 
          val(pointing_id), 
          val(beam_name), 
          val(beam_id), 
          val(filstr), 
          val(coherent_dm), 
          val(input_dp), 
          val(input_dp_id), 
          val(output_dp)

    output:
    tuple path(output_dp), env(output_dp_id), env(publish_dir), val(beam_name), val(beam_id), val(coherent_dm), env(tsamp), env(tobs), env(nsamples), env(freq_start_mhz), env(freq_end_mhz), env(tstart), env(tstart_utc), env(nchans), env(nbits), val(filstr)

    // Only publish if local_write=1
    publishDir "${params.publish_dir_prefix}/03_FILTOOLED/${target_name}/${utc_start}/${filstr}/", 
           mode: 'copy', enabled: params.filtool.local_write == 1 ? true : false

    script:

    """
    #!/bin/bash
    set -euo pipefail

    # Reusable function to check status of last command
    check_status() {
        local exit_code=\$?
        if [ \$exit_code -ne 0 ]; then
            >&2 echo "ERROR: Last command failed with exit code \$exit_code"
            exit 1
        fi
        return 0
    }

    # Check if the metadata extractor file exists
    if [[ ! -f "${program_args.get_metadata}" ]]; then
    echo "Error: File '${program_args.get_metadata}' does not exist." >&2
    exit 1
    fi

    workdir=\$(pwd)
    echo "Working directory: \${workdir}"
    publish_dir="${params.publish_dir_prefix}/03_FILTOOLED/${target_name}/${utc_start}/${filstr}/"
    mkdir -p \${publish_dir}

    if [ ${params.filtool.local_write} -eq 0 ]; then
        cd \${publish_dir}
    fi

    tmp_output_dp="${output_dp}"
    filtool_output_string=\${tmp_output_dp%_01.fil}
    filtool -t ${program_args.threads} --nbits ${program_args.nbits} --mean ${program_args.mean} --std ${program_args.std} --td ${program_args.time_decimation_factor} --fd ${program_args.freq_decimation_factor} --telescope ${program_args.telescope} -z ${program_args.rfi_filter} -o \$filtool_output_string -s ${target_name} -f ${input_dp} ${program_args.extra_args} 
    #Check if filtool command succeeded
    check_status
    # Get the metadata from the output file and store it in the environment variables
    while IFS='=' read -r key value
        do
            declare "\$key=\$value"
        done < <(python ${program_args.get_metadata} -f ${output_dp})
    
    # Check if the metadata extraction succeeded
    check_status
    # Generate a UUID for the output file
    output_dp_id=\$(uuidgen)
    check_status
    if [ ${params.filtool.local_write} -eq 0 ]; then
        # Create symlink to the output file
        cd \${workdir}
        ln -s \${publish_dir}/${output_dp} ${output_dp}
        check_status
    fi
    """
}

process peasoup {
    label 'peasoup'
    container params.apptainer_images.peasoup

    input:
    tuple val(program_name),
          val(pipeline_id), 
          val(hardware_id),
          val(program_args), 
          val(peasoup_id), 
          val(input_dp), 
          val(input_dp_id), 
          val(filstr),
          val(utc_start),
          val(beam_name),
          val(beam_id), 
          val(coherent_dm), 
          val(peasoup_orig_xml),
          val(output_dp), 
          val(target_name)
    
    output:
    tuple path(output_dp), env(output_dp_id), env(publish_dir), env(segment_pepoch), val(beam_name), val(coherent_dm), env(cfg_name), val(utc_start), val(target_name), val(beam_id), val(filstr)
    publishDir "${params.publish_dir_prefix}/05_SEARCH/${target_name}/${utc_start}/${filstr}/${params.pipeline_name}/${program_args.cfg_name}/", mode: 'copy'

    script:
    """
    #!/bin/bash
    set -euo pipefail

    # Reusable function to check status of last command
    check_status() {
        local exit_code=\$?
        if [ \$exit_code -ne 0 ]; then
            >&2 echo "ERROR: Last command failed with exit code \$exit_code"
            exit 1
        fi
        return 0
    }
    publish_dir="${params.publish_dir_prefix}/05_SEARCH/${target_name}/${utc_start}/${filstr}/${params.pipeline_name}/${program_args.cfg_name}/"


    # Write out dm_file.txt
    python -c "import numpy as np; dm_trials = np.arange(${program_args.dm_start}, ${program_args.dm_end}, ${program_args.dm_step}); f=open('dm_file.txt','w'); f.writelines(f'{dm}\\n' for dm in dm_trials); f.close()"
    check_status
    # Accumulate optional arguments
    optional_args=""
    if [ "${program_args.acc_start}" != "null" ]; then
        optional_args="\${optional_args} --acc_start ${program_args.acc_start}"
    fi
    if [ "${program_args.acc_end}" != "null" ]; then
        optional_args="\${optional_args} --acc_end ${program_args.acc_end}"
    fi
    if [ "${program_args.acc_pulse_width}" != "null" ]; then
        optional_args="\${optional_args} --acc_pulse_width ${program_args.acc_pulse_width}"
    fi
    if [ "${program_args.accel_tol}" != "null" ]; then
        optional_args="\${optional_args} --acc_tol ${program_args.accel_tol}"
    fi
    if [ "${program_args.dm_pulse_width}" != "null" ]; then
        optional_args="\${optional_args} --dm_pulse_width ${program_args.dm_pulse_width}"
    fi
    if [ "${program_args.min_snr}" != "null" ]; then
        optional_args="\${optional_args} -m ${program_args.min_snr}"
    fi
    if [ "${program_args.ram_limit_gb}" != "null" ]; then
        optional_args="\${optional_args} --ram_limit_gb ${program_args.ram_limit_gb}"
    fi
    if [ "${program_args.nharmonics}" != "null" ]; then
        optional_args="\${optional_args} --nharmonics ${program_args.nharmonics}"
    fi
    if [ "${program_args.ngpus}" != "null" ]; then
        optional_args="\${optional_args} -t ${program_args.ngpus}"
    fi
    if [ "${program_args.total_cands_limit}" != "null" ]; then
        optional_args="\${optional_args} --limit ${program_args.total_cands_limit}"
    fi
    if [ "${program_args.fft_size}" != "null" ]; then
        optional_args="\${optional_args} --fft_size ${program_args.fft_size}"
    fi
    if [ "${program_args.start_sample}" != "null" ]; then
        optional_args="\${optional_args} --start_sample ${program_args.start_sample}"
    fi
    if [ "${program_args.min_freq}" != "null" ]; then
        optional_args="\${optional_args} --min_freq ${program_args.min_freq}"
    fi
    if [ "${program_args.max_freq}" != "null" ]; then
        optional_args="\${optional_args} --max_freq ${program_args.max_freq}"
    fi
    if [ "${program_args.keplerian_template_bank}" != "null" ]; then
        optional_args="\${optional_args} --keplerian_template_bank_file ${program_args.keplerian_template_bank}"
    fi
    if [ "${program_args.birdie_list}" != "null" ]; then
        optional_args="\${optional_args} -z ${program_args.birdie_list}"
    fi

    if [ "${program_args.chan_mask}" != "null" ]; then
        optional_args="\${optional_args} -k ${program_args.chan_mask}"
    fi

    if [ "${program_args.nsamples}" != "null" ]; then
        optional_args="\${optional_args} --nsamples ${program_args.nsamples}"
    fi

    if [ "${program_args.extra_args}" != "null" ]; then
        optional_args="\${optional_args} ${program_args.extra_args}"
    fi
    output_dir="peasoup_results"
    mkdir -p \${output_dir}
    # Run peasoup
    echo "Running peasoup with the following command:"
    echo "peasoup -p -i ${input_dp} --cdm ${coherent_dm} \${optional_args} --dm_file dm_file.txt -o \${output_dir}/"
    peasoup -p -i ${input_dp} --cdm ${coherent_dm} \${optional_args} --dm_file dm_file.txt -o \${output_dir}/

    # Check if peasoup command succeeded
    check_status
    # Generate a UUID for the output file
    output_dp_id=\$(uuidgen)
    check_status
  
    # Generate a UUID for each search candidate to insert into the database. Dump this to a new XML file.
    python ${params.kafka.save_search_candidate_uuid} -i ${peasoup_orig_xml}
    #Check if python command succeeded
    check_status
    segment_pepoch=\$(grep "segment_pepoch" ${output_dp} | awk -F'[<>]' '/segment_pepoch/ {printf "%.6f", \$3}')
    #Passing this as an environment variable for posterity in logs.
    cfg_name=${program_args.cfg_name}
    
    """
}

process candy_picker{
    label 'candy_picker'
    container "${params.apptainer_images.candy_picker}"

    input:
    tuple val(program_name),
          val(pipeline_id),
          val(hardware_id),
          val(input_dp), 
          val(input_dp_id), 
          val(target_name), 
          val(beam_id), 
          val(utc_start),
          val(beam_name),
          val(coherent_dm_list),
          val(cfg_name),
          val(filstr),
          val(segment_pepoch)

    output:
    tuple env(output_dp), env(output_dp_id), env(publish_dir), val(beam_name), val(coherent_dm_list), val(cfg_name), val(utc_start), val(target_name), val(beam_id), val(filstr), val(segment_pepoch)

    script:
    """
    #!/bin/bash
    set -euo pipefail

    # Reusable function to check status of last command
    check_status() {
        local exit_code=\$?
        if [ \$exit_code -ne 0 ]; then
            >&2 echo "ERROR: Last command failed with exit code \$exit_code"
            exit 1
        fi
        return 0
    }
    

    candy_picker_input_file_string=""
    for filename_prefix in ${filstr}; do
        #Define directory strucure
        dirname="${params.publish_dir_prefix}/05_SEARCH/${target_name}/${utc_start}/\${filename_prefix}/${params.pipeline_name}/${cfg_name}/"
        filename="output.xml"
        # Create a directory with filename_prefix as its name
        mkdir -p \${filename_prefix}
        # Copy the output.xml file to the directory
        cp \${dirname}/\${filename} \${filename_prefix}/
        # Append the filename to the input file string
        candy_picker_input_file_string="\${candy_picker_input_file_string} \${filename_prefix}/\${filename}"
    done

    # Run candy_picker

    candy_picker -p ${params.candidate_filter.candy_picker.period_tolerance} -d ${params.candidate_filter.candy_picker.dm_tolerance} -n ${task.cpus} \${candy_picker_input_file_string}
    
    # Check if candy_picker command succeeded
    check_status
    output_dp=\$(ls -v **/output_*.xml | xargs realpath | tr '\n' ' ')
    output_dp_id=""
    for file in \$output_dp; do
        uuid=\$(uuidgen)
        output_dp_id="\$output_dp_id \$uuid"
    done

    output_files_relative_path=\$(ls -v **/output_*.xml)
    publish_dir=""
    for file in \$output_files_relative_path; do
        dirname=\$(dirname \$file)
        publish_dirname="${params.publish_dir_prefix}/05_SEARCH/${target_name}/${utc_start}/\${dirname}/${params.pipeline_name}/${cfg_name}/"
        publish_dir="\${publish_dir} \${publish_dirname}"
    done

    # Convert to arrays
    publish_dirs=(\$publish_dir)
    output_dps=(\$output_dp)

    # Ensure both have the same length
    if [[ \${#publish_dirs[@]} -ne \${#output_dps[@]} ]]; then
        echo "Error: Mismatch in the number of items between publish_dir and output_dp"
        exit 1
    fi

    #Iterate and copy each output_dp to its respective publish_dir
    for i in "\${!output_dps[@]}"; do
        src="\${output_dps[\$i]}"
        dest="\${publish_dirs[\$i]}"
        echo "Copying \${src} to \${dest}"
        cp \${src} \${dest}
        # Check if the copy command succeeded
        check_status
    done
    

    """

}

process pulsarx {
    label 'pulsarx'
    container "${params.apptainer_images.pulsarx}"
    //publishDir "${params.publish_dir_prefix}/09_FOLD_FIL/${target_name}/${utc_start}/${filstr}/${params.pipeline_name}/${cfg_name}/${foldGroupName}/", pattern: "*.{ar,png,cands,csv}", mode: 'copy'
    publishDir "${params.publish_dir_prefix}/09_FOLD_FIL/${target_name}/${utc_start}/${filstr}/${params.pipeline_name}/${cfg_name}/${foldGroupName}/", pattern: "*.{png,cands,csv}", mode: 'copy'

    input:
    tuple val(program_name),
          val(pipeline_id),
          val(hardware_id),
          val(program_args), 
          val(pulsarx_id), 
          val(input_dp), 
          val(input_dp_id),
          path(config_file), 
          val(foldGroupName),
          val(target_name),
          val(beam_id),
          val(utc_start),
          val(beam_name), 
          val(coherent_dm), 
          val(cfg_name),
          val(filstr)
    
    output:
    //path(*.ar, *.png) are kept for downstream processes, and env (output_archives, outputplots) for the database.
    tuple path("*.ar"), path("*.png"), path("*.cands"), path("*.csv"), path("search_fold_merged.csv"), val(foldGroupName), env(output_dp), env(output_dp_id), env(publish_dir), env(pulsarx_cands_file), env(fold_candidate_id), env(search_fold_merged), val(target_name), val(beam_id), val(utc_start), val(cfg_name), val(filstr)

    script:
    def custom_nbin_arg = program_args.custom_nbin_plan != null ? "--custom_nbin_plan=\"${program_args.custom_nbin_plan}\"" : ""
    def extra_args = program_args.extra_args != null ? "--extra_args=\"${program_args.extra_args}\"" : ""
    """
    #!/bin/bash
    set -euo pipefail

    # Reusable function to check status of last command
    check_status() {
        local exit_code=\$?
        if [ \$exit_code -ne 0 ]; then
            >&2 echo "ERROR: Last command failed with exit code \$exit_code"
            exit 1
        fi
        return 0
    }

    publish_dir="${params.publish_dir_prefix}/09_FOLD_FIL/${target_name}/${utc_start}/${filstr}/${params.pipeline_name}/${cfg_name}/${foldGroupName}/"
    filterbank_publish_dir="${params.publish_dir_prefix}/03_FILTOOLED/${target_name}/${utc_start}/${filstr}/"

    echo "Running PulsarX folding with the following command:"
    echo '''python ${params.folding.script} -i ${input_dp} -t pulsarx -l ${program_args.nbins_low} -u ${program_args.nbins_high} -b ${beam_name} -utc ${utc_start} -threads ${program_args.threads} -p ${program_args.template_path}/${program_args.template_file} -r ${filstr} -v --config_file ${config_file} -f \${filterbank_publish_dir} ${extra_args} ${custom_nbin_arg}'''
    python ${params.folding.script} -i ${input_dp} -t pulsarx -l ${program_args.nbins_low} -u ${program_args.nbins_high} -b ${beam_name} -utc ${utc_start} -threads ${program_args.threads} -p ${program_args.template_path}/${program_args.template_file} -r ${filstr} -v --config_file ${config_file} -f \${filterbank_publish_dir} ${extra_args} ${custom_nbin_arg}

    # Check if pulsarx command succeeded
    check_status

    # Generate UUIDs for data_product DB Table
    fold_cands=\$(ls -v *.ar)

    #Run dmffdot if there are missing png files.
    for file in \$fold_cands; do
        png_file="\${file%.ar}.png"
        if [ ! -f "\$png_file" ]; then
            echo "Missing PNG file for \$png_file. Running dmffdot."
            dmffdot --telescope MeerKAT -f \$file
            # Check if dmffdot command succeeded
            check_status
        fi
    done
    
    fold_dp_id=""
    for file in \$fold_cands; do
        uuid=\$(uuidgen)
        fold_dp_id="\$fold_dp_id \$uuid"
    done
    
    pulsarx_cands_file=\$(ls -v *.cands)
    #Generate IDs for fold_candidate DB Table.
    
    fold_candidate_id=""
    for file in \$fold_cands; do
        uuid=\$(uuidgen)
        fold_candidate_id="\$fold_candidate_id \$uuid"
    done

    filtered_df_for_folding=\$(ls -v filtered_df_for_folding.csv)
    python ${params.kafka.merged_fold_search} -p ${program_name} -f \${fold_cands} -x \${filtered_df_for_folding} -d \${fold_dp_id} -u \${fold_candidate_id} -c \${pulsarx_cands_file}
    # Check if python command succeeded
    check_status
    search_fold_merged=search_fold_merged.csv
    data_dir=\$(pwd)

    rest_files=\$(ls -v *.png *.csv)
    rest_dp_id=""
    for file in \$rest_files; do
        uuid=\$(uuidgen)
        rest_dp_id="\$rest_dp_id \$uuid"
    done
    output_dp="\$fold_cands \$rest_files"
    output_dp_id="\$fold_dp_id \$rest_dp_id"
    

    """
}

process pics{
    label 'pics'
    container "${params.apptainer_images.pics}"
    publishDir "${params.publish_dir_prefix}/10_FOLD_FIL_FILTER/${target_name}/${utc_start}/${filstr}/${params.pipeline_name}/${cfg_name}/${foldGroupName}/", pattern: "*.csv", mode: 'copy'
    //errorStrategy 'ignore'
    

    errorStrategy { 
        if (task.attempt <= 3) {
            sleep(task.attempt * 60000 as long) // Retry with increasing delay of 1 minute times the attempt number
            return 'retry' // Retry the task
        } else {
            return 'ignore' // Ignore errors after exceeding retries
        }
    }

    
    input:
    tuple val(program_name),
          val(pipeline_id),
          val(hardware_id),
          path(archives), 
          path(search_fold_merged),
          val(foldGroupName), 
          path(pngs),
          val(input_dp), 
          val(input_dp_id), 
          val(target_name),
          val(beam_id),
          val(utc_start),
          val(cfg_name),
          val(filstr),
          val(archive_source_dir)
          
    output:
    tuple path("search_fold_pics_merged.csv"), val(foldGroupName), env(output_dp), env(output_dp_id), env(publish_dir), val(beam_id)
    script:
    """
    #!/bin/bash
    set -euo pipefail

    # Reusable function to check status of last command
    check_status() {
        local exit_code=\$?
        if [ \$exit_code -ne 0 ]; then
            >&2 echo "ERROR: Last command failed with exit code \$exit_code"
            exit 1
        fi
        return 0
    }
    echo "Running PICS"
    publish_dir="${params.publish_dir_prefix}/10_FOLD_FIL_FILTER/${target_name}/${utc_start}/${filstr}/${params.pipeline_name}/${cfg_name}/${foldGroupName}/PICS/"
    mkdir -p \${publish_dir}
    python ${params.candidate_filter.ml_candidate_scoring.pics_script} -i \$(pwd) -m ${params.candidate_filter.ml_candidate_scoring.models_dir} -f ${search_fold_merged} -g --create_shortlist_csv -s ${archive_source_dir} -p \${publish_dir}
    # Check if python command succeeded
    check_status
    output_dp="search_fold_pics_merged.csv"
    output_dp_id=\$(uuidgen)

    """

}

process post_folding_heuristics{
    label 'post_folding_heuristics'
    container "${params.apptainer_images.pulsarx}"
    publishDir "${params.publish_dir_prefix}/10_FOLD_FIL_FILTER/${target_name}/${utc_start}/${filstr}/${params.pipeline_name}/${cfg_name}/${foldGroupName}/", pattern: "search_postfold_heuristics_merged.csv", mode: 'copy'

    //errorStrategy 'ignore'

    errorStrategy { 
        if (task.attempt <= 3) {
            sleep(task.attempt * 60000 as long) // Retry with increasing delay of 1 minute times the attempt number
            return 'retry' // Retry the task
        } else {
            return 'ignore' // Ignore errors after exceeding retries
        }
    }

    
    input:
    tuple val(program_name),
          val(pipeline_id),
          val(hardware_id),
          path(archives), 
          path(search_fold_merged),
          val(foldGroupName), 
          path(pngs),
          val(input_dp), 
          val(input_dp_id), 
          val(target_name),
          val(beam_id),
          val(utc_start),
          val(cfg_name),
          val(filstr),
          val(archive_source_dir)
          
    output:
    tuple path("search_postfold_heuristics_merged.csv"), val(foldGroupName), env(output_dp), env(output_dp_id), env(publish_dir), val(beam_id)
    script:
    """
    #!/bin/bash
    set -euo pipefail


    # Reusable function to check status of last command
    check_status() {
        local exit_code=\$?
        if [ \$exit_code -ne 0 ]; then
            >&2 echo "ERROR: Last command failed with exit code \$exit_code"
            exit 1
        fi
        return 0
    }
    publish_dir="${params.publish_dir_prefix}/10_FOLD_FIL_FILTER/${target_name}/${utc_start}/${filstr}/${params.pipeline_name}/${cfg_name}/${foldGroupName}/"
    mkdir -p \${publish_dir}

    python ${params.candidate_filter.calculate_post_folding_heuristics.script} -i ${search_fold_merged} --num_workers ${task.cpus} --threshold ${params.candidate_filter.calculate_post_folding_heuristics.alpha_snr_threshold} --create_shortlist_csv -g -s ${archive_source_dir} -p \${publish_dir}
    # Check if python command succeeded
    check_status
    output_dp="search_postfold_heuristics_merged.csv"
    output_dp_id=\$(uuidgen)
    """
}

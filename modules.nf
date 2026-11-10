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
    tuple path(output_dp), env(output_dp_id), env(publish_dir), env(segment_pepoch), val(beam_name), val(coherent_dm), env(cfg_name), val(utc_start), val(target_name), val(beam_id), val(filstr), val(program_args.dm_start), val(program_args.dm_end), emit: results
    // Dedispersed time series dumped by peasoup (-d) and normalised in place, one .dat/.inf pair per DM trial; only when
    // program_args.dump_timeseries == 1 (the hierarchical search sets it for the first bank chunk of every DM range).
    tuple val(filstr), val(utc_start), val(beam_id), val(program_args), path("timeseries/*"), optional: true, emit: timeseries

    publishDir "${params.publish_dir_prefix}/${program_args.search_dir ?: '05_SEARCH'}/${target_name}/${utc_start}/${filstr}/${params.pipeline_name}/${program_args.dm_start}_${program_args.dm_end}/${program_args.cfg_name}/", pattern: "${output_dp}", mode: 'copy'
    publishDir "${params.publish_dir_prefix}/${program_args.search_dir ?: '05_SEARCH'}/${target_name}/${utc_start}/${filstr}/${params.pipeline_name}/${program_args.timeseries_publish_dir ?: (program_args.dm_start + '_' + program_args.dm_end + '/' + program_args.cfg_name)}/", pattern: "timeseries/*", saveAs: { fn -> fn.toString().replaceFirst('^timeseries/', '') }, mode: 'copy'

    script:
    // Stage directory of the published results: the default search tree unless the search configuration
    // says otherwise (the hierarchical stage-2 search sets program_args.search_dir).
    def search_dir = program_args.search_dir ?: '05_SEARCH'
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
    publish_dir="${params.publish_dir_prefix}/${search_dir}/${target_name}/${utc_start}/${filstr}/${params.pipeline_name}/${program_args.dm_start}_${program_args.dm_end}/${program_args.cfg_name}/"


    # Write out dm_file.txt: trials dm_start + k * dm_step for all k with dm_start + k * dm_step < dm_end. The count is
    # floor((dm_end - dm_start) / dm_step - 1e-6) + 1 rather than np.arange's ceil, so that floating-point rounding of
    # an exact multiple (e.g. 137.2:137.3:0.05 -> 2.0000000000004547) cannot add dm_end itself as an extra trial.
    python -c "import numpy as np; n = int(np.floor((${program_args.dm_end} - ${program_args.dm_start}) / ${program_args.dm_step} - 1e-6)) + 1; dm_trials = np.round(${program_args.dm_start} + ${program_args.dm_step} * np.arange(n), 6); f=open('dm_file.txt','w'); f.writelines(f'{dm}\\n' for dm in dm_trials); f.close()"
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
    if [ "${program_args.dump_timeseries}" == "1" ]; then
        # dump the dedispersed time series of every DM trial (PRESTO-style .dat + .inf) before searching
        optional_args="\${optional_args} -d timeseries"
    fi
    output_dir="peasoup_results"
    mkdir -p \${output_dir}
    # Run peasoup
    echo "Running peasoup with the following command:"
    echo "peasoup -p -i ${input_dp} --cdm ${coherent_dm} \${optional_args} --dm_file dm_file.txt -o \${output_dir}/"
    peasoup -p -i ${input_dp} --cdm ${coherent_dm} \${optional_args} --dm_file dm_file.txt -o \${output_dir}/

    # Check if peasoup command succeeded
    check_status
    if [ "${program_args.dump_timeseries}" == "1" ]; then
        # normalise the dumped series once, in place (only the normalised series are published); the marker in
        # their .inf tells the stage-2 fold search not to normalise them again
        python ${params.hierarchical_search.script} normalise timeseries/*.dat
        check_status
    fi
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
          val(segment_pepoch),
          val(dm_start),
          val(dm_end),
          val(stage_dirs)

    output:
    tuple env(output_dp), env(output_dp_id), env(publish_dir), val(beam_name), val(coherent_dm_list), val(cfg_name), val(utc_start), val(target_name), val(beam_id), val(filstr), val(segment_pepoch), val(dm_start), val(dm_end), val(stage_dirs)

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
    
    filstr_array=(${filstr})
    dm_start_array=(${dm_start})
    dm_end_array=(${dm_end})
    filename="output.xml"

    candy_picker_input_file_string=""
    for i in "\${!filstr_array[@]}"; do
        filename_prefix="\${filstr_array[i]}"
        current_dm_start="\${dm_start_array[i]}"
        current_dm_end="\${dm_end_array[i]}"

        # Define the directory structure
        dirname="${params.publish_dir_prefix}/${stage_dirs.search_dir}/${target_name}/${utc_start}/\${filename_prefix}/${params.pipeline_name}/\${current_dm_start}_\${current_dm_end}/${cfg_name}/"
        # Create a directory with the filename prefix as its name
        candyjar_input_dir="\${filename_prefix}/\${current_dm_start}_\${current_dm_end}"
        mkdir -p \${candyjar_input_dir}

        # Copy the output.xml file to the directory
        cp \${dirname}/\${filename} \${candyjar_input_dir}/

        # Append the filename to the input file string
        candy_picker_input_file_string="\${candy_picker_input_file_string} \${candyjar_input_dir}/\${filename}"
    done

    # Run candy_picker
    candy_picker -p ${params.candidate_filter.candy_picker.period_tolerance} -d ${params.candidate_filter.candy_picker.dm_tolerance} -n ${task.cpus} \${candy_picker_input_file_string}
    
    # Check if candy_picker command succeeded
    check_status
    output_dp=\$(ls -v **/**/output_*.xml | xargs realpath | tr '\n' ' ')
    output_dp_id=""
    for file in \$output_dp; do
        uuid=\$(uuidgen)
        output_dp_id="\$output_dp_id \$uuid"
    done

    output_files_relative_path=\$(ls -v **/**/output_*.xml)
    publish_dir=""
    for file in \$output_files_relative_path; do
        dirname=\$(dirname \$file)
        filstr_dir=\$(echo "\$dirname" | cut -d'/' -f1)  
        dm_range_dir=\$(echo "\$dirname" | cut -d'/' -f2) 
        publish_dirname="${params.publish_dir_prefix}/${stage_dirs.search_dir}/${target_name}/${utc_start}/\${filstr_dir}/${params.pipeline_name}/\${dm_range_dir}/${cfg_name}/"
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
    publishDir "${params.publish_dir_prefix}/${stage_dirs.fold_dir}/${target_name}/${utc_start}/${filstr}/${params.pipeline_name}/${dm_start}_${dm_end}/${cfg_name}/${stage_dirs.fold_group_subdir == false ? '' : foldGroupName + '/'}", pattern: "*.{png,cands,csv}", mode: 'copy'

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
          val(filstr),
          val(dm_start),
          val(dm_end),
          val(stage_dirs)
    
    output:
    //path(*.ar, *.png) are kept for downstream processes, and env (output_archives, outputplots) for the database.
    tuple path("*.ar"), path("*.png"), path("*.cands"), path("*.csv"), path("search_fold_merged.csv"), val(foldGroupName), env(output_dp), env(output_dp_id), env(publish_dir), env(pulsarx_cands_file), env(fold_candidate_id), env(search_fold_merged), val(target_name), val(beam_id), val(utc_start), val(cfg_name), val(filstr), val(dm_start), val(dm_end), val(stage_dirs)

    script:
    def custom_nbin_arg = program_args.custom_nbin_plan != null ? "--custom_nbin_plan=\"${program_args.custom_nbin_plan}\"" : ""
    def extra_args = program_args.extra_args != null ? "--extra_args=\"${program_args.extra_args}\"" : ""
    def avoid_folding_file = program_args.avoid_folding_file != "null" ? "--avoid_folding_file=\"${program_args.avoid_folding_file}\"" : ""

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

    publish_dir="${params.publish_dir_prefix}/${stage_dirs.fold_dir}/${target_name}/${utc_start}/${filstr}/${params.pipeline_name}/${dm_start}_${dm_end}/${cfg_name}/${stage_dirs.fold_group_subdir == false ? '' : foldGroupName + '/'}"
    filterbank_publish_dir="${params.publish_dir_prefix}/03_FILTOOLED/${target_name}/${utc_start}/${filstr}/"

    echo "Running PulsarX folding with the following command:"
    echo '''python ${params.folding.script} -i ${input_dp} -t pulsarx -l ${program_args.nbins_low} -u ${program_args.nbins_high} -b ${beam_name} -utc ${utc_start} -threads ${program_args.threads} -p ${program_args.template_path}/${program_args.template_file} -r ${filstr} -v --config_file ${config_file} -f \${filterbank_publish_dir} ${extra_args} ${custom_nbin_arg} ${avoid_folding_file}'''
    python ${params.folding.script} -i ${input_dp} -t pulsarx -l ${program_args.nbins_low} -u ${program_args.nbins_high} -b ${beam_name} -utc ${utc_start} -threads ${program_args.threads} -p ${program_args.template_path}/${program_args.template_file} -r ${filstr} -v --config_file ${config_file} -f \${filterbank_publish_dir} ${extra_args} ${custom_nbin_arg} ${avoid_folding_file}

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
    publishDir "${params.publish_dir_prefix}/${stage_dirs.filter_dir}/${target_name}/${utc_start}/${filstr}/${params.pipeline_name}/${dm_start}_${dm_end}/${cfg_name}/${foldGroupName}/", pattern: "*.csv", mode: 'copy'
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
          val(archive_source_dir),
          val(dm_start),
          val(dm_end),
          val(stage_dirs)
          
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
    publish_dir="${params.publish_dir_prefix}/${stage_dirs.filter_dir}/${target_name}/${utc_start}/${filstr}/${params.pipeline_name}/${dm_start}_${dm_end}/${cfg_name}/${foldGroupName}/PICS/"
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
    publishDir "${params.publish_dir_prefix}/${stage_dirs.filter_dir}/${target_name}/${utc_start}/${filstr}/${params.pipeline_name}/${dm_start}_${dm_end}/${cfg_name}/${foldGroupName}/", pattern: "search_postfold_heuristics_merged.csv", mode: 'copy'

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
          val(archive_source_dir),
          val(dm_start),
          val(dm_end),
          val(stage_dirs)
          
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
    publish_dir="${params.publish_dir_prefix}/${stage_dirs.filter_dir}/${target_name}/${utc_start}/${filstr}/${params.pipeline_name}/${dm_start}_${dm_end}/${cfg_name}/${foldGroupName}/"
    mkdir -p \${publish_dir}

    python ${params.candidate_filter.calculate_post_folding_heuristics.script} -i ${search_fold_merged} --num_workers ${task.cpus} --threshold ${params.candidate_filter.calculate_post_folding_heuristics.alpha_snr_threshold} --create_shortlist_csv -g -s ${archive_source_dir} -p \${publish_dir}
    # Check if python command succeeded
    check_status
    output_dp="search_postfold_heuristics_merged.csv"
    output_dp_id=\$(uuidgen)
    """
}

/*
 * Hierarchical (two-stage) template bank search, stage 2 and 3: hierarchical_fold_search.
 *
 * One job per dumped DM trial of a filterbank/segment group. The job first clusters ALL stage-1
 * candidates of the group in Legendre-coefficient space into S/N peaks (hierarchical_search.py cluster,
 * clustered_peaks.csv; every job of the group repeats the same deterministic clustering, about a
 * second), then fold-searches the peaks whose nearest DM trial is its own (--dm_select): fine lattice
 * per peak, branch drift and clustering every round, local refinement (stage 3).
 *
 * 'meta' is a map carrying the bookkeeping fields of the group (target_name, utc_start, beam_name,
 * beam_id, filstr, coherent_dm, segment_pepoch, dm_start, dm_end, cfg_base, cfg_hier, ...).
 */
process hierarchical_fold_search {
    label 'hierarchical_fold'
    // peasoup image: CUDA + nvcc for the resampler, python with numpy for the fold search
    container "${params.apptainer_images.hierarchical_fold}"   // src/hierarchical_fold/hierarchical_fold.def: resample_timeseries + numpy/scipy
    tag "${meta.filstr} ${cfg_name} ${meta.dm_start}_${meta.dm_end}"

    publishDir "${params.publish_dir_prefix}/${params.hierarchical_search.search_publish_dir}/${meta.target_name}/${meta.utc_start}/${meta.filstr}/${params.pipeline_name}/${meta.dm_start}_${meta.dm_end}/${cfg_name}/", pattern: "{${output_dp},foldsearch_history.csv,clustered_peaks.csv,hierarchical_summary.json}", mode: 'copy'

    input:
    tuple val(program_name),
          val(pipeline_id),
          val(hardware_id),
          val(beam_id),
          val(input_dp),
          val(input_dp_id),
          val(output_dp),
          val(meta),
          val(cfg_name),
          val(dm_select),
          val(dm_list),
          path(stage1_xmls, stageAs: 'stage1_xml_?????.xml'),  // all stage-1 XML files of the group
          path(timeseries, stageAs: 'ts_?????/*')   // the .dat and .inf of this job's DM trial

    output:
    // Same shape as the peasoup process output so that the folding stage consumes it unchanged.
    tuple path(output_dp), env(output_dp_id), env(publish_dir), val(meta.segment_pepoch), val(meta.beam_name), val(meta.coherent_dm), val(cfg_name), val(meta.utc_start), val(meta.target_name), val(meta.beam_id), val(meta.filstr), val(meta.dm_start), val(meta.dm_end), emit: xml
    // template and S/N of every stage-2 round and stage-3 refinement iteration (published only)
    path("foldsearch_history.csv"), emit: history
    // S/N peaks of the stage-1 candidates that start the search (published only)
    tuple path("clustered_peaks.csv"), path("hierarchical_summary.json"), emit: clusters

    script:
    def hs = params.hierarchical_search
    def fd = hs.fold
    def optional_args = ""
    // null: taken from the header of the stage-1 template bank (MISMATCH, TOBS, SPIN PERIOD)
    if (hs.nominal_mismatch != null) optional_args += " --nominal_mismatch ${hs.nominal_mismatch}"
    if (hs.period_min != null)       optional_args += " --period_min ${hs.period_min}"
    if (hs.clip_to_bank == 0)        optional_args += " --no_bank_clip"
    if (hs.tobs != null) optional_args += " --tobs ${hs.tobs}"
    if (fd.use_resampler == 1) optional_args += " --resampler ./resample_timeseries --resampler_mode ${fd.resampler_mode ?: 'nearest'}"
    if (fd.use_resampler == 1 && (fd.method ?: 'gpu_fold') == 'gpu_fold') optional_args += " --gpu_fold"
    def cluster_args = ""
    if (hs.nominal_mismatch != null) cluster_args += " --nominal_mismatch ${hs.nominal_mismatch}"
    if (hs.tobs != null)      cluster_args += " --tobs ${hs.tobs}"
    if (hs.max_peaks != null) cluster_args += " --max_peaks ${hs.max_peaks}"
    """
    #!/bin/bash
    set -euo pipefail

    publish_dir="${params.publish_dir_prefix}/${hs.search_publish_dir}/${meta.target_name}/${meta.utc_start}/${meta.filstr}/${params.pipeline_name}/${meta.dm_start}_${meta.dm_end}/${cfg_name}/"

    # Start of stage 2: cluster all stage-1 candidates of the group into S/N peaks
    echo "Clustering stage-1 candidates from the following XML files:"
    ls -v stage1_xml_*.xml
    python ${hs.script} cluster \\
        -i \$(ls -v stage1_xml_*.xml) \\
        --order_highest ${hs.order_highest} \\
        --f0tol_fac ${hs.f0tol_fac} \\
        --distance_tol ${hs.distance_tol} \\
        --min_prominence ${hs.min_prominence} \\
        --workers ${task.cpus} ${cluster_args}
    # one stage-1 XML as header template for the fold search output
    cp -L stage1_xml_00001.xml stage1_reference.xml

    if [ "${fd.use_resampler}" == "1" ]; then
        # resampler / GPU fold search compiled into the hierarchical_fold image (src/hierarchical_fold); never built here
        if ! resampler=\$(command -v resample_timeseries); then
            >&2 echo "ERROR: resample_timeseries not found in the container \${APPTAINER_CONTAINER:-}; set"
            >&2 echo "       apptainer_images.hierarchical_fold to an image built with: sbatch src/build_sif.sbatch hierarchical_fold"
            exit 1
        fi
        echo "resampler: \${resampler}"
        ln -s "\${resampler}" ./resample_timeseries
    fi

    # Re-pair every .dat with its .inf (staged in separate directories) under unique names
    mkdir -p series
    i=0
    for dat in \$(ls -v ts_*/*.dat); do
        stem=\$(basename "\${dat%.dat}")
        inf=\$(ls ts_*/"\${stem}.inf" | head -1)
        i=\$((i + 1))
        cp -L "\${dat}" "series/\$(printf %03d \${i})_\${stem}.dat"
        cp -L "\${inf}" "series/\$(printf %03d \${i})_\${stem}.inf"
    done
    echo "Stage-2 fold search of the DM trial ${dm_select} (dumped DM trials: ${dm_list}), time series:"
    ls -v series/*.dat

    python ${hs.script} foldsearch \\
        --peaks clustered_peaks.csv \\
        --timeseries series/*.dat \\
        --reference_xml stage1_reference.xml \\
        --dm_select ${dm_select} --dm_list ${dm_list} --only_used \\
        --nsub ${fd.nsub} --max_phase_drift_bins ${fd.max_phase_drift_bins} --nsub_max ${fd.nsub_max} \\
        --nbins ${fd.nbins} --ducy_max ${fd.ducy_max} \\
        --f0_halfrange_bins ${fd.f0_halfrange_bins} --f0_step_bins ${fd.f0_step_bins} \\
        --dm_trials ${fd.dm_trials} --refine_iter ${fd.refine_iter == null ? 3 : fd.refine_iter} \\
        --max_rounds ${fd.max_rounds ?: 5} --edge_frac ${fd.edge_frac ?: 0.9} \\
        --drift_snr_ratio ${fd.drift_snr_ratio == null ? 1.5 : fd.drift_snr_ratio} --drift_fdiff ${fd.drift_fdiff ?: 3e-3} --drift_dm_tol ${fd.drift_dm_tol ?: 1.5} \\
        --f0tol_fac ${hs.f0tol_fac} --distance_tol ${hs.distance_tol} --min_prominence ${hs.min_prominence} \\
        --order_highest ${hs.order_highest} \\
        --search_halfrange_fac ${hs.search_halfrange_fac} --search_stepsize_fac ${hs.search_stepsize_fac} \\
        --lattice ${hs.lattice ?: 'linear'} --sample_n ${hs.sample_n ?: 2000} \\
        -o ${output_dp} --history foldsearch_history.csv ${optional_args}

    # the hierarchical_fold image has no uuidgen (uuid-runtime); use python
    output_dp_id=\$(python -c 'import uuid; print(uuid.uuid4())')
    """
}

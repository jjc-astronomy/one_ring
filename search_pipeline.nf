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


    script:
    """
    #!/bin/bash
    workdir=\$(pwd)
    publish_dir="${params.publish_dir_prefix}/03_FILTOOLED/${target_name}/${utc_start}/${filstr}/"
    mkdir -p \${publish_dir}
    cd \${publish_dir}
    tmp_output_dp="${output_dp}"
    filtool_output_string=\${tmp_output_dp%_01.fil}
    filtool -t ${program_args.threads} --nbits ${program_args.nbits} --mean ${program_args.mean} --std ${program_args.std} --td ${program_args.time_decimation_factor} --fd ${program_args.freq_decimation_factor} --telescope ${program_args.telescope} -z ${program_args.rfi_filter} -o \$filtool_output_string -s ${target_name} -f ${input_dp} ${program_args.extra_args}

    # Get the metadata from the output file and store it in the environment variables
    while IFS='=' read -r key value
        do
            declare "\$key=\$value"
        done < <(python ${program_args.get_metadata} -f ${output_dp})

    # Generate a UUID for the output file
    output_dp_id=\$(uuidgen)
    #Create symlink to the output file
    cd \${workdir}
    ln -s \${publish_dir}/${output_dp} ${output_dp}

    """
}

process peasoup {
    label 'peasoup'
    container params.apptainer_images.peasoup
    errorStrategy 'ignore'

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
    publish_dir="${params.publish_dir_prefix}/05_SEARCH/${target_name}/${utc_start}/${filstr}/${params.pipeline_name}/${program_args.cfg_name}/"


    # Write out dm_file.txt
    python -c "import numpy as np; dm_trials = np.arange(${program_args.dm_start}, ${program_args.dm_end}, ${program_args.dm_step}); f=open('dm_file.txt','w'); f.writelines(f'{dm}\\n' for dm in dm_trials); f.close()"

    # Accumulate optional arguments
    optional_args=""

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
    peasoup -i ${input_dp} \
    --acc_start ${program_args.acc_start} \
    --acc_end ${program_args.acc_end} \
    --acc_pulse_width ${program_args.acc_pulse_width} \
    --acc_tol ${program_args.accel_tol} \
    --dm_pulse_width ${program_args.dm_pulse_width} \
    -m ${program_args.min_snr} \
    --ram_limit_gb ${program_args.ram_limit_gb} \
    --nharmonics ${program_args.nharmonics} \
    -t ${program_args.ngpus} \
    --limit ${program_args.total_cands_limit} \
    --fft_size ${program_args.fft_size} \
    --start_sample ${program_args.start_sample} \
    --cdm ${coherent_dm} \
    --min_freq ${program_args.min_freq} \
    --max_freq ${program_args.max_freq} \
    \${optional_args} \
    --dm_file dm_file.txt \
    -o \${output_dir}/
    # Generate a UUID for the output file
    output_dp_id=\$(uuidgen)
  
    # Generate a UUID for each search candidate to insert into the database. Dump this to a new XML file.
    python ${params.kafka.save_search_candidate_uuid} -i ${peasoup_orig_xml}
    segment_pepoch=\$(grep "segment_pepoch" ${output_dp} | awk -F'[<>]' '/segment_pepoch/ {printf "%.6f", \$3}')
    #Passing this as an environment variable for posterity in logs.
    cfg_name=${program_args.cfg_name}
    
    """
}

process pulsarx {
    label 'pulsarx'
    container "${params.apptainer_images.pulsarx}"
    publishDir "${params.publish_dir_prefix}/09_FOLD_FIL/${target_name}/${utc_start}/${filstr}/${params.pipeline_name}/${cfg_name}/", pattern: "*.{ar,png,cands,csv}", mode: 'copy'

    errorStrategy 'ignore'

    input:
    tuple val(program_name),
          val(pipeline_id),
          val(hardware_id),
          val(program_args), 
          val(pulsarx_id), 
          val(input_dp), 
          val(input_dp_id), 
          val(target_name),
          val(beam_id),
          val(utc_start),
          val(beam_name), 
          val(coherent_dm), 
          val(cfg_name),
          val(filstr)
    
    output:
    //path(*.ar, *.png) are kept for downstream processes, and env (output_archives, outputplots) for the database.
    tuple path("*.ar"), path("*.png"), path("*.cands"), path("*.csv"), path("search_fold_merged.csv"), env(output_dp), env(output_dp_id), env(publish_dir), env(pulsarx_cands_file), env(fold_candidate_id), env(search_fold_merged), val(target_name), val(beam_id), val(utc_start), val(cfg_name), val(filstr)

    script:
    """
    #!/bin/bash
    publish_dir="${params.publish_dir_prefix}/09_FOLD_FIL/${target_name}/${utc_start}/${filstr}/${params.pipeline_name}/${cfg_name}/"

    python ${params.folding.fold_script} -i ${input_dp} -t pulsarx -l ${program_args.nbins_low} -u ${program_args.nbins_high} -b ${beam_name} -utc ${utc_start} -threads ${program_args.threads} -p ${program_args.template_path}/${program_args.template_file} -v
    
    # Generate UUIDs for data_product DB Table
    fold_cands=\$(ls -v *.ar)
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

    python ${params.kafka.merged_fold_search} -p pulsarx -f \${fold_cands} -x ${input_dp} -d \${fold_dp_id} -u \${fold_candidate_id} -c \${pulsarx_cands_file}
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
    publishDir "${params.publish_dir_prefix}/10_FOLD_FIL_FILTER/${target_name}/${utc_start}/${filstr}/${params.pipeline_name}/${cfg_name}/", pattern: "*.csv", mode: 'copy'
    
    input:
    tuple val(program_name),
          val(pipeline_id),
          val(hardware_id),
          path(archives), 
          path(search_fold_merged), 
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
    tuple path("search_fold_pics_merged.csv"), env(output_dp), env(output_dp_id), env(publish_dir), val(beam_id)
    script:
    """
    #!/bin/bash
    echo "Running PICS"
    publish_dir="${params.publish_dir_prefix}/10_FOLD_FIL_FILTER/${target_name}/${utc_start}/${filstr}/${params.pipeline_name}/${cfg_name}/PICS"
    mkdir -p \${publish_dir}
    python ${params.candidate_filter.pics_script} -i \$(pwd) -m ${params.candidate_filter.ml_models_dir} -f ${search_fold_merged} -g -c -s ${archive_source_dir} -p \${publish_dir}
    output_dp="search_fold_pics_merged.csv"
    output_dp_id=\$(uuidgen)

    """

}

process calculate_alpha_beta_gamma{
    label 'calculate_alpha_beta_gamma'
    container "${params.apptainer_images.pulsarx}"
    publishDir "${params.publish_dir_prefix}/10_FOLD_FIL_FILTER/${target_name}/${utc_start}/${filstr}/${params.pipeline_name}/${cfg_name}/zero_dm_pngs/", pattern: "DM0*.png", mode: 'copy'
    publishDir "${params.publish_dir_prefix}/10_FOLD_FIL_FILTER/${target_name}/${utc_start}/${filstr}/${params.pipeline_name}/${cfg_name}/", pattern: "search_fold_alpha_beta_gamma_merged.csv", mode: 'copy'

    errorStrategy 'ignore'
    
    input:
    tuple val(program_name),
          val(pipeline_id),
          val(hardware_id),
          path(archives), 
          path(search_fold_merged), 
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
    tuple path("search_fold_alpha_beta_gamma_merged.csv"), path("DM0*.png"), env(output_dp), env(output_dp_id), env(publish_dir), val(beam_id)
    script:
    """
    #!/bin/bash
    publish_dir="${params.publish_dir_prefix}/10_FOLD_FIL_FILTER/${target_name}/${utc_start}/${filstr}/${params.pipeline_name}/${cfg_name}"
    mkdir -p \${publish_dir}
    python ${params.candidate_filter.alpha_beta_gamma_script} -i ${search_fold_merged} --num_workers ${params.candidate_filter.calculate_alpha_beta_gamma_threads} --threshold ${params.candidate_filter.alpha_threshold} -c -g -s ${archive_source_dir} -p \${publish_dir}
    output_dp="search_fold_alpha_beta_gamma_merged.csv"
    output_dp_id=\$(uuidgen)
    """
}


workflow {
    // Extract 'filtool' object from pipeline config JSON
    def filtool_prog = params.programs.find { it.program_name == 'filtool' }

    // Build a channel that emits one map per data product:
    // Each emitted value is a single Groovy map with all fields needed for the process.
   filtool_input = Channel.from( filtool_prog.data_products )
    .map { dp ->
        

        [
            program_name : filtool_prog.program_name,
            pipeline_id  : params.pipeline_id,
            hardware_id  : dp.hardware_id,
            program_args : filtool_prog.arguments,
            program_id   : filtool_prog.program_id,
            target_name  : dp.target_name,
            utc_start    : dp.utc_start,
            pointing_id  : dp.pointing_id,
            beam_name    : dp.beam_name,
            beam_id      : dp.beam_id,
            filstr       : dp.filstr,
            coherent_dm  : dp.coherent_dm,
            filenames    : dp.filenames,
            dp_id        : dp.dp_id.join(' '),
            output_dp    : "${dp.target_name}_${dp.beam_name}_${dp.coherent_dm}_01.fil"
        ]
    }

        
    
    // Run the filtool process on each emitted value in parallel
    filtool_output = filtool(filtool_input)

    // filtool_mapped emits one item per filtool result keyed by coherent_dm
    filtool_mapped = filtool_output.map { output_dp, output_dp_id, publish_dir, beam_name, beam_id, coherent_dm, tsamp, tobs, nsamples, freq_start_mhz, freq_end_mhz, tstart, tstart_utc, nchans, nbits, filstr ->
    [
        coherent_dm.toString(),
        [
            output_dp      : output_dp,
            output_dp_id   : output_dp_id,
            beam_name      : beam_name,
            beam_id        : beam_id,
            tsamp          : tsamp,
            tobs           : tobs,
            nsamples       : nsamples,
            freq_start_mhz : freq_start_mhz,
            freq_end_mhz   : freq_end_mhz,
            tstart         : tstart,
            tstart_utc     : tstart_utc,
            nchans         : nchans,
            nbits          : nbits,
            filstr         : filstr
        ]
    ]
    }


    // Suppose peasoup_prog is a list of multiple entries all with coherent_dm=120.0
    def peasoup_prog = params.programs.findAll { it.program_name == 'peasoup' }

    // Emit multiple peasoup configs keyed by the same coherent_dm
    peasoup_input = Channel.from(peasoup_prog)
        .map { dp ->
            [ dp.arguments.coherent_dm.toString(), [
                program_args : dp.arguments,
                coherent_dm  : dp.arguments.coherent_dm,
                program_id   : dp.program_id
            ] ]
        }


    peasoup_input = filtool_mapped.combine(peasoup_input, by:0).map { coherent_dm, filtool_map, peasoup_map ->
        return [
            program_name     : peasoup_prog.program_name[0],
            pipeline_id      : params.pipeline_id,
            hardware_id      : filtool_prog.data_products[0].hardware_id,
            program_args     : peasoup_map.program_args,
            program_id       : peasoup_map.program_id,
            input_dp         : filtool_map.output_dp,
            input_dp_id      : filtool_map.output_dp_id,
            filstr           : filtool_map.filstr,
            utc_start        : filtool_map.tstart_utc,
            beam_name        : filtool_map.beam_name,
            beam_id          : filtool_map.beam_id,
            coherent_dm      : coherent_dm,
            peasoup_orig_xml : "peasoup_results/overview.xml",
            output_dp        : "output.xml",
            target_name      : filtool_prog.data_products[0].target_name

        ]
    }
    peasoup_output = peasoup(peasoup_input)
    def pulsarx_prog = params.programs.findAll { it.program_name == 'pulsarx' }
    
    
    pulsarx_input = Channel.from(pulsarx_prog)
        .map { dp ->
            [ dp.arguments.pepoch.toString(), [
                program_args : dp.arguments,
                program_id   : dp.program_id
            ] ]
        }
    peasoup_mapped = peasoup_output.map { output_dp, output_dp_id, publish_dir, segment_pepoch, beam_name, coherent_dm, cfg_name, utc_start, target_name, beam_id, filstr ->
        [
            segment_pepoch.toString(),
            [
                output_dp      : output_dp,
                output_dp_id   : output_dp_id,
                segment_pepoch : segment_pepoch,
                beam_name      : beam_name,
                coherent_dm    : coherent_dm,
                cfg_name       : cfg_name,
                utc_start      : utc_start,
                target_name    : target_name,
                beam_id        : beam_id,
                filstr         : filstr
            ]
        ]
    }
    pulsarx_input = peasoup_mapped.combine(pulsarx_input, by:0).map { segment_pepoch, peasoup_map, pulsarx_map ->
        return [
            program_name     : pulsarx_prog.program_name[0],
            pipeline_id      : params.pipeline_id,
            hardware_id      : filtool_prog.data_products[0].hardware_id,
            program_args     : pulsarx_map.program_args,
            program_id       : pulsarx_map.program_id,
            input_dp         : peasoup_map.output_dp,
            input_dp_id      : peasoup_map.output_dp_id,
            target_name      : peasoup_map.target_name,
            beam_id          : peasoup_map.beam_id,
            utc_start        : peasoup_map.utc_start,
            beam_name        : peasoup_map.beam_name,
            coherent_dm      : peasoup_map.coherent_dm,
            cfg_name         : peasoup_map.cfg_name,
            filstr           : peasoup_map.filstr
        ]
    }
    
    pulsarx_output = pulsarx(pulsarx_input)

    if (params.candidate_filter.ml_fold_candidate_scoring == 1){

        pics_input = pulsarx_output.map { archives, pngs, cands, csvs, search_fold_merged_path, output_dp, output_dp_id, publish_dir, pulsarx_cands_file, fold_candidate_id, search_fold_merged_val, target_name, beam_id, utc_start, cfg_name, filstr ->
            [
                program_name    : "pics",
                pipeline_id     : params.pipeline_id,
                hardware_id     : filtool_prog.data_products[0].hardware_id,
                output_archives : archives,
                search_fold_merged : search_fold_merged_path,
                pngs : pngs,
                output_dp : output_dp,
                output_dp_id : output_dp_id,
                target_name : target_name,
                beam_id : beam_id,
                utc_start : utc_start,
                cfg_name : cfg_name,
                filstr : filstr,
                archive_source_dir : publish_dir,
            ]
            
        }
       pics_output = pics(pics_input)


    }
  if (params.candidate_filter.calculate_alpha_beta_gamma == 1){

        alpha_beta_gamma_input = pulsarx_output.map { archives, pngs, cands, csvs, search_fold_merged_path, output_dp, output_dp_id, publish_dir, pulsarx_cands_file, fold_candidate_id, search_fold_merged_val, target_name, beam_id, utc_start, cfg_name, filstr ->
            [
                program_name    : "alpha_beta_gamma",
                pipeline_id     : params.pipeline_id,
                hardware_id     : filtool_prog.data_products[0].hardware_id,
                output_archives : archives,
                search_fold_merged : search_fold_merged_path,
                pngs : pngs,
                output_dp : output_dp,
                output_dp_id : output_dp_id,
                target_name : target_name,
                beam_id : beam_id,
                utc_start : utc_start,
                cfg_name : cfg_name,
                filstr : filstr,
                archive_source_dir : publish_dir,
            ]
            
        }
       calculate_alpha_beta_gamma_output = calculate_alpha_beta_gamma(alpha_beta_gamma_input)
}
}


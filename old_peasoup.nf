#!/usr/bin/env nextflow
nextflow.enable.dsl=2

params.fil_files = "/hercules/scratch/vishnu/KNOWN_PULSAR_TEST/*.fil"
params.fft_size = "67108864"
params.dm_file = "/hercules/scratch/vishnu/NEXTFLOW/dm_file.txt"
params.search_singularity_image = "/hercules/scratch/vishnu/singularity_images/peasoup_latest.sif"
//params.fold_singularity_image = "/u/vishnu/singularity_images/presto_gpu.sif"
params.fold_singularity_image = "/u/vishnu/singularity_images/pulsarx_latest.sif"

params.fold_script = "/hercules/scratch/vishnu/NEXTFLOW/compact_nextflow/fold_peasoup_candidates.py"
params.min_snr = "8.0"
params.acc_start = "-50.0"
params.acc_end = "50.0"
params.ram_limit_gb = "60.0"
params.nh = "4"
params.ngpus = "1"
params.total_cands_limit = "100000"

// fil_files_channel = Channel.fromPath( "${params.fil_files}", checkIfExists: true )
//     fil_files_channel.view( it -> "Running search on ${it}" )


fil_files_channel = Channel.fromPath( "${params.fil_files}", checkIfExists: true )
    .map { file -> 
        def (POINTING, BAND, UTC_OBS, BEAM) = file.name.tokenize('_')
        return [file, POINTING, BAND, UTC_OBS, BEAM]
    }

fil_files_channel.view( it -> "Running search on ${it[0]}" ) // Adjusted this to view the file path only



// process peasoup {
//     label 'gpu_short'
//     container "${params.search_singularity_image}"

//     input:
//     path fil_file
//     path dm_file
//     val fft_size
//     val total_cands_limit
//     val min_snr
//     val acc_start
//     val acc_end
//     val ram_limit_gb
//     val nh
//     val ngpus

//     output:
//     path("**/*.xml"), emit: peasoup_xml_out
//     path(fil_file), emit: peasoup_fil_out

//     script:
//     """
//     peasoup -i ${fil_file} --fft_size ${fft_size} --limit ${total_cands_limit} -m ${min_snr} --acc_start ${acc_start} --acc_end ${acc_end} --dm_file ${dm_file} --ram_limit_gb ${ram_limit_gb} -n ${nh} -t ${ngpus} 
//     """
//     publishDir 'RESULTS/$POINTING/$UTC/$BEAM/', mode: 'copy'

// }

process peasoup {
    label 'gpu_short'
    container "${params.search_singularity_image}"

    input:
    path fil_file
    val POINTING
    val BAND
    val UTC_OBS
    val BEAM
    path dm_file
    val fft_size
    val total_cands_limit
    val min_snr
    val acc_start
    val acc_end
    val ram_limit_gb
    val nh
    val ngpus

    output:
    path("**/*.xml"), emit: peasoup_xml_out
    path(fil_file), emit: peasoup_fil_out

    script:
    """
    peasoup -i ${fil_file} --fft_size ${fft_size} --limit ${total_cands_limit} -m ${min_snr} --acc_start ${acc_start} --acc_end ${acc_end} --dm_file ${dm_file} --ram_limit_gb ${ram_limit_gb} -n ${nh} -t ${ngpus} 
    """
    publishDir "RESULTS/${POINTING}/${UTC_OBS.split('-')[0]}/${BAND}/${BEAM}/03_SEARCH/", mode: 'copy'
}


process create_fold_commands {
    label 'short'
    container "${params.fold_singularity_image}"

    input:
    path peasoup_fil_out
    path peasoup_xml_out

    output:
    path('commands.txt'), emit: commands
    path peasoup_fil_out, emit: fold_fil_out

    script:
    """
    python ${params.fold_script} -i ${peasoup_xml_out} > commands.txt
    """
}


process prepfold {
    label 'short'
    container "${params.fold_singularity_image}"

    input:
    tuple val(command), path(peasoup_fil_out)

    script:
    """
    echo ${command} > test.txt
    """
}


process execute_command {
    label 'short'
    container "${params.fold_singularity_image}"

    input:
    tuple val(command), path(peasoup_fil_out)

    script:
    """
    echo ${command} > test.txt
    """
}

process fold_peasoup_cands_pulsarx {
    label 'long_multi'
    container "${params.fold_singularity_image}"

    input:
    path peasoup_xml_out
    val POINTING
    val BAND
    val UTC_OBS
    val BEAM

    script:
    """
    python ${params.fold_script} -i ${peasoup_xml_out} -t pulsarx
    """

    publishDir "RESULTS/${POINTING}/${UTC_OBS.split('-')[0]}/${BAND}/${BEAM}/04_FOLDING/", mode: 'copy'


}



}


workflow {
    main:
    peasoup_out = peasoup(fil_files_channel, params.dm_file, params.fft_size, params.total_cands_limit, params.min_snr, params.acc_start, params.acc_end, params.ram_limit_gb, params.nh, params.ngpus)


    
    // create_fold_commands_out = create_fold_commands(peasoup_out.peasoup_fil_out, peasoup_out.peasoup_xml_out)
    
    // commands_ch = create_fold_commands_out.commands.splitText()
    // commands_ch.view { "Processing command: ${it}" }
    // combined_ch = commands_ch.combine(create_fold_commands_out.fold_fil_out)

    // execute_command(combined_ch)
}

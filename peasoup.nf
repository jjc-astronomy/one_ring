#!/usr/bin/env nextflow
nextflow.enable.dsl=2

params.fil_files = "/hercules/scratch/vishnu/NEXTFLOW/compact_nextflow/*.fil"
params.fft_size = "8388608"
params.dm_file = "/hercules/scratch/vishnu/NEXTFLOW/dm_file.txt"
params.search_singularity_image = "/hercules/scratch/vishnu/singularity_images/peasoup_latest.sif"
//params.fold_singularity_image = "/u/vishnu/singularity_images/presto_gpu.sif"
params.fold_singularity_image = "/u/vishnu/singularity_images/pulsarx_latest.sif"
params.pulsarx_fold_template="/hercules/scratch/vishnu/NEXTFLOW/compact_nextflow/meerkat_fold.template"
params.fold_script = "/hercules/scratch/vishnu/NEXTFLOW/compact_nextflow/fold_peasoup_candidates.py"
params.min_snr = "8.0"
params.acc_start = "-50.0"
params.acc_end = "50.0"
params.ram_limit_gb = "60.0"
params.nh = "4"
params.ngpus = "1"
params.total_cands_limit = "100000"
params.telescope = "meerkat"

// fil_files_channel = Channel.fromPath( "${params.fil_files}", checkIfExists: true )
//     .map { file -> 
//         def (POINTING, BAND, UTC_OBS, BEAM) = file.name.tokenize('_')
//         BEAM = new File(BEAM).baseName
//         return tuple(file, POINTING, BAND, UTC_OBS, BEAM, params.dm_file, params.fft_size, params.total_cands_limit, params.min_snr, params.acc_start, params.acc_end, params.ram_limit_gb, params.nh, params.ngpus)
//     }

fil_files_channel = Channel.fromPath( "${params.fil_files}", checkIfExists: true )
    .map { file -> 
        def (POINTING, BAND, UTC_OBS, BEAM) = file.name.tokenize('_')
        BEAM = new File(BEAM).baseName
        return tuple(file, POINTING, BAND, UTC_OBS, BEAM)
    }


//fil_files_channel.view

// Split the channel to get only the file for filtool
//only_file_channel = fil_files_channel.map { it[0] }


fil_files_channel.view( it -> "Running search on ${it[0]}" ) 

process filtool {
    label 'short'
    container "${params.fold_singularity_image}"

    input:
    tuple path(fil_file), val(POINTING), val(BAND), val(UTC_OBS), val(BEAM)
    val rfi_filter
    val threads
    val telescope

    output:
    tuple path("${POINTING}_${BAND}_${UTC_OBS}_${BEAM}_01.fil"), val(POINTING), val(BAND), val(UTC_OBS), val(BEAM)

    script:
    """
    filtool -t ${threads} --telescope ${telescope} -z ${rfi_filter} --cont -o ${POINTING}_${BAND}_${UTC_OBS}_${BEAM} -f ${fil_file} 
    """
}



process peasoup {
    label 'gpu_short'
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


process fold_peasoup_cands_pulsarx {
    label 'short'
    container "${params.fold_singularity_image}"
    //publishDir "RESULTS/${POINTING}/${UTC_OBS}/${BAND}/${BEAM}/04_FOLDING/", pattern: '*.ar,*.png', mode: 'copy'
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
    python3 ${params.fold_script} -i ${peasoup_xml_out} -t pulsarx -p ${pulsarx_fold_template} -b ${BEAM}
    """

}

workflow {

    
    filtool_output= filtool(fil_files_channel, "zdot", "12", params.telescope)
    peasoup_results = peasoup(filtool_output, params.dm_file, params.fft_size, params.total_cands_limit, params.min_snr, params.acc_start, params.acc_end, params.ram_limit_gb, params.nh, params.ngpus)
    fold_peasoup_cands_pulsarx(peasoup_results, params.pulsarx_fold_template)


}



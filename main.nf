#!/usr/bin/env nextflow
nextflow.enable.dsl=2

fil_files_channel = Channel.fromPath( "${params.fil_files}", checkIfExists: true )
    .map { file -> 
        def (POINTING, BAND, UTC_OBS, BEAM) = file.name.tokenize('_')
        BEAM = new File(BEAM).baseName
        return tuple(file, POINTING, BAND, UTC_OBS, BEAM)
    }



fil_files_channel.view( it -> "Running search on ${it[0]}" ) 

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


process fold_peasoup_cands_pulsarx {
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

include { fromQuery } from 'plugin/nf-sqldb'
include { sqlInsert } from 'plugin/nf-sqldb'
 
 workflow {   

    outputChannel = readfile_parser(fil_files_channel).splitCsv()
    outputChannel.flatMap{it ->
    def metadata = [:]
    [fieldNames, it].transpose().each { fieldName, value ->
        metadata[fieldName] = value
    }
    return metadata
    }

    outputChannel.view()
 
    query = 'select * from project'
    //query = 'show tables'
    channel.fromQuery(query, db: 'compact').view()
    filtool_output = filtool(fil_files_channel, "zdot", "12", params.telescope)
    peasoup_results = peasoup(filtool_output, params.dm_file, params.fft_size, params.total_cands_limit, params.min_snr, params.acc_start, params.acc_end, params.ram_limit_gb, params.nh, params.ngpus)
    fold_peasoup_cands_pulsarx(peasoup_results, params.pulsarx_fold_template)
    prepfold(peasoup_results)

}






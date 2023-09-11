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
    label 'filtool'
    container "${params.fold_singularity_image}"
    //scratch "${params.tmp_dir}"
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
    python3 ${params.fold_script} -i ${peasoup_xml_out} -t pulsarx -p ${pulsarx_fold_template} -b ${BEAM} -threads 4
    """

}

process prepfold {
    label 'prepfold'
    container "${params.presto_singularity_image}"
    //scratch "${params.tmp_dir}"
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



process InsertTelescope {
    executor = 'local'
    
    input:
    val telescopeName

    output:
    stdout 

    script:
    """
    echo "SELECT COUNT(*) FROM telescope WHERE name='${telescopeName}' AND rownum = 1"
    """
}

// // Check if the telescopes exist in the database
//     process checkTelescopeExist {
//         input:
//         val name from telescopeNames

//         output:
//         tuple val(name), val(true) optional true into existStatus

//         script:
//         """
//         EXISTS=$(fromQuery 'SELECT COUNT(*) FROM telescope WHERE name="$name"')
//         if [[ "$EXISTS" -gt 0 ]]; then
//             touch exists
//         fi
//         """
//     }

//     // Collect the statuses and filter for non-existent telescopes
//     existStatus
//         .filter { status -> status[1] == null }
//         .map { it[0] }
//         .set { missingTelescopes }

//     // Insert missing telescopes into the database
//     process insertTelescope {
//         input:
//         val name from missingTelescopes

//         script:
//         """
//         sqlInsert "INSERT INTO telescope (name) VALUES ('$name')"
//         """
//     }



// include { fromQuery } from 'plugin/nf-sqldb'
// include { sqlInsert } from 'plugin/nf-sqldb'

// workflow {
    
//     // Define the telescope names
//     Channel.from(['MeerKAT', 'Effelsberg', 'Parkes'])
//         .set { telescopeNames }

    
// }



// Insert missing telescopes into the database
process insertTelescope {
    input:
    val telescopeName

    output:
    val count 

    script:
    """
    description=""
    case "$telescopeName" in
        "MeerKAT")
            description="South African radio telescope"
            ;;
        "Effelsberg")
            description="German radio telescope"
            ;;
        "Parkes")
            description="Australian radio telescope"
            ;;
        "Arecibo")
            description="Puerto Rican radio telescope"
            ;;
        "GBT")
            description="Robert C. Byrd Green Bank Telescope"
            ;;
        "FAST")
            description="Five-hundred-meter Aperture Spherical radio Telescope"
            ;;
        "Simulated")
            description="Simulated data"
            ;;
        *)
            description="Unknown telescope"
            ;;
    esac

    query="SELECT COUNT(*) FROM telescope WHERE name='$telescopeName'"
    echo -n "$telescopeName:$description" > count.txt
    """

}


process telescope_insert {
    executor = 'local'
    input:
    val telescopeName
    val description

    output:
    stdout

    script:
    """
    echo "INSERT INTO telescope (name, description) 
    SELECT * FROM (SELECT '$telescopeName', '$description') AS tmp
    WHERE NOT EXISTS (
        SELECT name FROM telescope WHERE name = '$telescopeName'
    ) LIMIT 1;"

    """
}

process test_insert {
    executor = 'local'
    input:
    val telescopeName
    val description

    output:
    stdout

    script:
    """
    echo "$telescopeName, $description"

    """
}



def fieldNames = ['Telescope', 'Pointing', 'START_UTC', 'MJD', 'RA_STR_J2000', 'DEC_STR_J2000', 'RA_DEG_J2000', 'DEC_DEG_J2000', 'TSAMP', 'CENTRAL_FREQ', 'LOW_CHAN_MHz', 'HIGH_CHAN_MHz', 'CHAN_BW_MHz', 'NUM_CHAN', 'BW_MHz', 'NBIT', 'NSAMPLES', 'TOBS']

// Channel outputChannel = readfile_parser(fil_files_channel).splitCsv()
// Channel metadataMap = outputChannel.map { line -> 
//     def values = line.split(',')
//     def metadata = [:]
//     for(int i=0; i < fieldNames.size(); i++) {
//         metadata[fieldNames[i]] = values[i]
//     }
//     return metadata
// }
// metadataMap.set { parsed_metadata }



include { fromQuery } from 'plugin/nf-sqldb'
include { sqlInsert } from 'plugin/nf-sqldb'
def values = line.split(',')
def metadata = [:]

workflow {
    readfile_parser(fil_files_channel).splitCsv().view { "Telescope: ${it[0]}, Pointing: ${it[1]}" }
    outputChannel = readfile_parser(fil_files_channel).splitCsv()
    metadataMap = outputChannel.map { line -> 
    for(int i=0; i < fieldNames.size(); i++) {
        metadata[fieldNames[i]] = values[i]
    }
    return metadata

    }
    metadataMap.set { parsed_metadata }

    parsed_metadata.view()

    
    
    //channel.fromQuery(query, db: 'compact').view()
    //Channel.of('Parkes', 'Australian radio telescope').collate(2).sqlInsert( into: 'telescope', columns: 'name, description', db: 'compact' )
    //Channel.fromList(['GBT', 'Robert C. Byrd Green Bank Telescope']).collate(2).sqlInsert( into: 'telescope', columns: 'name, description', db: 'compact' )
    //insert_query = telescope_insert('Parkes', 'Australian radio telescope')   
    //insert_query.view() 
    //test_insert('Parkes', 'Australian radio telescope').collate(2).sqlInsert( into: 'telescope', columns: 'name, description', db: 'compact' )
    // sqlInsert(statement: insert_query, db: 'compact')
    //query = 'SELECT * from telescope'
    //query = 'DELETE from telescope where'
    //channel.fromQuery(query, db: 'compact').view()
    
    

    // filtool_output = filtool(fil_files_channel, "zdot", "12", params.telescope)
    // peasoup_results = peasoup(filtool_output, params.dm_file, params.fft_size, params.total_cands_limit, params.min_snr, params.acc_start, params.acc_end, params.ram_limit_gb, params.nh, params.ngpus)
    // fold_peasoup_cands_pulsarx(peasoup_results, params.pulsarx_fold_template)
    // prepfold(peasoup_results)

}






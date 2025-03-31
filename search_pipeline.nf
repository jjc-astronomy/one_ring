nextflow.enable.dsl=2
import groovy.json.JsonOutput

include { filtool; peasoup; candy_picker; pulsarx; pics; post_folding_heuristics } from './modules'


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
    
    //Dump the fold configuration to a JSON file.

    def fold_config = JsonOutput.prettyPrint(JsonOutput.toJson(params.folding.configuration))
    def pulsarx_fold_config = file("pulsarx_fold_config.json")
    pulsarx_fold_config.text = fold_config

    def foldGroupName = params.folding.configuration.keySet().iterator().next()

    if (params.candidate_filter.candy_picker.enable == 1){

        candy_picker_groups = params.candy_picker_groups
        candy_picker_input = peasoup_output.map { 
        xml_list, xml_dp_id_list, publish_dir_list, segment_pepoch_list, beam_name, coherent_dm, cfg_name, utc_start_list, target_name, beam_id, filstr_list ->
        
        // Ensure coherent_dm is a BigDecimal (if not already)
        def cdm = coherent_dm instanceof BigDecimal ? coherent_dm : new BigDecimal(coherent_dm.toString())
        
        def match = candy_picker_groups.find { group ->
            def lowerBound = new BigDecimal(group[0].toString())
            def upperBound = new BigDecimal(group[1].toString())
            return lowerBound <= cdm && cdm <= upperBound
        }
        
        def groupKey = match ? match.toString() : 'none'
        return tuple(groupKey, xml_list, xml_dp_id_list, publish_dir_list, segment_pepoch_list, beam_name, coherent_dm, cfg_name, utc_start_list, target_name, beam_id, filstr_list)
        }
        .groupTuple(by:[0, 9, 8, 10, 5, 7])
        //Group by the interval key -> 0
        //target name -> 9
        // utc_start -> 8
        //beam id -> 10
        //beam name -> 5 (Using beam name is optional given that beam id is unique)
        //same search configuration eg full,half segment searches -> 7
        // Candy pickers runs on same beam, cdm but different subband_dm
        .map { groupKey, xml_list, xml_dp_id_list, publish_dir_list, segment_pepoch, beam_name, coherent_dm, cfg_name, utc_start, target_name, beam_id, filstr_list ->
                
            // Sort coherent_dm in ascending order along with the corresponding lists to guarantee consistent ordering
            def combined = []
            (0..<coherent_dm.size()).each { i ->
                combined << [cdm: new BigDecimal(coherent_dm[i].toString()), xml: xml_list[i], xml_dp: xml_dp_id_list[i], filstr: filstr_list[i]]
            }
            def combined_sorted = combined.sort { a, b -> a.cdm <=> b.cdm }
            def sorted_coherent_dm = combined_sorted.collect { it.cdm }
            def sorted_xml_list = combined_sorted.collect { it.xml }
            def sorted_xml_dp_id_list = combined_sorted.collect { it.xml_dp }
            def sorted_filstr_list = combined_sorted.collect { it.filstr }

            return [
                program_name : 'candy_picker',
                pipeline_id  : params.pipeline_id,
                hardware_id  : filtool_prog.data_products[0].hardware_id,
                input_dp     : sorted_xml_list.join(" "),
                input_dp_id  : sorted_xml_dp_id_list.join(" "),
                target_name  : target_name,
                beam_id      : beam_id,
                utc_start    : utc_start,
                beam_name    : beam_name,
                coherent_dm  : sorted_coherent_dm,
                cfg_name     : cfg_name,
                filstr       : sorted_filstr_list.join(" "),
                segment_pepoch : segment_pepoch[0]
            ]
            
        }





        candy_picker_output = candy_picker(candy_picker_input)

        candy_picker_mapped = candy_picker_output.map { output_dp, output_dp_id, publish_dir, beam_name, coherent_dm, cfg_name, utc_start, target_name, beam_id, filstr, segment_pepoch ->
            
            return [
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
                
        }

        // Create a new channel that unwraps the grouped records into individual accepted outputs.
        candy_picker_unwrapped = candy_picker_mapped.flatMap { record ->
            
            // Split output_dp by whitespace/newlines to get an array of file paths.
            def files = record.output_dp.toString().split(/[\s\n]+/)
            
            // Split output_dp_id by whitespace and remove any empty entries.
            def dpIds = record.output_dp_id.toString().split(/\s+/).findAll { it.trim() }
            
            // For coherent_dm and filstr, we assume each entry corresponds to a pair (one accepted and one rejected).
            def cohDMs = (record.coherent_dm instanceof List) ? record.coherent_dm : record.coherent_dm.toString().split(/\s+/).findAll { it.trim() }
            def filstrs = record.filstr.toString().split(/\s+/).findAll { it.trim() }
            
            // Determine the number of pairs (each pair contains an accepted and a rejected file)
            def nPairs = files.size() / 2
            def results = []
            
            /*
            * For each pair of file paths in the 'files' list, we have two indices:
            * - idx1: points to the first file in the pair.
            * - idx2: points to the second file in the pair.
            *
            * Typically, one file is the accepted output (output_picked.xml) and the other is the rejected output.
            * However, the order in which these files appear might not always be the same.
            * For example, in some cases, the rejected file might appear first (idx1) and the accepted file second (idx2).
            *
            * The following lines use a ternary operator to determine which file is the accepted one,
            * by checking if the file at idx1 contains the substring "output_picked.xml".
            * If it does, then that file (and its corresponding DP ID) is selected.
            * Otherwise, the file at idx2 is chosen.
            */

            for (int i = 0; i < nPairs; i++) {
                // Calculate the indices for this pair
                int idx1 = 2 * i
                int idx2 = 2 * i + 1
                
                // Identify which file in the pair is the accepted one.
                def acceptedFile = files[idx1].contains("output_picked.xml") ? files[idx1] : files[idx2]
                def acceptedId   = files[idx1].contains("output_picked.xml") ? dpIds[idx1]   : dpIds[idx2]
                
                   results << [ 
                        output_dp      : acceptedFile,
                        output_dp_id   : acceptedId,
                        segment_pepoch : record.segment_pepoch,
                        beam_name      : record.beam_name,
                        coherent_dm    : cohDMs[i],
                        cfg_name       : record.cfg_name,
                        utc_start      : record.utc_start,
                        target_name    : record.target_name,
                        beam_id        : record.beam_id,
                        filstr         : filstrs[i]
                    ]
                
            }
            return results
            
        }
    
        //Map candy_picker_unwrapped with segment_pepoch as key and prepare input for pulsarx
        candy_picker_with_pepoch_key = candy_picker_unwrapped.map { record ->
        [
            record.segment_pepoch.toString(),  // key
            record                             // the record as the value
        ]
        }

        pulsarx_input = candy_picker_with_pepoch_key.combine(pulsarx_input, by:0).map { segment_pepoch, candy_picker_map, pulsarx_map ->
            return [
                program_name     : pulsarx_prog.program_name[0],
                pipeline_id      : params.pipeline_id,
                hardware_id      : filtool_prog.data_products[0].hardware_id,
                program_args     : pulsarx_map.program_args,
                program_id       : pulsarx_map.program_id,
                input_dp         : candy_picker_map.output_dp,
                input_dp_id      : candy_picker_map.output_dp_id,
                fold_config      : pulsarx_fold_config,
                foldGroupName    : foldGroupName,
                target_name      : candy_picker_map.target_name,
                beam_id          : candy_picker_map.beam_id,
                utc_start        : candy_picker_map.utc_start,
                beam_name        : candy_picker_map.beam_name,
                coherent_dm      : candy_picker_map.coherent_dm,
                cfg_name         : candy_picker_map.cfg_name,
                filstr           : candy_picker_map.filstr
            ]
        }
    }
    else {

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
            fold_config      : pulsarx_fold_config,
            foldGroupName    : foldGroupName,
            target_name      : peasoup_map.target_name,
            beam_id          : peasoup_map.beam_id,
            utc_start        : peasoup_map.utc_start,
            beam_name        : peasoup_map.beam_name,
            coherent_dm      : peasoup_map.coherent_dm,
            cfg_name         : peasoup_map.cfg_name,
            filstr           : peasoup_map.filstr
        ]
    }

    }

    //Fold with pulsarx
    pulsarx_output = pulsarx(pulsarx_input)

    if (params.candidate_filter.ml_candidate_scoring.enable == 1){

        pics_input = pulsarx_output.map { archives, pngs, cands, csvs, search_fold_merged_path, pulsarx_batch_name, output_dp, output_dp_id, publish_dir, pulsarx_cands_file, fold_candidate_id, search_fold_merged_val, target_name, beam_id, utc_start, cfg_name, filstr ->
            [
                program_name    : "pics",
                pipeline_id     : params.pipeline_id,
                hardware_id     : filtool_prog.data_products[0].hardware_id,
                output_archives : archives,
                search_fold_merged : search_fold_merged_path,
                pulsarx_batch_name : pulsarx_batch_name,
                pngs : pngs,
                output_dp : output_dp,
                output_dp_id : output_dp_id,
                target_name : target_name,
                beam_id : beam_id,
                utc_start : utc_start,
                cfg_name : cfg_name,
                filstr : filstr,
                archive_source_dir : publish_dir
            ]
            
        }
       pics_output = pics(pics_input)


    }
    if (params.candidate_filter.calculate_post_folding_heuristics.enable == 1){

        calculate_post_folding_heuristics_input = pulsarx_output.map { archives, pngs, cands, csvs, search_fold_merged_path, pulsarx_batch_name, output_dp, output_dp_id, publish_dir, pulsarx_cands_file, fold_candidate_id, search_fold_merged_val, target_name, beam_id, utc_start, cfg_name, filstr ->
            [
                program_name    : "post_folding_heuristics",
                pipeline_id     : params.pipeline_id,
                hardware_id     : filtool_prog.data_products[0].hardware_id,
                output_archives : archives,
                search_fold_merged : search_fold_merged_path,
                pulsarx_batch_name : pulsarx_batch_name,
                pngs : pngs,
                output_dp : output_dp,
                output_dp_id : output_dp_id,
                target_name : target_name,
                beam_id : beam_id,
                utc_start : utc_start,
                cfg_name : cfg_name,
                filstr : filstr,
                archive_source_dir : publish_dir
            ]
            
        }
        
        post_folding_heuristics_output = post_folding_heuristics(calculate_post_folding_heuristics_input)
}
}


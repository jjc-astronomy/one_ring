nextflow.enable.dsl=2
import groovy.json.JsonOutput

include { filtool; peasoup; candy_picker; pulsarx; pics; post_folding_heuristics } from './modules'
include { peasoup as peasoup_hierarchical; generate_fine_template_bank; hierarchical_fold_search; merge_hierarchical_candidates } from './modules'


workflow {
    def scratchDir = file("scratch")
    scratchDir.mkdirs()  

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

    if (params.filtool_only) {
        all_filtool = filtool_output.collect()
        return
    }

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

    /*
     * Publish directories. Stage 1 normally goes to 05_SEARCH / 09_FOLD_FIL / 10_FOLD_FIL_FILTER. When the
     * hierarchical search is switched on, everything produced by this run (stage-1 and stage-2 search results,
     * folds and filters) goes to the hierarchical trees instead (hierarchical_search.*_publish_dir).
     */
    def hierarchical_enabled = params.hierarchical_search != null && params.hierarchical_search.enable == 1
    // PulsarX folding configurations (filter blocks on the XML 'snr' + candidate limits), dumped to JSON files:
    // stage 1 uses folding.configuration (FFT S/N); the hierarchical stage 2 uses
    // hierarchical_search.folding_configuration (fold S/N of the stage-2 search), falling back to the former.
    def scratch_dir = file("scratch"); scratch_dir.mkdirs()
    def pulsarx_fold_config = file("scratch/pulsarx_fold_config.json")
    pulsarx_fold_config.text = JsonOutput.prettyPrint(JsonOutput.toJson(params.folding.configuration))
    def foldGroupName = params.folding.configuration.keySet().iterator().next()
    def hier_folding = (params.hierarchical_search?.folding_configuration) ?: params.folding.configuration
    def pulsarx_fold_config_hier = file("scratch/pulsarx_fold_config_hierarchical.json")
    pulsarx_fold_config_hier.text = JsonOutput.prettyPrint(JsonOutput.toJson(hier_folding))
    def foldGroupName_hier = hier_folding.keySet().iterator().next()

    def hierarchical_dirs = [
        search_dir : params.hierarchical_search?.search_publish_dir ?: '11_HIERARCHICAL_SEARCH',
        fold_dir   : params.hierarchical_search?.fold_publish_dir   ?: '12_HIERARCHICAL_SEARCH',
        filter_dir : params.hierarchical_search?.filter_publish_dir ?: '13_HIERARCHICAL_SEARCH'
    ]
    def stage1_dirs = (hierarchical_enabled ? hierarchical_dirs : [search_dir: '05_SEARCH', fold_dir: '09_FOLD_FIL', filter_dir: '10_FOLD_FIL_FILTER']) +
                      [fold_config: pulsarx_fold_config, fold_group: foldGroupName]

    /*
     * Time series dump for stage 2 (hierarchical_search.dump_timeseries == 1): the first bank chunk (lowest
     * cfg_name) of every DM range and segment dumps the dedispersed time series with peasoup -d. The other
     * chunks of the same range would dump identical files, so they do not.
     */
    def stage2_method = hierarchical_enabled ? (params.hierarchical_search.stage2_method ?: 'fold') : null
    def segKey = { a -> [a.fft_size, a.start_sample, a.nsamples].collect { it.toString() }.join('|') }
    def dump_program_ids = [] as Set
    def dump_counts = [:]      // coherent_dm|segment -> number of dumping jobs (= DM ranges) per filterbank
    if (hierarchical_enabled && (params.hierarchical_search.dump_timeseries == 1 || stage2_method == 'fold')) {
        def dump_regex = params.hierarchical_search.stage1_cfg_regex ?: '.*'
        peasoup_prog.findAll { p -> p.arguments.cfg_name.toString() ==~ dump_regex }
            .groupBy { p -> [p.arguments.coherent_dm, p.arguments.dm_start, p.arguments.dm_end, p.arguments.fft_size, p.arguments.start_sample, p.arguments.nsamples].collect { it.toString() }.join('|') }
            .each { key, progs ->
                def first = progs.min { p -> p.arguments.cfg_name.toString() }
                dump_program_ids << first.program_id
                def ck = "${first.arguments.coherent_dm}|${segKey(first.arguments)}".toString()
                dump_counts[ck] = (dump_counts[ck] ?: 0) + 1
            }
    }
    def cfgDir = { cfg -> def c = cfg.toString(); c.contains('/') ? c.substring(0, c.lastIndexOf('/')) : c }

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
            program_args     : hierarchical_enabled ? peasoup_map.program_args + [
                                   search_dir            : stage1_dirs.search_dir,
                                   dump_timeseries       : dump_program_ids.contains(peasoup_map.program_id) ? 1 : 0,
                                   // publish directory of the dump (null: the rule falls back to the cfg directory and publishes nothing)
                                   timeseries_publish_dir: dump_program_ids.contains(peasoup_map.program_id) ? "${peasoup_map.program_args.dm_start}_${peasoup_map.program_args.dm_end}/${cfgDir(peasoup_map.program_args.cfg_name)}/timeseries".toString() : null
                               ] : peasoup_map.program_args,
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
    peasoup(peasoup_input)
    peasoup_output = peasoup.out.results

    /*
     * Hierarchical (two-stage) template bank search (params.hierarchical_search.enable == 1).
     *
     * 1. The stage-1 peasoup results of one filterbank + segment (all template bank chunks and, if
     *    merge_dm_ranges == 1, all incoherent DM ranges) are grouped together.
     * 2. generate_fine_template_bank clusters the candidates in Legendre-coefficient space, keeps the
     *    S/N peaks and builds a fine template bank around every peak (one or more chunk files).
     * 3. peasoup runs again on the same filterbank with every fine template bank chunk (stage 2).
     *    The stage-1 search arguments are reused; keys in params.hierarchical_search.peasoup_overrides
     *    replace them. The DM range is the union of the stage-1 DM ranges of the group.
     * 4. Optionally (merge_stage2 == 1) the stage-2 chunk results are merged into a single XML file
     *    (clustered again if cluster_stage2 == 1), which is what gets folded.
     *
     * Everything downstream (candy_picker, pulsarx, pics, ...) consumes 'search_output'. Every record of
     * 'search_output' carries, as its last element, the map of stage directories its products are published
     * to (search_dir / fold_dir / filter_dir).
     */
    search_output = peasoup_output.map { rec -> rec + [stage1_dirs] }

    if (params.hierarchical_search != null && params.hierarchical_search.enable == 1) {
        def hs = params.hierarchical_search
        def stage1_cfg_regex = hs.stage1_cfg_regex ?: '.*'
        def stage2_dirs = hierarchical_dirs + [fold_config: pulsarx_fold_config_hier, fold_group: foldGroupName_hier]
        def sameNumber = { a, b -> new BigDecimal(a.toString()).compareTo(new BigDecimal(b.toString())) == 0 }
        def findStage1Program = { cfg_name, coherent_dm, dm_start, dm_end ->
            peasoup_prog.find { p ->
                p.arguments.cfg_name.toString() == cfg_name.toString() &&
                sameNumber(p.arguments.coherent_dm, coherent_dm) &&
                sameNumber(p.arguments.dm_start, dm_start) &&
                sameNumber(p.arguments.dm_end, dm_end)
            }
        }
        // Number of stage-1 peasoup runs that end up in the same group (same filterbank, same segment),
        // so that a group can be released as soon as its own members have finished.
        def stage1GroupSize = { prog ->
            def a = prog.arguments
            peasoup_prog.count { p ->
                (p.arguments.cfg_name.toString() ==~ stage1_cfg_regex) &&
                sameNumber(p.arguments.coherent_dm, a.coherent_dm) &&
                p.arguments.fft_size.toString() == a.fft_size.toString() &&
                p.arguments.start_sample.toString() == a.start_sample.toString() &&
                p.arguments.nsamples.toString() == a.nsamples.toString() &&
                (hs.merge_dm_ranges == 1 || (sameNumber(p.arguments.dm_start, a.dm_start) && sameNumber(p.arguments.dm_end, a.dm_end)))
            }
        }

        hierarchical_groups = peasoup_output
            .filter { rec -> rec[6].toString() ==~ stage1_cfg_regex }
            .map { output_dp, output_dp_id, publish_dir, segment_pepoch, beam_name, coherent_dm, cfg_name, utc_start, target_name, beam_id, filstr, dm_start, dm_end ->
                def stage1_prog = findStage1Program(cfg_name, coherent_dm, dm_start, dm_end)
                if (stage1_prog == null) {
                    throw new IllegalStateException("hierarchical_search: no peasoup program found for cfg_name=${cfg_name} coherent_dm=${coherent_dm} dm_start=${dm_start} dm_end=${dm_end}")
                }
                def pepoch_key = new BigDecimal(segment_pepoch.toString()).stripTrailingZeros().toPlainString()
                def key = [target_name, utc_start, beam_id, beam_name, filstr, coherent_dm, pepoch_key]
                if (hs.merge_dm_ranges != 1) key += [dm_start, dm_end]
                def group_size = stage1GroupSize(stage1_prog)
                def group_key = group_size > 0 ? groupKey(key.join('|'), group_size) : key.join('|')
                tuple(group_key, output_dp, output_dp_id, segment_pepoch, beam_name, coherent_dm, cfg_name, utc_start, target_name, beam_id, filstr, dm_start, dm_end, stage1_prog)
            }
            .groupTuple(by: 0)
            .map { key, xml_list, xml_dp_id_list, segment_pepoch_list, beam_name_list, coherent_dm_list, cfg_name_list, utc_start_list, target_name_list, beam_id_list, filstr_list, dm_start_list, dm_end_list, stage1_prog_list ->
                // Stage-2 DM range: union of the stage-1 DM ranges (original string formatting is kept)
                def i_min = (0..<dm_start_list.size()).min { new BigDecimal(dm_start_list[it].toString()) }
                def i_max = (0..<dm_end_list.size()).max { new BigDecimal(dm_end_list[it].toString()) }
                // Directory part of the stage-1 cfg names, e.g. "30m/ck02" for "30m/ck02/tb_ck000"
                def cfg_bases = cfg_name_list.collect { c ->
                    def cs = c.toString()
                    cs.contains('/') ? cs.substring(0, cs.lastIndexOf('/')) : cs
                }.unique().sort()
                def cfg_base = cfg_bases.join('_')
                def meta = [
                    target_name         : target_name_list[0],
                    utc_start           : utc_start_list[0],
                    beam_name           : beam_name_list[0],
                    beam_id             : beam_id_list[0],
                    filstr              : filstr_list[0],
                    coherent_dm         : coherent_dm_list[0],
                    segment_pepoch      : segment_pepoch_list[0],
                    dm_start            : dm_start_list[i_min],
                    dm_end              : dm_end_list[i_max],
                    cfg_base            : cfg_base,
                    cfg_hier            : (hs.cfg_suffix ? "${cfg_base}/${hs.cfg_suffix}" : cfg_base).toString(),
                    stage1_cfg_names    : cfg_name_list.collect { it.toString() },
                    stage1_xml_dp_ids   : xml_dp_id_list,
                    stage1_program_args : stage1_prog_list[0].arguments,
                    stage1_program_id   : stage1_prog_list[0].program_id
                ]
                [
                    program_name : 'hierarchical_template_bank',
                    pipeline_id  : params.pipeline_id,
                    hardware_id  : filtool_prog.data_products[0].hardware_id,
                    beam_id      : meta.beam_id,
                    input_dp     : xml_list.collect { it.toString() }.join(' '),
                    input_dp_id  : xml_dp_id_list.collect { it.toString() }.join(' '),
                    meta         : meta,
                    stage1_xmls  : xml_list
                ]
            }

        generate_fine_template_bank(hierarchical_groups)
        fine_peaks = generate_fine_template_bank.out.peaks
        fine_banks = generate_fine_template_bank.out.banks

        // Join the filterbank of the group back in (same utc, beam and filstr as the filtool output)
        filtool_by_filstr = filtool_mapped.map { coherent_dm, filtool_map ->
            tuple([filtool_map.tstart_utc, filtool_map.beam_id, filtool_map.filstr].join('|'), filtool_map)
        }

        if (stage2_method == 'peasoup') {
            // One stage-2 peasoup run per fine template bank chunk
            stage2_input = fine_banks
                .map { meta, template_banks ->
                    tuple([meta.utc_start, meta.beam_id, meta.filstr].join('|'), meta, template_banks)
                }
                .combine(filtool_by_filstr, by: 0)
                .flatMap { join_key, meta, template_banks, filtool_map ->
                    def tb_list = template_banks instanceof List ? template_banks : [template_banks]
                    def overrides = hs.peasoup_overrides ?: [:]
                    tb_list.collect { tb ->
                        def chunk = tb.getBaseName() - 'fine_tb_'
                        def program_args = meta.stage1_program_args + overrides + [
                            dm_start                : meta.dm_start,
                            dm_end                  : meta.dm_end,
                            keplerian_template_bank : tb.toString(),
                            cfg_name                : "${meta.cfg_hier}/${chunk}".toString(),
                            search_dir              : stage2_dirs.search_dir
                        ]
                        [
                            program_name     : peasoup_prog.program_name[0],
                            pipeline_id      : params.pipeline_id,
                            hardware_id      : filtool_prog.data_products[0].hardware_id,
                            program_args     : program_args,
                            program_id       : meta.stage1_program_id,
                            input_dp         : filtool_map.output_dp,
                            input_dp_id      : filtool_map.output_dp_id,
                            filstr           : filtool_map.filstr,
                            utc_start        : filtool_map.tstart_utc,
                            beam_name        : filtool_map.beam_name,
                            beam_id          : filtool_map.beam_id,
                            coherent_dm      : meta.coherent_dm,
                            peasoup_orig_xml : "peasoup_results/overview.xml",
                            output_dp        : "output.xml",
                            target_name      : meta.target_name
                        ]
                    }
                }
            peasoup_hierarchical(stage2_input)
            stage2_output = peasoup_hierarchical.out.results
            // number of chunks per group, for the merge
            stage2_nchunks = fine_banks.map { meta, template_banks ->
                def n = template_banks instanceof List ? template_banks.size() : 1
                tuple([meta.target_name, meta.utc_start, meta.beam_id, meta.filstr, meta.dm_start, meta.dm_end, meta.cfg_hier].join('|'), n)
            }
        }
        else {
            /*
             * Stage 2 by folding (LOKI scheme): the dumped time series of the group's DM trials are folded with
             * the peak's orbit into sub-integrations, which are then aligned per fine-lattice template.
             * The peaks of a group are split into jobs of hs.fold.peaks_per_job peaks.
             */
            def peaks_per_job = (hs.fold?.peaks_per_job ?: 100) as int

            // time series of one filterbank + segment: all its DM ranges (one dumping job each)
            timeseries_groups = peasoup.out.timeseries
                .map { filstr, utc_start, beam_id, args, files ->
                    def key = [utc_start, beam_id, filstr, segKey(args)].join('|')
                    def n = dump_counts["${args.coherent_dm}|${segKey(args)}".toString()] ?: 1
                    tuple(groupKey(key, n), files instanceof List ? files : [files])
                }
                .groupTuple(by: 0)
                .map { key, lists -> tuple(key.toString(), lists.flatten()) }

            fold_jobs = fine_peaks
                .flatMap { meta, peaks_csv, summary_json, reference_xml, output_dp, output_dp_id, publish_dir ->
                    def summary = new groovy.json.JsonSlurper().parse(summary_json.toFile())
                    def npeaks = (summary.npeaks ?: 0) as int
                    def nchunks = Math.max(1, (int) Math.ceil(npeaks / (double) peaks_per_job))
                    def key = [meta.utc_start, meta.beam_id, meta.filstr, segKey(meta.stage1_program_args)].join('|')
                    (0..<nchunks).collect { chunk -> tuple(key, meta, peaks_csv, reference_xml, chunk, nchunks, output_dp_id) }
                }
            stage2_input = fold_jobs
                .combine(timeseries_groups, by: 0)
                .map { key, meta, peaks_csv, reference_xml, chunk, nchunks, peaks_dp_id, ts_files ->
                    [
                        program_name : 'hierarchical_fold_search',
                        pipeline_id  : params.pipeline_id,
                        hardware_id  : filtool_prog.data_products[0].hardware_id,
                        beam_id      : meta.beam_id,
                        input_dp     : ([peaks_csv] + ts_files).collect { it.toString() }.join(' '),
                        input_dp_id  : peaks_dp_id.toString().split(/\s+/)[0],
                        output_dp    : 'output.xml',
                        meta         : meta,
                        cfg_name     : "${meta.cfg_hier}/fold_ck${String.format('%03d', chunk)}".toString(),
                        chunk        : chunk,
                        nchunks      : nchunks,
                        peaks_csv    : peaks_csv,
                        reference_xml: reference_xml,
                        timeseries   : ts_files
                    ]
                }
            stage2_output = hierarchical_fold_search(stage2_input)
            stage2_nchunks = fold_jobs
                .map { key, meta, peaks_csv, reference_xml, chunk, nchunks, peaks_dp_id ->
                    tuple([meta.target_name, meta.utc_start, meta.beam_id, meta.filstr, meta.dm_start, meta.dm_end, meta.cfg_hier].join('|'), nchunks)
                }
                .unique()
        }

        if (hs.merge_stage2 == 1) {
            // stage2_nchunks: number of stage-2 jobs per group, so that the merge starts as soon as all are done
            merge_input = stage2_output
                .map { output_dp, output_dp_id, publish_dir, segment_pepoch, beam_name, coherent_dm, cfg_name, utc_start, target_name, beam_id, filstr, dm_start, dm_end ->
                    def cs = cfg_name.toString()
                    def cfg_hier = cs.substring(0, cs.lastIndexOf('/'))
                    def key = [target_name, utc_start, beam_id, filstr, dm_start, dm_end, cfg_hier].join('|')
                    tuple(key, output_dp, output_dp_id, segment_pepoch, beam_name, coherent_dm, cfg_hier, utc_start, target_name, beam_id, filstr, dm_start, dm_end)
                }
                .combine(stage2_nchunks, by: 0)
                .map { key, output_dp, output_dp_id, segment_pepoch, beam_name, coherent_dm, cfg_hier, utc_start, target_name, beam_id, filstr, dm_start, dm_end, nchunks ->
                    tuple(groupKey(key, nchunks), output_dp, output_dp_id, segment_pepoch, beam_name, coherent_dm, cfg_hier, utc_start, target_name, beam_id, filstr, dm_start, dm_end)
                }
                .groupTuple(by: 0)
                .map { key, xml_list, xml_dp_id_list, segment_pepoch_list, beam_name_list, coherent_dm_list, cfg_hier_list, utc_start_list, target_name_list, beam_id_list, filstr_list, dm_start_list, dm_end_list ->
                    def meta = [
                        target_name    : target_name_list[0],
                        utc_start      : utc_start_list[0],
                        beam_name      : beam_name_list[0],
                        beam_id        : beam_id_list[0],
                        filstr         : filstr_list[0],
                        coherent_dm    : coherent_dm_list[0],
                        segment_pepoch : segment_pepoch_list[0],
                        dm_start       : dm_start_list[0],
                        dm_end         : dm_end_list[0],
                        cfg_hier       : cfg_hier_list[0]
                    ]
                    [
                        program_name : 'hierarchical_merge',
                        pipeline_id  : params.pipeline_id,
                        hardware_id  : filtool_prog.data_products[0].hardware_id,
                        beam_id      : meta.beam_id,
                        input_dp     : xml_list.collect { it.toString() }.join(' '),
                        input_dp_id  : xml_dp_id_list.collect { it.toString() }.join(' '),
                        output_dp    : 'output.xml',
                        meta         : meta,
                        stage2_xmls  : xml_list
                    ]
                }

            hierarchical_output = merge_hierarchical_candidates(merge_input)
        }
        else {
            hierarchical_output = stage2_output
        }

        hierarchical_output = hierarchical_output.map { rec -> rec + [stage2_dirs] }
        search_output = hs.fold_stage1_candidates == 1 ? hierarchical_output.mix(search_output) : hierarchical_output
    }

    if(params.search_only){
        all_peasoup = search_output.collect()
        return
    }

    def pulsarx_prog = params.programs.findAll { it.program_name == 'pulsarx' }
    
    
    pulsarx_input = Channel.from(pulsarx_prog)
        .map { dp ->
            [ dp.arguments.pepoch.toString(), [
                program_args : dp.arguments,
                program_id   : dp.program_id
            ] ]
        }
    
    // (the folding configuration files were written at the top of the workflow)

    if (params.candidate_filter.candy_picker.enable == 1){

        candy_picker_groups = params.candy_picker_groups
        candy_picker_input = search_output.map { 
        xml_list, xml_dp_id_list, publish_dir_list, segment_pepoch_list, beam_name, coherent_dm, cfg_name, utc_start_list, target_name, beam_id, filstr_list, dm_start_list, dm_end_list, stage_dirs ->

        // Ensure coherent_dm is a BigDecimal (if not already)
        def cdm = coherent_dm instanceof BigDecimal ? coherent_dm : new BigDecimal(coherent_dm.toString())
        
        def match = candy_picker_groups.find { group ->
            def lowerBound = new BigDecimal(group[0].toString())
            def upperBound = new BigDecimal(group[1].toString())
            return lowerBound <= cdm && cdm <= upperBound
        }
        
        def groupKey = match ? match.toString() : 'none'
        return tuple(groupKey, xml_list, xml_dp_id_list, publish_dir_list, segment_pepoch_list, beam_name, coherent_dm, cfg_name, utc_start_list, target_name, beam_id, filstr_list, dm_start_list, dm_end_list, stage_dirs)
        }
        .groupTuple(by:[0, 9, 8, 10, 5, 7, 14])
        //stage directories (stage 1 vs hierarchical stage 2) -> 14
        //Group by the interval key -> 0
        //target name -> 9
        // utc_start -> 8
        //beam id -> 10
        //beam name -> 5 (Using beam name is optional given that beam id is unique)
        //same search configuration eg full,half segment searches -> 7
        // Candy pickers runs on same target, utc_start, beam, cdm but different subband_dm
        /* 
        Note for future Developers: Because we choose to 
        groupby by 7 (same search configuration), this means that if you 
        split the peasoup config by acc.range, or template bank range, 
        eg config1 runs between acc = -20 to -10 ms-2, 
        config2 acc = -10, to 0 ms-2, 
        we do NOT run candy picker across them. 
        Those two configs will be run separately!
        If such a feature arises, then do the following:
        1. Ensure peasoup process in modules.nf emits start_sample and FFT size as output.
        2. Propagate it downstream and group by those two fields as well, but remove 7 (search configuration).
        3. Now that 7 is not grouped by, cfg_name would become a list cfg_name_list similar to dm_start_list/xml_dp_id_list
        4. Inside the candy_picker process in modules.nf, cfg_name would now become a bash array, similar to filstr_list/dm_start_list/dm_end_list, iterate through it.
        5. Ensure that if candy_picker is switched off (else condition), start sample and FFT size is also emitted from peasoup process (start_sample, fft_size ->).
        
        */ 
        .map { groupKey, xml_list, xml_dp_id_list, publish_dir_list, segment_pepoch, beam_name, coherent_dm, cfg_name, utc_start, target_name, beam_id, filstr_list, dm_start_list, dm_end_list, stage_dirs ->

            // Sort coherent_dm in ascending order along with the corresponding lists to guarantee consistent ordering
            def combined = []
            (0..<coherent_dm.size()).each { i ->
                combined << [cdm: new BigDecimal(coherent_dm[i].toString()), xml: xml_list[i], xml_dp: xml_dp_id_list[i], filstr: filstr_list[i], dm_start: dm_start_list[i], dm_end: dm_end_list[i]]
            }
            def combined_sorted = combined.sort { a, b -> a.cdm <=> b.cdm }
            def sorted_coherent_dm = combined_sorted.collect { it.cdm }
            def sorted_xml_list = combined_sorted.collect { it.xml }
            def sorted_xml_dp_id_list = combined_sorted.collect { it.xml_dp }
            def sorted_filstr_list = combined_sorted.collect { it.filstr }
            def sorted_dm_start_list = combined_sorted.collect { it.dm_start }
            def sorted_dm_end_list = combined_sorted.collect { it.dm_end }
            
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
                segment_pepoch : segment_pepoch[0],
                dm_start    : sorted_dm_start_list.join(" "),
                dm_end      : sorted_dm_end_list.join(" "),
                stage_dirs  : stage_dirs
            ]
            
        }




        candy_picker_output = candy_picker(candy_picker_input)


        candy_picker_mapped = candy_picker_output.map { output_dp, output_dp_id, publish_dir, beam_name, coherent_dm, cfg_name, utc_start, target_name, beam_id, filstr, segment_pepoch, dm_start, dm_end, stage_dirs ->

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
                    filstr         : filstr,
                    dm_start       : dm_start,
                    dm_end         : dm_end,
                    stage_dirs     : stage_dirs
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
            def dmStarts = record.dm_start.toString().split(/\s+/).findAll { it.trim() }
            def dmEnds = record.dm_end.toString().split(/\s+/).findAll { it.trim() }

            
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
                        filstr         : filstrs[i],
                        dm_start       : dmStarts[i],
                        dm_end         : dmEnds[i],
                        stage_dirs     : record.stage_dirs
                    ]
                
            }
            return results
            
        }
    
        //Map candy_picker_unwrapped with segment_pepoch as key and prepare input for pulsarx
        
         candy_picker_with_pepoch_key = candy_picker_unwrapped.map { record ->
        [
            new BigDecimal(record.segment_pepoch).stripTrailingZeros().toPlainString(), // key
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
                fold_config      : candy_picker_map.stage_dirs.fold_config,
                foldGroupName    : candy_picker_map.stage_dirs.fold_group,
                target_name      : candy_picker_map.target_name,
                beam_id          : candy_picker_map.beam_id,
                utc_start        : candy_picker_map.utc_start,
                beam_name        : candy_picker_map.beam_name,
                coherent_dm      : candy_picker_map.coherent_dm,
                cfg_name         : candy_picker_map.cfg_name,
                filstr           : candy_picker_map.filstr,
                dm_start         : candy_picker_map.dm_start,
                dm_end           : candy_picker_map.dm_end,
                stage_dirs       : candy_picker_map.stage_dirs
            ]
        }
    }
    else {

        peasoup_mapped = search_output.map { output_dp, output_dp_id, publish_dir, segment_pepoch, beam_name, coherent_dm, cfg_name, utc_start, target_name, beam_id, filstr, dm_start, dm_end, stage_dirs ->
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
                filstr         : filstr,
                dm_start       : dm_start,
                dm_end         : dm_end,
                stage_dirs     : stage_dirs
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
            fold_config      : peasoup_map.stage_dirs.fold_config,
            foldGroupName    : peasoup_map.stage_dirs.fold_group,
            target_name      : peasoup_map.target_name,
            beam_id          : peasoup_map.beam_id,
            utc_start        : peasoup_map.utc_start,
            beam_name        : peasoup_map.beam_name,
            coherent_dm      : peasoup_map.coherent_dm,
            cfg_name         : peasoup_map.cfg_name,
            filstr           : peasoup_map.filstr,
            dm_start         : peasoup_map.dm_start,
            dm_end           : peasoup_map.dm_end,
            stage_dirs       : peasoup_map.stage_dirs
        ]
    }

    }

    pulsarx_output = pulsarx(pulsarx_input)

    if (params.candidate_filter.ml_candidate_scoring.enable == 1){

        pics_input = pulsarx_output.map { archives, pngs, cands, csvs, search_fold_merged_path, pulsarx_batch_name, output_dp, output_dp_id, publish_dir, pulsarx_cands_file, fold_candidate_id, search_fold_merged_val, target_name, beam_id, utc_start, cfg_name, filstr, dm_start, dm_end, stage_dirs ->
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
                archive_source_dir : publish_dir,
                dm_start : dm_start,
                dm_end : dm_end,
                stage_dirs : stage_dirs
            ]
            
        }
       pics_output = pics(pics_input)

     }
    if (params.candidate_filter.calculate_post_folding_heuristics.enable == 1){

        calculate_post_folding_heuristics_input = pulsarx_output.map { archives, pngs, cands, csvs, search_fold_merged_path, pulsarx_batch_name, output_dp, output_dp_id, publish_dir, pulsarx_cands_file, fold_candidate_id, search_fold_merged_val, target_name, beam_id, utc_start, cfg_name, filstr, dm_start, dm_end, stage_dirs ->
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
                archive_source_dir : publish_dir,
                dm_start : dm_start,
                dm_end : dm_end,
                stage_dirs : stage_dirs
            ]
            
        }
        
        post_folding_heuristics_output = post_folding_heuristics(calculate_post_folding_heuristics_input)
    }

}
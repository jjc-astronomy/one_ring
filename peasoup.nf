#!/usr/bin/env nextflow
nextflow.enable.dsl=2

params.fil_files = "/hercules/scratch/vishnu/KNOWN_PULSAR_TEST/*.fil"
params.fft_size = "67108864"
params.dm_file = "/hercules/scratch/vishnu/NEXTFLOW/dm_file.txt"
params.singularity_image = "/hercules/scratch/vishnu/singularity_images/sample_peasoup_image_centos.sif"
params.min_snr = "8.0"
params.acc_start = "-100.0"
params.acc_end = "100.0"
params.ram_limit_gb = "60.0"
params.nh = "4"
params.ngpus = "1"
params.total_cands_limit = "100000"


fil_files_channel = Channel.fromPath( "${params.fil_files}", checkIfExists: true )
    fil_files_channel.view( it -> "Running search on ${it}" )


process peasoup {
    container "${params.singularity_image}"

    executor 'slurm'
    queue 'gpu.q'
    cpus 1
    memory '8 GB'
    time '1h'
    clusterOptions '--gres=gpu:1'

    input:
    file fil_file
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
    stdout emit: peasoup_stdout

    script:
    """
    #!/usr/bin/env bash
    output_basename="\$(basename ${fil_file})"
    #get filename without extension
    output_path="\${output_basename%.*}"
    fft_size=${fft_size}
    dm_file=${dm_file}

    echo \${fil_file}
    echo mkdir -p \${output_path}
    peasoup -i ${fil_file} --fft_size ${fft_size} --limit ${total_cands_limit} -m ${min_snr} -o \${output_path} --acc_start ${acc_start} --acc_end ${acc_end} --dm_file \${dm_file} --ram_limit_gb ${ram_limit_gb} -n ${nh} -t ${ngpus} 

    """
}


workflow {
    
    peasoup(fil_files_channel, params.dm_file, params.fft_size, params.total_cands_limit, params.min_snr, params.acc_start, params.acc_end, params.ram_limit_gb, params.nh, params.ngpus )
}

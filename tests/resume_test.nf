#!/usr/bin/env nextflow

// Input: A simple string
input_string = "Hello, World!"

// Process definition
process test_process {
    input:
    val input_string

    output:
    path("output.txt")

    script:
    """
    # Run the Python script and write its output to a file
    python ${params.script_path} "${input_string}" > output.txt
    """
}

// Workflow definition
workflow {
    // Call the process with the input string
    test_process(input_string)
}
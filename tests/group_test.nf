#!/usr/bin/env nextflow
nextflow.enable.dsl=2

// Define the intervals (closed at the beginning, closed at the end)
// e.g. [48,49) means 48 ≤ dm ≤ 49, etc.
def intervals = [[0.0, 0.0],
 [47.0, 48.5],
 [49.0, 50.5],
 [51.0, 52.5],
 [53.0, 54.5],
 [55.0, 56.5]
 ]

// Simulate output values from a previous process X.
// In practice, these would come from a process output like "coherent_dm".
// Here we create a channel with test values.
Channel.from(48.5, 49, 51, 50.5, 52, 53, 55, 56.5)
    .set { coherent_dm_ch }

// Downstream process: process each group separately.
    // This simulates launching N instances of a process, one per interval group.
process ProcessGroup {
    // Tag the process instance with the group key for easy tracking.
    tag "$group"
    
    // Input a tuple: the interval group key and the list of values in that group.
    input:
    tuple val(group), val(values)
    
    // The script prints the group key and its corresponding values.
    script:
    """
    echo "Processing group: ${group} with values: ${values}"
    """
}
workflow {

    // Map each dm value to the interval group it belongs to.
    // If no interval is matched, tag it as 'none'.
    def grouped_dm = coherent_dm_ch.map { dm ->
        // Find the interval where dm is >= lower and < upper
        def match = intervals.find { it[0] <= dm && dm <= it[1] }
        // Use the matching interval as the group key; convert to string for clarity.
        def groupKey = match ? match.toString() : 'none'
        return tuple(groupKey, dm)
    }
    // Group by the interval key so that all values in the same group are collected together.
    .groupTuple()
    
    // Print out the grouping to the console
    grouped_dm.view { group, values ->
        "Group: ${group} contains values: ${values}"
    }

    

    // Launch the ProcessGroup for each group.
    grouped_dm | ProcessGroup
}

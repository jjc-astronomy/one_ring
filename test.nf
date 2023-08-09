process generate_commands {
    output:
    path 'commands.txt', emit: commands

    script:
    """
    echo "command1" > commands.txt
    echo "command2" >> commands.txt
    echo "command3" >> commands.txt
    """
}

process run_commands {
    input:
    val command

    script:
    """
    bash -c ${command}
    """
}

workflow {
    generate_commands()
    commands_ch = generate_commands.out.commands.splitText().map{ it.trim() }
    commands_ch.set{ cmd_channel }
    run_commands(cmd_channel)
}

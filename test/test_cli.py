from ssh_slurm_runner.cli import parse_cli_args


def test__given_valid_args__should_return_matching_config():
    config = parse_cli_args([
        "slurm.job",
        "--host",
        "cluster.example.com",
        "--user",
        "the_user",
        "--keyfile",
        "/home/user/.ssh/kefile",
        
    ])
    assert config.jobfile == "slurm.job"
    assert config.host == "cluster.example.com"
    assert config.user == "the_user"
    assert config.keyfile == "/home/user/.ssh/kefile"
import argparse

def parse_cli_args(args):
    parser = argparse.ArgumentParser("ssh_slurm_runner")
    parser.add_argument("jobfile", type=str)
    parser.add_argument("--host", type=str)
    parser.add_argument("--user", type=str)
    parser.add_argument("--keyfile", type=str)

    return parser.parse_args(args)
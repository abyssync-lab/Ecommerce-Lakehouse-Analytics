from project_cli import build_parser


def test_cli_accepts_all_public_commands():
    parser = build_parser()
    for command in ["doctor", "check", "report", "pipeline", "mongodb", "thrift"]:
        assert parser.parse_args([command]).command == command

from project_cli import build_parser


def test_cli_accepts_all_public_commands():
    parser = build_parser()
    for command in [
        "doctor",
        "check",
        "report",
        "reconcile",
        "pipeline",
    ]:
        assert parser.parse_args([command]).command == command


def test_cli_pipeline_subcommands_and_flags():
    parser = build_parser()

    # Bootstrap mặc định
    args_boot = parser.parse_args(["pipeline", "--scd2"])
    assert args_boot.command == "pipeline"
    assert args_boot.scd2 is True
    assert args_boot.mode == "bootstrap"

    # Incremental explicit
    args_inc = parser.parse_args(
        ["pipeline", "incremental", "--input", "batch.csv", "--batch-id", "B001", "--scd2"]
    )
    assert args_inc.command == "pipeline"
    assert args_inc.mode == "incremental"
    assert args_inc.input == "batch.csv"
    assert args_inc.batch_id == "B001"
    assert args_inc.scd2 is True

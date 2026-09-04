from environment_check import DependencyStatus, format_environment_report


def test_environment_report_contains_status_and_purpose():
    report = format_environment_report(
        [DependencyStatus("java", True, "Spark", "C:/Java/bin/java.exe")]
    )
    assert "ĐẠT" in report
    assert "Spark" in report

import pytest

from bitflyer_realtime_lake.channels import detect_event_type, extract_product_code


@pytest.mark.parametrize(
    ("channel", "event_type", "product_code"),
    [
        ("lightning_executions_BTC_JPY", "executions", "BTC_JPY"),
        ("lightning_ticker_FX_BTC_JPY", "ticker", "FX_BTC_JPY"),
        ("lightning_board_snapshot_ETH_BTC", "board_snapshot", "ETH_BTC"),
        ("lightning_board_BTC_JPY", "board_delta", "BTC_JPY"),
    ],
)
def test_channel_detection(channel: str, event_type: str, product_code: str) -> None:
    assert detect_event_type(channel) == event_type
    assert extract_product_code(channel) == product_code


def test_board_snapshot_is_detected_before_board_delta() -> None:
    channel = "lightning_board_snapshot_BTC_JPY"
    assert detect_event_type(channel) == "board_snapshot"

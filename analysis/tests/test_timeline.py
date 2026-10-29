import json

import timeline


def log(path, *records, text=()):
    lines = [json.dumps(r) for r in records] + list(text)
    path.write_text("\n".join(lines) + "\n")


def write_logs(d):
    # files are not in time order: the timeline must merge them by "time"
    log(d / "risk.log",
        {"time": "2026-10-12T10:00:00.004000000-03:00", "level": "DEBUG", "msg": "risk scored", "payment_id": "p1", "score": 12},
        {"time": "2026-10-12T10:00:00.009000000-03:00", "level": "DEBUG", "msg": "risk scored", "payment_id": "p2", "score": 7})
    log(d / "gateway.log",
        {"time": "2026-10-12T09:59:59.000000000-03:00", "level": "INFO", "msg": "gateway started", "version": "v1"},
        {"time": "2026-10-12T10:00:00.000500000-03:00", "level": "DEBUG", "msg": "payment received", "payment_id": "p1"},
        {"time": "2026-10-12T10:00:00.001000000-03:00", "level": "DEBUG", "msg": "rpc", "side": "client",
         "code": "ok", "protocol": "grpc", "payment_id": "p1", "http": "HTTP/3.0"})
    log(d / "loadgen.log",
        {"time": "2026-10-12T10:00:00.000000000-03:00", "level": "DEBUG", "msg": "payment request", "payment_id": "p1", "account_id": 4821},
        {"time": "2026-10-12T10:00:00.003000000-03:00", "level": "DEBUG", "msg": "payment response", "payment_id": "p1", "code": "ok"},
        {"time": "2026-10-12T10:00:00.008000000-03:00", "level": "DEBUG", "msg": "payment request", "payment_id": "p2", "account_id": 1},
        text=["netem: eth0 clear"])


def test_timeline_orders_and_groups(tmp_path):
    write_logs(tmp_path)
    assert timeline.main([str(tmp_path)]) == 0
    out = (tmp_path / "timeline.txt").read_text().splitlines()
    assert out[0] == "payment p1 (HTTP/3.0)"
    p1 = out[1:out.index("")]
    assert p1[0].startswith("  +0.000 ms  loadgen  payment request")
    assert [l.split()[2] for l in p1] == ["loadgen", "gateway", "gateway", "loadgen", "risk"]
    assert p1[-1].startswith("  +4.000 ms  risk ")  # the async step lands after the response
    assert "payment p2 (?)" in out and out.index("payment p2 (?)") > out.index("payment p1 (HTTP/3.0)")


def test_timeline_skips_non_json_and_unkeyed(tmp_path):
    write_logs(tmp_path)
    timeline.main([str(tmp_path)])
    text = (tmp_path / "timeline.txt").read_text()
    assert "netem" not in text and "gateway started" not in text


def test_timeline_fields(tmp_path):
    write_logs(tmp_path)
    timeline.main([str(tmp_path)])
    lines = (tmp_path / "timeline.txt").read_text().splitlines()
    assert any(l.endswith("risk     risk scored score=12") for l in lines)
    assert any(l.endswith("payment request account_id=4821") for l in lines)


def test_timeline_usage():
    assert timeline.main([]) == 2


def test_timeline_empty_is_a_failure(tmp_path, capsys):
    # debug never applied or logs lost: a trace without payments must not look fine
    log(tmp_path / "gateway.log", {"time": "2026-10-12T10:00:00Z", "level": "INFO", "msg": "gateway started"})
    assert timeline.main([str(tmp_path)]) == 1
    assert "no payment_id" in capsys.readouterr().err

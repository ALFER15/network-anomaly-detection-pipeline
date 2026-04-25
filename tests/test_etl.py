from __future__ import annotations

from pathlib import Path

from etl.cicids2017_etl import CICIDS2017ETL


def test_etl_read_chunks(tmp_path: Path) -> None:
    csv_path = tmp_path / "sample.csv"
    csv_path.write_text(
        "Flow Duration,Total Fwd Packets,Total Backward Packets,Total Length of Fwd Packets,Total Length of Bwd Packets,Flow Bytes/s,Flow Packets/s,Fwd Packet Length Mean,Bwd Packet Length Mean,SYN Flag Count,ACK Flag Count,PSH Flag Count,URG Flag Count,Label\n"
        "1,2,3,4,5,6,7,8,9,1,0,1,0,Benign\n",
        encoding="utf-8",
    )

    etl = CICIDS2017ETL(str(csv_path), chunk_size=10)
    chunk = next(etl.read_chunks())

    assert not chunk.empty
    assert "Label" in chunk.columns
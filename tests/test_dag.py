"""Тесты валидности DAG."""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


class TestDAG:
    def test_dag_file_exists(self):
        dag_path = PROJECT_ROOT / "dags" / "market_pipeline_dag.py"
        assert dag_path.exists()

    def test_dag_imports(self):
        dag_code = (PROJECT_ROOT / "dags" / "market_pipeline_dag.py").read_text(encoding="utf-8")
        # Проверяем что все ключевые task_id определены
        expected_tasks = [
            "ingest_moex",
            "ingest_yahoo_stocks",
            "ingest_yahoo_forex",
            "ingest_crypto",
            "calculate_features",
            "spark_features_batch",
            "run_anomaly_detection",
            "run_greenplum_analytics",
            "run_risk_metrics",
            "run_portfolio",
            "check_alerts",
            "send_telegram_alert",
        ]
        for task_id in expected_tasks:
            assert f'task_id="{task_id}"' in dag_code or f"task_id='{task_id}'" in dag_code

    def test_symbols_yaml_has_50_assets(self):
        import yaml
        with open(PROJECT_ROOT / "config" / "symbols.yaml", encoding="utf-8") as f:
            symbols = yaml.safe_load(f)
        total = sum(len(v) for v in symbols.values())
        assert total >= 50, f"Expected 50+ symbols, got {total}"

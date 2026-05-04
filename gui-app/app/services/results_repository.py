import csv
import json
from pathlib import Path

from app.models.benchmark_models import BenchmarkRun
from app.utils.paths import results_dir_path


class ResultsRepository:
    """Persists benchmark results for later inspection."""

    def __init__(self):
        self.results_dir = results_dir_path()
        self.results_dir.mkdir(parents=True, exist_ok=True)

    def save(self, benchmark_run: BenchmarkRun) -> tuple[Path, Path]:
        safe_name = benchmark_run.name.replace(" ", "_")
        base = self.results_dir / safe_name

        json_path = base.with_suffix(".json")
        csv_path = base.with_suffix(".csv")

        with json_path.open("w", encoding="utf-8") as handle:
            json.dump(benchmark_run.to_dict(), handle, indent=2)

        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=[
                    "policy",
                    "query",
                    "hits",
                    "misses",
                    "evictions",
                    "hit_rate",
                    "elapsed_time",
                    "ok",
                    "error",
                ],
            )
            writer.writeheader()
            for record in benchmark_run.records:
                writer.writerow(record.to_dict())

        return json_path, csv_path

    def save_sweep_csv(self, name: str, rows: list[dict]) -> Path:
        safe_name = name.replace(" ", "_")
        csv_path = (self.results_dir / safe_name).with_suffix(".csv")

        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=["frames", "policy", "hit_rate", "misses", "evictions"],
            )
            writer.writeheader()
            writer.writerows(rows)

        return csv_path

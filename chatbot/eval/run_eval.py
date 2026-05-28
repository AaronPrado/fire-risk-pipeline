import json
import re
from pathlib import Path

from chatbot.src.llm.generator import generate_sql
from chatbot.src.sql.validator import ValidationError

_DATASET_PATH = Path(__file__).parent / "dataset.jsonl"


def _load_dataset() -> list[dict]:
    entries = []
    for line in _DATASET_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            entries.append(json.loads(line))
    return entries


def _normalize_sql(sql: str) -> str:
    return re.sub(r"\s+", " ", sql.strip().lower())


def _evaluate(question: str, expected: str) -> dict:
    result = {
        "question": question,
        "expected": expected,
        "generated": None,
        "passed": False,
        "error": None,
    }
    try:
        generated = generate_sql(question)
        result["generated"] = generated
        result["passed"] = _normalize_sql(generated) == _normalize_sql(expected)
    except ValidationError as e:
        result["error"] = f"ValidationError: {e}"
    except Exception as e:
        result["error"] = f"{type(e).__name__}: {e}"
    return result


def main() -> None:
    dataset = _load_dataset()
    results = []

    print(f"Evaluando {len(dataset)} preguntas...\n")

    for i, entry in enumerate(dataset, 1):
        result = _evaluate(entry["question"], entry["sql"])
        results.append(result)

        status = "✓" if result["passed"] else "✗"
        truncated = (
            entry["question"][:70] + "..." if len(entry["question"]) > 70 else entry["question"]
        )
        print(f"[{i:02d}] {status} {truncated}")

        if not result["passed"]:
            if result["error"]:
                print(f"      Error:    {result['error']}")
            else:
                print(f"      Esperado: {result['expected']}")
                print(f"      Generado: {result['generated']}")

    passed = sum(r["passed"] for r in results)
    total = len(results)
    accuracy = passed / total * 100 if total > 0 else 0

    print(f"\n{'─' * 60}")
    print(f"Resultado: {passed}/{total} correctos ({accuracy:.0f}%)")


if __name__ == "__main__":
    main()

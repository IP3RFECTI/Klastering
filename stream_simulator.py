import json
import random
from datetime import datetime, timedelta


def add_timestamps(input_file="events.json", output_file="events_stream.json"):
    with open(input_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    current_time = datetime(2024, 7, 1, 0, 0, 0)

    for event in data:
        # случайный шаг времени
        delta = timedelta(seconds=random.randint(5, 300))
        current_time += delta

        event["time"] = current_time.isoformat()

    # перемешиваем немного, чтобы было не идеально
    random.shuffle(data)

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Добавлено время → {output_file}")


if __name__ == "__main__":
    add_timestamps()
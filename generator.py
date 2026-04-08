import random
import uuid
import json

SYSTEMS = ["Cloud", "Network", "Hardware", "Software", "SupportPortal"]

TEMPLATES = [
    {
        "title": ["Принтер не печатает", "Ошибка печати", "Зависла очередь печати"],
        "description": [
            "Устройство не реагирует",
            "Ошибка при отправке задания",
            "Принтер мигает красным"
        ],
        "category": "Оборудование"
    },
    {
        "title": ["Сервер недоступен", "Падение сервера", "Ошибка подключения"],
        "description": [
            "Нет ответа от сервера",
            "Timeout при запросе",
            "Ошибка 500"
        ],
        "category": "Инфраструктура"
    },
    {
        "title": ["Сеть не работает", "Проблемы с интернетом", "Потеря пакетов"],
        "description": [
            "Нет соединения",
            "Высокий пинг",
            "Обрывы связи"
        ],
        "category": "Сеть"
    },
    {
        "title": ["Ошибка приложения", "Краш программы", "Не запускается"],
        "description": [
            "Приложение закрывается",
            "Ошибка при старте",
            "Зависает интерфейс"
        ],
        "category": "ПО"
    }
]


def generate_payload():
    template = random.choice(TEMPLATES)

    payload = {}

    # случайно выбираем поля
    if random.random() > 0.2:
        payload["title"] = random.choice(template["title"])

    if random.random() > 0.2:
        payload["description"] = random.choice(template["description"])

    if random.random() > 0.3:
        payload["category"] = template["category"]

    # иногда добавляем шум
    if random.random() > 0.7:
        payload["severity"] = random.choice(["low", "medium", "high"])

    if random.random() > 0.8:
        payload["class"] = random.choice(["A", "B", "C"])

    return payload


def generate_dataset(n=10000, output_file="events.json"):
    data = []

    for _ in range(n):
        event = {
            "system": random.choice(SYSTEMS),
            "record_id": str(uuid.uuid4()),
            "payload": generate_payload()
        }
        data.append(event)

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Сгенерировано {n} событий → {output_file}")


if __name__ == "__main__":
    generate_dataset(10000)
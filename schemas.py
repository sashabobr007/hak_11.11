from pydantic import BaseModel


class Task(BaseModel):
    id_point: int
    adres: str
    when_created: str
    all_cards_delivered: str
    days_after: int
    quantity_accepted_tasks: int
    quantity_cards: int

class User(BaseModel):
    fio: str
    password: str
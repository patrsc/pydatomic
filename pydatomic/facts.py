from pydantic import BaseModel, Field
from typing import Any
from uuid import uuid4


class Datom(BaseModel):
    """represents a single datom"""
    
    id: int = Field(alias="_id")
    """unique ID of the datom (used in MongoDB as document ID)"""
    e: int
    """entity ID"""
    a: str
    """attribute"""
    v: Any
    """value associated to the attribute"""
    tx: int
    """transaction entity ID"""
    op: bool
    """operation: add = True, retract = False"""

    def __eq__(self, other):
        return self.id == other.id

    def __lt__(self, other):
        return self.id < other.id


class Facts:
    """represents a set of new facts (additions or retractions) before they are added to a database"""

    def __init__(self) -> None:
        self._facts = []  # list of tuples (e a v op)

    def add_set(self, entity, dct):
        """add a set of facts (attribute-value pairs) to the same entity"""
        if entity is None:
            entity = self._generate_entity()
        for attribute, value in dct.items():
            self.add(entity, attribute, value)

    def add(self, entity, attribute: str, value):
        """add single fact to entity"""
        self._facts.append((entity, attribute, value, True))

    def remove(self, entity, attribute: str, value):
        """remove single fact from entity"""
        self._facts.append((entity, attribute, value, False))

    def replace(self, entity, attribute: str, old_value, new_value):
        """replace single fact of entity"""
        self._facts.append((entity, attribute, old_value, False))
        self._facts.append((entity, attribute, new_value, True))

    def _generate_entity(self):
        """generate temporary entity id"""
        return str(uuid4())

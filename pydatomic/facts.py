from pydantic import BaseModel, Field
from typing import Any, Union
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
        """create an empty set of facts where facts can be stored later (the Facts object is mutable)
        :return: the Facts object
        """
        self._facts = []  # list of tuples (e a v op)

    def add_set(self, entity: Union[None, int, str, list], dct: dict[str, Any]):
        """add a set of facts (attribute/value pairs) to the same (given) entity
        :param entity: entity as int, lookup ref, temporary ID (string) or None for new (anonymous) entity
        :param dct: dict with attribute/value pairs
        """
        if entity is None:
            entity = self._generate_entity()
        for attribute, value in dct.items():
            self.add(entity, attribute, value)

    def add(self, entity: Union[None, int, str, list], attribute: str, value):
        """add single fact to entity
        :param entity: entity as int, lookup ref, temporary ID (string) or None for new (anonymous) entity
        :param attribute: the attribute
        :param value: the value to add for the attribute
        """
        self._facts.append((entity, attribute, value, True))

    def remove(self, entity: Union[None, int, str, list], attribute: str, value):
        """remove (retract) single fact from entity
        :param entity: entity as int, lookup ref, temporary ID (string) or None for new (anonymous) entity
        :param attribute: the attribute
        :param value: the value to remove for the attribute
        """
        self._facts.append((entity, attribute, value, False))

    def replace(self, entity: Union[None, int, str, list], attribute: str, old_value, new_value):
        """replace single fact of entity
        :param entity: entity as int, lookup ref, temporary ID (string) or None for new (anonymous) entity
        :param attribute: the attribute
        :param old_value: the old value to remove for the attribute
        :param new_value: the new value to add for the attribute
        """
        self._facts.append((entity, attribute, old_value, False))
        self._facts.append((entity, attribute, new_value, True))

    def _generate_entity(self):
        """generate temporary entity id"""
        return str(uuid4())

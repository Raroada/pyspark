from typing import List

class Item:
    def __init__(self, query:str, dependance:List[str], table_name:str, id:int):
        self.table_name = table_name
        self.query = query
        self.next:List[Item] = []
        self.dependance_completed = 0
        self.dependance = dependance
        self.dependance_cnt = 0
        self.id = id

    def is_head(self):
        True if self.dependance_cnt == 0 else False

    def can_run(self) -> bool:
        self.dependance_completed += 1
        True if self.dependance_completed == self.dependance_cnt else False

    def add_successor(self, Item):
        self.next.append(Item)
        self.dependance_cnt += 1
    
    def print(self):
        tables = 'nothing'
        if self.next is not []:
            tables = ', '.join([i.table_name for i in self.next])
        print(
f"""Object: {self.table_name}
Query: {self.query}
Has dependancies on: {self.dependance}
With Id of: {self.id}
Calls {tables} when done
"""
        )

class LinkedList:
    def __init__(self):
        self.head:List[Item] = None

    def append(self, data):
        new_node = Item(data)
        if not self.head:
            self.head.append(new_node)
            return
        current = self.head
        while current.next:
            current = current.next
        current.next = new_node

    def display(self):
        elements = []
        current = self.head
        while current:
            elements.append(current.data)
            current = current.next
        print(elements)
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
        self.is_processing = False

    def is_head(self):
        True if self.dependance_cnt == 0 else False

    def can_run(self) -> bool:
        self.dependance_completed += 1
        if (self.dependance_completed == self.dependance_cnt) and (self.is_processing == False):
            self.is_processing = True
            return True
        else:
            return False

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

class ProcessingList:
    def __init__(self):
        self.items:dict[str, Item] = {}
        self.heads:List[Item] = []

    def append_to_list(self, item:Item):
        if item.dependance is []:
            self.head
        self.items.append(item)

    def set_linked_list_heads(self):
        self.heads = [v for k,v in self.items.items() if v.dependance is []]
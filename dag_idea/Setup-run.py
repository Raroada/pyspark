from typing import List
from dag_idea import Item, ProcessingList


item_ls = [{
    'table':'dbo.Table1'
    ,'query':'SELECT 1 as col'
},
{
    'table':'dbo.Table2'
    ,'query':'SELECT 2 as new_col FROM dbo.Table1'
},
{
    'table':'dbo.Table3'
    ,'query':'SELECT col, new_col FROM dbo.Table1 t LEFT JOIN dbo.Table2 tt ON 1=1'
},
{
    'table':'dbo.Table4'
    ,'query':'SELECT 1 as col'
}
]

def test(obj:Item):
        print(f"start processing")
        print(obj.table_name)
        obj.process()
        next_ls:List[Item] = obj.get_next() 
        if next_ls is None:
            return
        for n in next_ls:
            test(n)
    

schemas = ['dbo']

item_strip_ls = {}
table_name_ls = []
id = 1
for item in item_ls:
    table_name = item['table']
    for sc in schemas:
        table_name_ls.append(table_name.lower())
    item_strip_ls[table_name] = {
        'id':id
        ,'query':item['query']
        ,'query_splice':item['query'].split(sep=' ')
    }
    id += 1

for k,v in item_strip_ls.items():
    dependance = []
    for i in v['query_splice']:
        if i.lower() in table_name_ls:
            dependance.append(i)
    v['dependance'] = dependance

linked_list = ProcessingList()
for k,v in item_strip_ls.items():
    item = Item(
            dependance=v['dependance']
            ,query=v['query']
            ,table_name=k
            ,id=v['id']
        )
    
    item.print()
    linked_list.items[k] = item


for k,v in linked_list.items.items():
    if v.dependance is not []:
        for d in v.dependance:
            item = linked_list.items.get(d)
            linked_list.add_successor(head_item=item, next_item=v)

linked_list.set_linked_list_heads()
for i in linked_list.heads:
    test(i)
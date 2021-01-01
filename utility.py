def space_remover(_list:list):
        expr = lambda x: x.replace(" ","")
        space_removed = map(expr,_list)
        return list(space_removed)
from    multiprocessing import Pool, freeze_support
from    dataclasses import dataclass
from functools import partial
from itertools import repeat


def mapper(items, one_item, workerCount=10):


    with Pool(workerCount) as p:
        # output_dict = p.map(_map, items, one_item)
        output_dict = p.starmap(_map, zip(items, repeat(one_item)))
    
    
    print(output_dict)





def _map(item, one_item):
    
    
    return item



def main():
    items = [1,2,3,4,5]
    one_item = 1

    mapper(items, one_item)


    pass



if __name__ == "__main__":
    main()

# def sum(a,b):
#     return a+b
 
# # list 1
# lst1=[2,4,6,8]
 
# # list 2
# lst2=[1,3,5,7,9]
 
# with Pool(3) as p:
#     result = p.map(sum,lst1,lst2)

# print(result)

# # result=list(map(sum,lst1,lst2))
# # print(result)
import pandas as pd
package_size_array=pd.read_csv('Package1.csv')['Package Size'].to_list()
numberincase=pd.read_csv('Package1.csv')['Number in Case'].to_list()
input_array_description=pd.read_csv('input.csv')['Item Description Line 1'].to_list()
# print(package_size_array)
# print(input_array_description)
print(numberincase)

# for index,package in enumerate(package_size_array):
#     for j in input_array_description:
#         if package.lower() in j.lower().split():
#             print('found',package,j,conversion[index])



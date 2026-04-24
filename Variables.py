#Simple List
a=[1,2,3,4]
print(a)
print(type(a))


#Unpak a colletion

fruits = ["apple", "banana", "cherry"]
x, y, z = fruits
print(x)
print(y)
print(z)


#Basic Multiple Assignment

a, b, c = 10, 20, 30
print(a)
print(b)
print(c)


#Assign Same Value to Multiple Variables

x = y = z = 100
print(x, y, z)


#Using List

values = [1, 2, 3]
a, b, c = values
print(a, b, c)


#Swapping Values

a, b = 5, 10
a, b = b, a
print(a, b)


#Example 

name, age, role = "Pratyusha", 22, "Data Engineer"
print(name)
print(age)
print(role)


#Reading Data from a Database Row

row = ("Pratyusha", 22, "Data Engineer")
name, age, role = row
print(name, age, role)


#Processing CSV Data

data = "101,Pratyusha,50000"
emp_id, name, salary = data.split(",")
print(emp_id, name, salary)


#Looping Through Records

records = [
    ("Alice", 25),
    ("Bob", 30),
    ("Charlie", 35)
]
for name, age in records:
    print(f"{name} is {age} years old")


#Swapping During Data Cleaning

min_val, max_val = 100, 50
if min_val > max_val:
    min_val, max_val = max_val, min_val
print(min_val, max_val)


#Handling Variable-Length Data (* important in pipelines)

record = ("2026-01-01", "Pratyusha", "Data Engineer", "India")
date, name, *details = record
print(date)
print(name)
print(details)


#Working with Dictionaries (Key-Value unpacking)

data = {"name": "Pratyusha", "age": 22}
for key, value in data.items():
    print(key, value)
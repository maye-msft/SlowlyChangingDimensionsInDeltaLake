# Slowly Changing Dimensions implemenation with Databricks Delta Lake
Slowly Changing Dimensions (SCD) are dimensions which change over time and in Data Warehuse we need to track the changes of the attributes keep the accuracy of the report.

And typically there are three types of SCD

- Type 1: SCD1, No history preservation
- Type 2: SCD2, Unlimited history preservation and new rows
- Type 3: SCD3, Limited history preservation

For example we have a dataset

ShortName | Fruit | Color | Price
--------- | ----- | ----- | -----
FA | Fiji Apple | Red | 3.6
BN | Banana | Yellow | 1
GG | Green Grape | Green | 2
RG | Red Grape | Red | 2

If we change the price of "Fiji Apple" into 3.5, the dataset will be 

with SCD1:

ShortName | Fruit | Color | Price
--------- | ----- | ----- | -----
FA | Fiji Apple | Red | 3.5
BN | Banana | Yellow | 1
GG | Green Grape | Green | 2
RG | Red Grape | Red | 2

with SCD2:

ShortName | Fruit | Color | Price | is_last
--------- | ----- | ----- | ----- | -------
FA | Fiji Apple | Red | 3.5 | Y
FA | Fiji Apple | Red | 3.6 | N
BN | Banana | Yellow | 1 | Y
GG | Green Grape | Green | 2 | Y
RG | Red Grape | Red | 2 | Y

with SCD3:

ShortName | Fruit | Color | Price | Color_old | Price_old 
--------- | ----- | ----- | ----- | --------- | --------- 
FA | Fiji Apple | Red | 3.5 | Red | 3.6
BN | Banana | Yellow | 1 | NULL | NULL
GG | Green Grape | Green | 2 | NULL | NULL
RG | Red Grape | Red | 2 | NULL | NULL


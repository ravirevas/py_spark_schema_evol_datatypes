import re

hive_table_name = "sparktable3"
source_file = "/home/cloudera/csv_test.csv"

parquet_types = \
    {
        'tinyint': ['smallint', 'int', 'bigint'],
        'smallint': ['int', 'bigint'],
        'int': ['bigint', 'float'],
        'float': ['double'],
        'varchar': ['varchar', 'string'],
        'char': ['char', 'varchar', 'string'],
        'decimal':['decimal']
    }

#double to decimal X
#decimal(5,2) to decimal(6,3)
#float to decimal X

def data_type_convt_check(data_type_changes):
    # check if datatype is present in dict key or not

    if data_type_changes[0] in parquet_types:
        print("\n..........key is present in dict.......case1....\n")

        # check if datatype (ex:int) value is present or not
        if (data_type_changes[1] in parquet_types[data_type_changes[0]]):
            return "0"

        # if value is not present in dict
        else:
            return "1"

    # 0:valid (datatype can be converted to new datatype)
    # 1:invalid(datatype cannot be converted to new datatype as no entry in dicts value)
    # 2: invalid(datatype key not found )
    # 3:invalid (datatype is converted from varchar(20)to varchar(10) size not compatible

    # looking for datatypes like (varchar(20))
    elif (hasNumbers(data_type_changes[0]) is True and hasNumbers(data_type_changes[1]) is True):

        # looking for varchar in dict key
        if (alphabetsOnly(data_type_changes[0]) in parquet_types):

            print("\n..........key is present in dict........... case2\n")

            # looking for varchar in keys value
            if (alphabetsOnly(data_type_changes[1]) in parquet_types[alphabetsOnly(data_type_changes[0])]):

                # if varchar(20) is > than varchar(10)
                if (int(getNumbers(data_type_changes[1])) > int(getNumbers(data_type_changes[0]))):
                    print("hi")
                    return "0"

                else:
                    return "3"
            else:
                return "1"
        else:
            return "2"

    # for case like varchar(20) to string
    elif (hasNumbers(data_type_changes[0]) is True and hasNumbers(data_type_changes[1]) is False):

        if (alphabetsOnly(data_type_changes[0] in parquet_types)):

            print("key is present in dict case3\n")

            if data_type_changes[1] in parquet_types[alphabetsOnly(data_type_changes[0])]:
                return "0"

            else:
                return "1"
        else:
            return "2"

    else:
        print(".............key not found..........\n")
        return "2"


def hasNumbers(inputString):
    # fn to check if string contains a number (i.e varchar(30))
    return bool(re.search(r'\d+', str(inputString)))




def getNumbers(inputString):
    # fn to return number from varchar(30) i.e (30)
    return re.search(r'\d+', inputString).group(0)



# Function to extract all the numbers from the given string
def getNumbers(str):
    array = re.findall(r'[0-9]+', str)
    return array

# Driver code
str = "varchar(23,23)"
array = getNumbers(str)
print(type(array))
print(len(array))


def alphabetsOnly(input):
    # fn to return varchar from varchar(30)
    regex = re.compile('[^a-zA-Z]')
    alphabet = regex.sub('', str(input))
    return alphabet

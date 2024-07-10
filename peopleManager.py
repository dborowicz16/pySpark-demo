from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower

spark = SparkSession.builder.config('spark.app.name', 'learning_spark_sql').getOrCreate()

peopleDf = spark.read.option('header', True).option('delimiter', ',').csv('people-100.csv')

def make_choice():
    print('Select an option!')
    print('[1] See entire table')
    print('[2] Reverse table order')
    print('[3] Show only a certain amount of columns')
    print('[4] Search for a person by their first name')
    print('[5] Search for a person by their last name')
    print('[6] Filter by sex')
    print('[7] Filter by email')
    print('[8] Filter by age')
    print('[9] Search for job title')
    user_choice = str(input())
    
    if (user_choice == str(1)):
         showTable()
    elif (user_choice == str(2)):
        reverseTable()
    elif (user_choice == str(3)):
         limitColumns()
    elif (user_choice == str(4)):
        searchByFirstName()
    elif (user_choice == str(5)):
        searchByLastName()
    elif (user_choice == str(6)):
        filterBySex()
    elif (user_choice == str(7)):
        searchByEmail()
    elif (user_choice == str(8)):
        filterByAge()
    elif (user_choice == str(9)):
        searchByJobTitle()

def ask_if_done():
    print('Are you done using the program or would you like to keep using it?')
    isFinished = str(input('[1] Yes, I am done with the program \n[2] No, I would like to keep using the program \n'))

    if (isFinished == str(1)):
        print('I hope that you enjoyed checking out some of what pySpark can do!')
    elif (isFinished == str(2)):
        make_choice()
    else:
        print('You must enter either 1 or 2')
        ask_if_done()

def showTable():
    peopleDf.show(peopleDf.count(), truncate=False)
    ask_if_done()

def reverseTable():
    peopleDf.withColumn("Id", col("Id").cast("int")).orderBy(col("Id").desc()).show(peopleDf.count(), truncate=False)
    ask_if_done()

def limitColumns():
    num_columns = input('How many columns would you like to show? ')

    if (num_columns.isnumeric()):
        peopleDf.show(int(num_columns), truncate=False)
        ask_if_done()
    else:
        print('You must input a number!')
        limitColumns()


def searchByFirstName():
    first_name = input('Enter a first name to search for \n')

    if (not first_name.isnumeric()):
        peopleDf.filter(col("First Name") == first_name.capitalize()).show(peopleDf.count(), truncate=False)
        ask_if_done()
    else:
        print('You must enter a name such as Lori')
        searchByFirstName()

def searchByLastName():
    last_name = input('Enter a last name to search for \n')

    if (not last_name.isnumeric()):
        peopleDf.filter(col("Last Name") == last_name.capitalize()).show(peopleDf.count(), truncate=False)
        ask_if_done()
    else:
        print('You must enter a name such as Todd')
        searchByFirstName()

def filterBySex():
    sex = input('Input Male or Female \n')
    if (sex.capitalize() == 'Male' or sex.capitalize() == 'Female'):
        peopleDf.filter(col("Sex") == sex.capitalize()).show(peopleDf.count(), truncate=False)
        ask_if_done()
    else:
        print('You must enter either Male or Female')
        filterBySex()

def searchByEmail():
    print('Search for a specific email or a part of an email. \nFor example you can search for a specific email provider like gmail ')
    search_term = input()

    if (not search_term.isnumeric()):
        peopleDf.filter(col("Email").like(f"%{search_term.lower()}%")).show(peopleDf.count(), truncate=False)
        ask_if_done()
    else:
        print('Enter a full email found on the table such as cassandra80@gmail.com or a part of an email like yahoo')
        searchByEmail()

def filterByAge():
    age = input('Input an age as a cutoff point \n')

    if (age.isnumeric()):
        print('Do you want to view people who are younger or older than ' + age + ' ?')
        comparison = input('[1] Younger \n[2] Older\n')

        if (comparison == str(1)):
            peopleDf.filter(col("Age") < age).withColumn("Age", col("Age").cast("int")).orderBy(col("Age").desc()).show(peopleDf.count(), truncate=False)
            ask_if_done()
        elif (comparison == str(2)):
            peopleDf.filter(col("Age") > age).withColumn("Age", col("Age").cast("int")).orderBy(col("Age").asc()).show(peopleDf.count(), truncate=False)
            ask_if_done()
        else:
            print('You must enter either 1 or 2')
            filterByAge()
    else:
        print('You must enter a number!')
        filterByAge()

def searchByJobTitle():
    job = input('Input a job title \n')

    if (not job.isnumeric()):
        peopleDf.filter(lower(col("Job Title")).like(f"%{job.lower()}%")).show(peopleDf.count(), truncate=False)
        ask_if_done()
    else:
        print('You ust enter a valid job title such as engineer')
        searchByJobTitle()

print('PySpark is a way to process big data using Python syntax!')
make_choice()



     






    

       

        






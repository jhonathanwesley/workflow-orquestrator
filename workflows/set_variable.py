from prefect.variables import Variable

# Set a variable

#Variable.set("key", "value")

# Get a variable

my_var = Variable.get("variable")
print(my_var)
# Update a variable by passing the `overwrite` parameter
# If the variable does not exist, it will be created

# Variable.set("my_variable", "my_new_value", overwrite=True)

# Delete a variable
#Variable.unset("my_variable")

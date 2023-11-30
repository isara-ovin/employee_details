
# Colibri Coding challange

#### Assumptions

* Person is eligible to work from 16 years of age
* If  ((Age - 16) - years of experiance ) < 0 than record is invalid
* Gender column contains only binary values
* If years of experiance or salary is null assumed that record is invalid where applicable

Used sqlite3 as the default database and used pyspark session to create dataframes as the task is based on pyspark.Startup will be slow as it will create spark session on startup and also APIs based on pyspark will perform slow as each endpoint will create dataframe based on sqlite3.




## API Reference for Django CRUD

Configured two routers from the root ```api/``` for the each section of the task
browsable api is available at http://127.0.0.1:8000/api/

#### Get all items
```http
  GET api/person/
```

#### Get record by id
#### PUT DELETE is available from here to update or delete a record
```http
  GET api/person/<pk>
```

#### General search on following fields ['first_name', 'last_name', 'email', 'industry']
```http
  GET api/person/?search=Cuesta
```

#### Get record by ordering
```http
  GET /api/person/?ordering=first_name
```

#### Get by limiting and offsetting
```http
  GET /api/person/limit=10
  GET /api/person/?offset=10&limit=10
```

#### Filtering by any key field in the json following are examples
```http
  GET api/person/?first_name__icontains=Ha
  GET api/person/?first_name__icontains=Ha&last_name=Souter
  GET api/person/?gender=M&salary__gte=200000

```


## API Reference for Pyspark

#### Average age per industry
```http
  GET api/aggregation/average_age_per_industry
```
#### Average salaries per industry
```http
  GET api/aggregation/average_salary_per_industry
```
#### Average salaries per years of experience
```http
  GET api/aggregation/average_salary_per_experiance
```
#### Average salary by domain (extracted from email field)
```http
  GET api/aggregation/average_salary_by_domain
```
#### Percentage of women in industry representation
```http
  GET api/aggregation/women_in_industry_stat/
  GET api/aggregation/women_in_industry_stat/?employee_count__gt=50

```

from rest_framework import viewsets
from rest_framework.views import APIView
from django.http import JsonResponse
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.filters import OrderingFilter, SearchFilter
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework.pagination import LimitOffsetPagination
from rest_framework import status
from .models import Person
from .serializers import PersonSerializer #, AggregationSerializer
from .filters import PersonModelFilter
from django.conf import settings

from pyspark.sql.functions import col, current_date, datediff, expr
from pyspark.sql.types import DateType
from pyspark.sql.functions import col, avg, count, when, lit, format_number, regexp_extract


LEGAL_AGE = 16
class PersonViewSet(viewsets.ModelViewSet):
    queryset = Person.objects.all()
    serializer_class = PersonSerializer
    filter_backends = [OrderingFilter, SearchFilter, DjangoFilterBackend]
    pagination_class = LimitOffsetPagination
    search_fields = ['first_name', 'last_name', 'email', 'industry']
    filterset_class = PersonModelFilter
    ordering_fields = '__all__'

class AggregationViewSet(viewsets.ViewSet):

    # GET endpoint: /aggregation/
    def list(self, request):
        data = {"message": "Following endpoints are available from the current root",
                "endpoints": ['average_age_per_industry', 'average_salary_per_industry',
                                'average_salary_per_experiance', 'average_salary_by_domain']}
        return Response(data)

    # GET endpoint: /aggregation/average_age_per_industry/
    @action(detail=False, methods=['get'])
    def average_age_per_industry(self, request):

        try:
            # Fetch required data 
            df = self.create_dataframe(Person, ['industry', 'age'])

            response = self.avg_by_industry(df, 'age')
            status_code = status.HTTP_200_OK
        except Exception as e:
            response = {'error': f'Error calculating average age per industry: {str(e)}'}
            status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
 
        return Response(response, status=status_code)
    
    # GET endpoint: /aggregation/average_salary_per_industry/
    @action(detail=False, methods=['get'])
    def average_salary_per_industry(self, request):

        try:
            df = self.create_dataframe(Person, ['industry', 'salary'])

            response = self.avg_by_industry(df, 'salary')
            status_code = status.HTTP_200_OK
        except Exception as e:
            response = {'error': f'Error calculating average salary per industry: {str(e)}'}
            status_code = status.HTTP_500_INTERNAL_SERVER_ERROR

        return Response(response, status=status_code)

    # GET endpoint: /aggregation/average_salary_per_experiance/
    @action(detail=False, methods=['get'])
    def average_salary_per_experiance(self, request):

        try: 
            columns = ['salary', 'years_of_experience']
            df = self.create_dataframe(Person, columns)

            response = self.avg_by_industry(df, *columns)
            status_code = status.HTTP_200_OK
        except Exception as e:
            response = {'error': f'Error calculating average age per experiance: {str(e)}'}
            status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        
        return Response(response, status=status_code)


    # GET endpoint: /aggregation/average_salary_by_domain/
    @action(detail=False, methods=['get'])
    def average_salary_by_domain(self, request):

        try:
            columns = ['salary', 'email']
            df = self.create_dataframe(Person, columns)

            # Using regexp_extract to extract the domain
            domain_extraction_regex = r"@([a-zA-Z0-9.-]+)"
            df = df.withColumn("email_domain", regexp_extract(col("email"), domain_extraction_regex, 1))
            df = df.na.drop(subset=['email'])

            response = self.avg_by_industry(df, 'salary', 'email_domain')
            response['message'] = f'Only {df.count()} records remain after removing Nulls for email field'
            status_code = status.HTTP_200_OK

        except Exception as e:
            response = {'error': f'Error calculating average by salary by domain from email: {str(e)}'}
            status_code = status.HTTP_500_INTERNAL_SERVER_ERROR

        return Response(response, status=status_code)


    # GET endpoint: /aggregation/women_in_industry_stat/
    @action(detail=False, methods=['get'])
    def women_in_industry_stat(self, request):

        employee_count = request.query_params.get('employee_count__gt', 0)

        try:
            columns = ['industry', 'gender']
            df = self.create_dataframe(Person, columns)

            # Cleaning invalid records
            df = df.na.drop(subset=['industry', 'gender'])
            df = df.filter(~col('industry').isin('n/a'))

            # Pivoting data on gender column
            df_gender = df.groupBy("industry").pivot("gender", ["M", "F"]).agg(count("*").alias("count"))
            df_gender = df_gender.filter((col('M') + col('F')) > employee_count)
            df_gender = df_gender.withColumn('women_percentage', (col('F')/(col('M') + col('F')))*100)
            df_gender = df_gender.orderBy(col('women_percentage').desc())
            df_gender = df_gender.withColumnsRenamed({'industry':'industry','M':'Men', 'F':'Women', 'women_percentage':'women_percentage'})

            values = [row.asDict() for row in df_gender.collect()]
            message = f'Null values for gender and industries with n/a were removed'

            response = {'data':values, 'records_considered':df.count(), 'message':message}
            status_code = status.HTTP_200_OK

        except Exception as e:
            response = {'error': f'Error calculating gender representation: {str(e)}'}
            status_code = status.HTTP_500_INTERNAL_SERVER_ERROR

        return Response(response, status=status_code)


    # Following will provide logical support for APIs
    def create_dataframe(self, model, fields):
        try:
            # Query from sqlite and create dataframe
            data = model.objects.all()
            df = settings.SPARK.createDataFrame(data)

            # Calculating age
            date_format = "dd/MM/yyyy"
            df = df.withColumn("date_of_birth", expr(f"TO_DATE(date_of_birth, '{date_format}')").cast(DateType()))
            df = df.withColumn("age", expr("year(current_date()) - year(date_of_birth)"))

            # Removing invalid records assuming a person can work from age 16
            df = df.withColumn('gap', (col('age') - LEGAL_AGE) - col('years_of_experience'))
            df = df.filter(df.gap > 0)

            # Needed columns from dataframe
            df = df.select(*fields)

            return df
        except Exception as e:
            return Response({'error': f'Error creating DataFrame: {str(e)}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def avg_by_industry(self, df, avg_by, group_by='industry'):
        message = ''
        # Calculate counts for response
        null_count = df.filter(col(group_by).isNull()).count()

        # Removing records where avg_by column is Null
        df = df.na.drop(subset=[avg_by])
        row_count = df.count()

        if group_by == 'industry':
            na_count = df.filter(col(group_by) == lit("n/a")).count()
            df = df.withColumn(group_by, when(col(group_by) == lit("n/a"), lit("Not Applicable")).otherwise(col(group_by)))
            df = df.fillna("Other", group_by)  
            message = f'Renamed {na_count} records n/a to Not Applicable and Filled {null_count} Null records as Other'          
        
        elif group_by == 'years_of_experience':
            message = f'{null_count} records were removed from aggregation'

        # Calculating the average
        df_avg = df.groupBy(group_by).agg(avg(avg_by).alias('average'), count(group_by).alias('count'))
        df_avg = df_avg.withColumn("average", col('average').cast("double"))
        df_avg = df_avg.withColumn('average', format_number('average', 2))

        # Generating list of dicts
        values = [row.asDict() for row in df_avg.collect()]

        return {'data':values, 'records_considered':row_count, 'message':message}